/**
 * 订阅持久化服务实现
 *
 * @author zhenglin
 * @date 2025/08/09
 */
package com.vmqtt.backend.service;

import com.vmqtt.backend.storage.StorageKeyspace;
import com.vmqtt.backend.storage.StorageOperations;
import com.vmqtt.backend.storage.engine.RocksDBStorageEngine;
import com.vmqtt.common.model.TopicSubscription;
import com.vmqtt.common.protocol.MqttQos;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

/**
 * 订阅持久化服务
 * 负责MQTT主题订阅信息的持久化存储、检索和管理
 * 支持主题匹配、订阅者查找等高级功能
 */
@Slf4j
@Service
public class SubscriptionPersistenceService {
    
    @Autowired
    private StorageOperations storageOperations;
    
    // 内存缓存，提高主题匹配性能
    private final Map<String, Set<String>> topicSubscribersCache = new ConcurrentHashMap<>();
    private volatile long cacheLastUpdated = 0L;
    private static final long CACHE_REFRESH_INTERVAL = 30000L; // 30秒
    
    /**
     * 添加订阅
     *
     * @param subscription 订阅信息
     * @return 是否成功
     */
    public boolean addSubscription(TopicSubscription subscription) {
        if (subscription == null || subscription.getClientId() == null || 
            subscription.getTopicFilter() == null) {
            log.warn("Cannot add invalid subscription");
            return false;
        }
        
        try {
            // 设置订阅时间
            if (subscription.getSubscribedAt() == null) {
                subscription.setSubscribedAt(LocalDateTime.now());
            }
            
            StorageOperations.BatchOperationBuilder batch = storageOperations.batchOperation();
            
            // 存储主订阅记录
            String subscriptionKey = StorageKeyspace.subscriptionKey(
                subscription.getClientId(), subscription.getTopicFilter());
            batch.put(RocksDBStorageEngine.SUBSCRIPTION_CF, subscriptionKey, subscription);
            
            // 添加主题订阅者索引
            String topicSubscribersKey = StorageKeyspace.topicSubscribersKey(
                subscription.getTopicFilter(), subscription.getClientId());
            batch.put(RocksDBStorageEngine.SUBSCRIPTION_CF, topicSubscribersKey, subscription);
            
            batch.execute();
            
            // 更新缓存
            invalidateTopicSubscribersCache();
            
            log.debug("Subscription added: clientId={}, topicFilter={}, qos={}", 
                     subscription.getClientId(), subscription.getTopicFilter(), subscription.getQos());
            return true;
            
        } catch (Exception e) {
            log.error("Failed to add subscription: clientId={}, topicFilter={}", 
                     subscription.getClientId(), subscription.getTopicFilter(), e);
            return false;
        }
    }
    
    /**
     * 批量添加订阅
     *
     * @param subscriptions 订阅信息列表
     * @return 成功添加的订阅数量
     */
    public int batchAddSubscriptions(List<TopicSubscription> subscriptions) {
        if (subscriptions == null || subscriptions.isEmpty()) {
            return 0;
        }
        
        try {
            StorageOperations.BatchOperationBuilder batch = storageOperations.batchOperation();
            int successCount = 0;
            
            for (TopicSubscription subscription : subscriptions) {
                if (subscription != null && subscription.getClientId() != null && 
                    subscription.getTopicFilter() != null) {
                    
                    if (subscription.getSubscribedAt() == null) {
                        subscription.setSubscribedAt(LocalDateTime.now());
                    }
                    
                    // 主订阅记录
                    String subscriptionKey = StorageKeyspace.subscriptionKey(
                        subscription.getClientId(), subscription.getTopicFilter());
                    batch.put(RocksDBStorageEngine.SUBSCRIPTION_CF, subscriptionKey, subscription);
                    
                    // 主题订阅者索引
                    String topicSubscribersKey = StorageKeyspace.topicSubscribersKey(
                        subscription.getTopicFilter(), subscription.getClientId());
                    batch.put(RocksDBStorageEngine.SUBSCRIPTION_CF, topicSubscribersKey, subscription);
                    
                    successCount++;
                }
            }
            
            if (successCount > 0) {
                batch.execute();
                invalidateTopicSubscribersCache();
                log.info("Batch added {} subscriptions", successCount);
            }
            
            return successCount;
            
        } catch (Exception e) {
            log.error("Failed to batch add subscriptions", e);
            return 0;
        }
    }
    
    /**
     * 移除订阅
     *
     * @param clientId 客户端ID
     * @param topicFilter 主题过滤器
     * @return 被移除的订阅信息
     */
    public Optional<TopicSubscription> removeSubscription(String clientId, String topicFilter) {
        if (clientId == null || topicFilter == null) {
            return Optional.empty();
        }
        
        try {
            // 首先获取订阅信息
            Optional<TopicSubscription> subscriptionOpt = getSubscription(clientId, topicFilter);
            if (!subscriptionOpt.isPresent()) {
                log.debug("Subscription not found for removal: clientId={}, topicFilter={}", 
                         clientId, topicFilter);
                return Optional.empty();
            }
            
            TopicSubscription subscription = subscriptionOpt.get();
            
            StorageOperations.BatchOperationBuilder batch = storageOperations.batchOperation();
            
            // 删除主订阅记录
            String subscriptionKey = StorageKeyspace.subscriptionKey(clientId, topicFilter);
            batch.delete(RocksDBStorageEngine.SUBSCRIPTION_CF, subscriptionKey);
            
            // 删除主题订阅者索引
            String topicSubscribersKey = StorageKeyspace.topicSubscribersKey(topicFilter, clientId);
            batch.delete(RocksDBStorageEngine.SUBSCRIPTION_CF, topicSubscribersKey);
            
            batch.execute();
            
            // 更新缓存
            invalidateTopicSubscribersCache();
            
            log.debug("Subscription removed: clientId={}, topicFilter={}", clientId, topicFilter);
            return Optional.of(subscription);
            
        } catch (Exception e) {
            log.error("Failed to remove subscription: clientId={}, topicFilter={}", clientId, topicFilter, e);
            return Optional.empty();
        }
    }
    
    /**
     * 移除客户端的所有订阅
     *
     * @param clientId 客户端ID
     * @return 被移除的订阅数量
     */
    public int removeAllSubscriptions(String clientId) {
        if (clientId == null || clientId.trim().isEmpty()) {
            return 0;
        }
        
        try {
            // 获取客户端的所有订阅
            List<TopicSubscription> subscriptions = getClientSubscriptions(clientId, 10000);
            
            if (subscriptions.isEmpty()) {
                return 0;
            }
            
            StorageOperations.BatchOperationBuilder batch = storageOperations.batchOperation();
            
            for (TopicSubscription subscription : subscriptions) {
                // 删除主订阅记录
                String subscriptionKey = StorageKeyspace.subscriptionKey(
                    subscription.getClientId(), subscription.getTopicFilter());
                batch.delete(RocksDBStorageEngine.SUBSCRIPTION_CF, subscriptionKey);
                
                // 删除主题订阅者索引
                String topicSubscribersKey = StorageKeyspace.topicSubscribersKey(
                    subscription.getTopicFilter(), subscription.getClientId());
                batch.delete(RocksDBStorageEngine.SUBSCRIPTION_CF, topicSubscribersKey);
            }
            
            batch.execute();
            
            // 更新缓存
            invalidateTopicSubscribersCache();
            
            log.info("Removed {} subscriptions for client: {}", subscriptions.size(), clientId);
            return subscriptions.size();
            
        } catch (Exception e) {
            log.error("Failed to remove all subscriptions for client: {}", clientId, e);
            return 0;
        }
    }
    
    /**
     * 获取订阅信息
     *
     * @param clientId 客户端ID
     * @param topicFilter 主题过滤器
     * @return 订阅信息
     */
    public Optional<TopicSubscription> getSubscription(String clientId, String topicFilter) {
        if (clientId == null || topicFilter == null) {
            return Optional.empty();
        }
        
        try {
            String subscriptionKey = StorageKeyspace.subscriptionKey(clientId, topicFilter);
            Optional<TopicSubscription> subscription = storageOperations.get(
                RocksDBStorageEngine.SUBSCRIPTION_CF, subscriptionKey, TopicSubscription.class);
            
            if (subscription.isPresent()) {
                log.debug("Subscription retrieved: clientId={}, topicFilter={}", clientId, topicFilter);
            }
            
            return subscription;
            
        } catch (Exception e) {
            log.error("Failed to get subscription: clientId={}, topicFilter={}", clientId, topicFilter, e);
            return Optional.empty();
        }
    }
    
    /**
     * 获取客户端的所有订阅
     *
     * @param clientId 客户端ID
     * @param limit 限制数量
     * @return 订阅列表
     */
    public List<TopicSubscription> getClientSubscriptions(String clientId, int limit) {
        if (clientId == null || clientId.trim().isEmpty()) {
            return List.of();
        }
        
        try {
            String subscriptionPrefix = StorageKeyspace.SUBSCRIPTION_PREFIX + 
                                      StorageKeyspace.KEY_SEPARATOR + clientId;
            
            List<StorageOperations.KeyValuePair<TopicSubscription>> subscriptions = 
                storageOperations.scanByPrefix(RocksDBStorageEngine.SUBSCRIPTION_CF, 
                                             subscriptionPrefix, limit, TopicSubscription.class);
            
            List<TopicSubscription> result = subscriptions.stream()
                    .map(StorageOperations.KeyValuePair::getValue)
                    .filter(Objects::nonNull)
                    .sorted((s1, s2) -> s1.getSubscribedAt().compareTo(s2.getSubscribedAt()))
                    .collect(Collectors.toList());
            
            log.debug("Found {} subscriptions for client: {}", result.size(), clientId);
            return result;
            
        } catch (Exception e) {
            log.error("Failed to get client subscriptions: clientId={}", clientId, e);
            return List.of();
        }
    }
    
    /**
     * 获取主题的订阅者
     *
     * @param topicFilter 主题过滤器
     * @param limit 限制数量
     * @return 订阅者列表
     */
    public List<TopicSubscription> getTopicSubscribers(String topicFilter, int limit) {
        if (topicFilter == null || topicFilter.trim().isEmpty()) {
            return List.of();
        }
        
        try {
            String subscribersPrefix = StorageKeyspace.TOPIC_SUBSCRIBERS_PREFIX + 
                                     StorageKeyspace.KEY_SEPARATOR + topicFilter;
            
            List<StorageOperations.KeyValuePair<TopicSubscription>> subscribers = 
                storageOperations.scanByPrefix(RocksDBStorageEngine.SUBSCRIPTION_CF, 
                                             subscribersPrefix, limit, TopicSubscription.class);
            
            List<TopicSubscription> result = subscribers.stream()
                    .map(StorageOperations.KeyValuePair::getValue)
                    .filter(Objects::nonNull)
                    .sorted((s1, s2) -> s1.getSubscribedAt().compareTo(s2.getSubscribedAt()))
                    .collect(Collectors.toList());
            
            log.debug("Found {} subscribers for topic: {}", result.size(), topicFilter);
            return result;
            
        } catch (Exception e) {
            log.error("Failed to get topic subscribers: topicFilter={}", topicFilter, e);
            return List.of();
        }
    }
    
    /**
     * 查找匹配主题的订阅
     *
     * @param topic 发布主题
     * @param limit 限制数量
     * @return 匹配的订阅列表
     */
    public List<TopicSubscription> findMatchingSubscriptions(String topic, int limit) {
        if (topic == null || topic.trim().isEmpty()) {
            return List.of();
        }
        
        try {
            List<TopicSubscription> matchingSubscriptions = new ArrayList<>();
            
            // 使用缓存加速主题匹配
            Set<String> potentialTopicFilters = getCachedTopicFilters();
            
            for (String topicFilter : potentialTopicFilters) {
                if (matchingSubscriptions.size() >= limit) {
                    break;
                }
                
                if (isTopicMatch(topic, topicFilter)) {
                    List<TopicSubscription> subscribers = getTopicSubscribers(topicFilter, limit);
                    matchingSubscriptions.addAll(subscribers);
                }
            }
            
            // 如果缓存为空或过期，直接扫描订阅
            if (potentialTopicFilters.isEmpty()) {
                matchingSubscriptions = findMatchingSubscriptionsDirect(topic, limit);
            }
            
            // 按QoS和订阅时间排序
            List<TopicSubscription> result = matchingSubscriptions.stream()
                    .filter(Objects::nonNull)
                    .distinct()
                    .limit(limit)
                    .sorted((s1, s2) -> {
                        int qosCompare = s2.getQos().compareTo(s1.getQos()); // QoS高的优先
                        if (qosCompare != 0) return qosCompare;
                        return s1.getSubscribedAt().compareTo(s2.getSubscribedAt()); // 早订阅的优先
                    })
                    .collect(Collectors.toList());
            
            log.debug("Found {} matching subscriptions for topic: {}", result.size(), topic);
            return result;
            
        } catch (Exception e) {
            log.error("Failed to find matching subscriptions for topic: {}", topic, e);
            return List.of();
        }
    }
    
    /**
     * 更新订阅统计信息
     *
     * @param clientId 客户端ID
     * @param topicFilter 主题过滤器
     * @param updateType 更新类型
     * @return 是否成功
     */
    public boolean updateSubscriptionStats(String clientId, String topicFilter, StatsUpdateType updateType) {
        if (clientId == null || topicFilter == null || updateType == null) {
            return false;
        }
        
        try {
            Optional<TopicSubscription> subscriptionOpt = getSubscription(clientId, topicFilter);
            if (!subscriptionOpt.isPresent()) {
                log.debug("Subscription not found for stats update: clientId={}, topicFilter={}", 
                         clientId, topicFilter);
                return false;
            }
            
            TopicSubscription subscription = subscriptionOpt.get();
            
            // 更新统计信息
            switch (updateType) {
                case MESSAGE_MATCHED:
                    subscription.updateMatchStats();
                    break;
                case MESSAGE_DELIVERED_QOS0:
                    subscription.updateDeliveryStats(MqttQos.AT_MOST_ONCE);
                    break;
                case MESSAGE_DELIVERED_QOS1:
                    subscription.updateDeliveryStats(MqttQos.AT_LEAST_ONCE);
                    break;
                case MESSAGE_DELIVERED_QOS2:
                    subscription.updateDeliveryStats(MqttQos.EXACTLY_ONCE);
                    break;
                case MESSAGE_DROPPED:
                    subscription.updateDropStats();
                    break;
            }
            
            // 保存更新后的订阅
            String subscriptionKey = StorageKeyspace.subscriptionKey(clientId, topicFilter);
            storageOperations.put(RocksDBStorageEngine.SUBSCRIPTION_CF, subscriptionKey, subscription);
            
            log.debug("Subscription stats updated: clientId={}, topicFilter={}, updateType={}", 
                     clientId, topicFilter, updateType);
            return true;
            
        } catch (Exception e) {
            log.error("Failed to update subscription stats: clientId={}, topicFilter={}, updateType={}", 
                     clientId, topicFilter, updateType, e);
            return false;
        }
    }
    
    /**
     * 检查订阅是否存在
     *
     * @param clientId 客户端ID
     * @param topicFilter 主题过滤器
     * @return 是否存在
     */
    public boolean subscriptionExists(String clientId, String topicFilter) {
        if (clientId == null || topicFilter == null) {
            return false;
        }
        
        try {
            String subscriptionKey = StorageKeyspace.subscriptionKey(clientId, topicFilter);
            return storageOperations.exists(RocksDBStorageEngine.SUBSCRIPTION_CF, subscriptionKey);
        } catch (Exception e) {
            log.error("Failed to check subscription existence: clientId={}, topicFilter={}", 
                     clientId, topicFilter, e);
            return false;
        }
    }
    
    /**
     * 异步添加订阅
     *
     * @param subscription 订阅信息
     * @return CompletableFuture<Boolean>
     */
    @Async
    public CompletableFuture<Boolean> addSubscriptionAsync(TopicSubscription subscription) {
        return CompletableFuture.completedFuture(addSubscription(subscription));
    }
    
    /**
     * 异步获取客户端订阅
     *
     * @param clientId 客户端ID
     * @param limit 限制数量
     * @return CompletableFuture<List<TopicSubscription>>
     */
    @Async
    public CompletableFuture<List<TopicSubscription>> getClientSubscriptionsAsync(String clientId, int limit) {
        return CompletableFuture.completedFuture(getClientSubscriptions(clientId, limit));
    }
    
    /**
     * 获取订阅统计信息
     *
     * @return 订阅统计
     */
    public SubscriptionStatistics getSubscriptionStatistics() {
        try {
            SubscriptionStatistics stats = new SubscriptionStatistics();
            
            // 获取总订阅数
            stats.totalSubscriptions = storageOperations.getApproximateKeyCount(RocksDBStorageEngine.SUBSCRIPTION_CF) / 2; // 除以2因为有索引
            
            // 获取主题过滤器数量
            Set<String> topicFilters = getCachedTopicFilters();
            stats.uniqueTopicFilters = topicFilters.size();
            
            // 这里可以添加更详细的统计逻辑
            
            return stats;
            
        } catch (Exception e) {
            log.error("Failed to get subscription statistics", e);
            return new SubscriptionStatistics();
        }
    }
    
    /**
     * 直接扫描查找匹配订阅（缓存失效时使用）
     */
    private List<TopicSubscription> findMatchingSubscriptionsDirect(String topic, int limit) {
        try {
            List<TopicSubscription> matchingSubscriptions = new ArrayList<>();
            
            // 扫描所有主题订阅者索引
            String subscribersPrefix = StorageKeyspace.TOPIC_SUBSCRIBERS_PREFIX + StorageKeyspace.KEY_SEPARATOR;
            List<StorageOperations.KeyValuePair<TopicSubscription>> allSubscriptions = 
                storageOperations.scanByPrefix(RocksDBStorageEngine.SUBSCRIPTION_CF, 
                                             subscribersPrefix, limit * 10, TopicSubscription.class);
            
            for (StorageOperations.KeyValuePair<TopicSubscription> kvp : allSubscriptions) {
                if (matchingSubscriptions.size() >= limit) {
                    break;
                }
                
                TopicSubscription subscription = kvp.getValue();
                if (subscription != null && isTopicMatch(topic, subscription.getTopicFilter())) {
                    matchingSubscriptions.add(subscription);
                }
            }
            
            return matchingSubscriptions;
            
        } catch (Exception e) {
            log.error("Failed to find matching subscriptions directly for topic: {}", topic, e);
            return List.of();
        }
    }
    
    /**
     * 获取缓存的主题过滤器
     */
    private Set<String> getCachedTopicFilters() {
        long currentTime = System.currentTimeMillis();
        
        if (currentTime - cacheLastUpdated > CACHE_REFRESH_INTERVAL || topicSubscribersCache.isEmpty()) {
            refreshTopicSubscribersCache();
        }
        
        return new HashSet<>(topicSubscribersCache.keySet());
    }
    
    /**
     * 刷新主题订阅者缓存
     */
    private synchronized void refreshTopicSubscribersCache() {
        try {
            topicSubscribersCache.clear();
            
            // 扫描所有订阅获取主题过滤器
            String subscribersPrefix = StorageKeyspace.TOPIC_SUBSCRIBERS_PREFIX + StorageKeyspace.KEY_SEPARATOR;
            List<StorageOperations.KeyValuePair<TopicSubscription>> allSubscriptions = 
                storageOperations.scanByPrefix(RocksDBStorageEngine.SUBSCRIPTION_CF, 
                                             subscribersPrefix, 10000, TopicSubscription.class);
            
            for (StorageOperations.KeyValuePair<TopicSubscription> kvp : allSubscriptions) {
                TopicSubscription subscription = kvp.getValue();
                if (subscription != null && subscription.getTopicFilter() != null) {
                    topicSubscribersCache.computeIfAbsent(subscription.getTopicFilter(), k -> new HashSet<>())
                                       .add(subscription.getClientId());
                }
            }
            
            cacheLastUpdated = System.currentTimeMillis();
            log.debug("Refreshed topic subscribers cache with {} topic filters", topicSubscribersCache.size());
            
        } catch (Exception e) {
            log.error("Failed to refresh topic subscribers cache", e);
        }
    }
    
    /**
     * 使缓存失效
     */
    private void invalidateTopicSubscribersCache() {
        cacheLastUpdated = 0L;
    }
    
    /**
     * MQTT主题匹配算法
     * 支持单级通配符(+)和多级通配符(#)
     *
     * @param topic 发布主题
     * @param filter 主题过滤器
     * @return 是否匹配
     */
    private boolean isTopicMatch(String topic, String filter) {
        if (topic == null || filter == null) {
            return false;
        }
        
        // 完全匹配
        if (topic.equals(filter)) {
            return true;
        }
        
        // 多级通配符 # 匹配所有
        if ("#".equals(filter)) {
            return true;
        }
        
        // 以$开头的系统主题不能被通配符匹配
        if (topic.startsWith("$") && (filter.contains("+") || filter.contains("#"))) {
            return false;
        }
        
        String[] topicLevels = topic.split("/");
        String[] filterLevels = filter.split("/");
        
        return matchTopicLevels(topicLevels, filterLevels, 0, 0);
    }
    
    /**
     * 递归匹配主题级别
     */
    private boolean matchTopicLevels(String[] topicLevels, String[] filterLevels, 
                                   int topicIndex, int filterIndex) {
        // 处理多级通配符 #
        if (filterIndex < filterLevels.length && "#".equals(filterLevels[filterIndex])) {
            // # 必须是过滤器的最后一个级别
            return filterIndex == filterLevels.length - 1;
        }
        
        // 如果过滤器已经处理完
        if (filterIndex >= filterLevels.length) {
            return topicIndex >= topicLevels.length;
        }
        
        // 如果主题已经处理完但过滤器还有
        if (topicIndex >= topicLevels.length) {
            return false;
        }
        
        String currentFilter = filterLevels[filterIndex];
        String currentTopic = topicLevels[topicIndex];
        
        // 单级通配符 + 或完全匹配
        if ("+".equals(currentFilter) || currentTopic.equals(currentFilter)) {
            return matchTopicLevels(topicLevels, filterLevels, topicIndex + 1, filterIndex + 1);
        }
        
        return false;
    }
    
    /**
     * 统计更新类型
     */
    public enum StatsUpdateType {
        MESSAGE_MATCHED,
        MESSAGE_DELIVERED_QOS0,
        MESSAGE_DELIVERED_QOS1,
        MESSAGE_DELIVERED_QOS2,
        MESSAGE_DROPPED
    }
    
    /**
     * 订阅统计信息
     */
    public static class SubscriptionStatistics {
        public long totalSubscriptions = 0;
        public long uniqueTopicFilters = 0;
        public long sharedSubscriptions = 0;
        public long wildcardSubscriptions = 0;
        public LocalDateTime lastUpdated = LocalDateTime.now();
        
        @Override
        public String toString() {
            return String.format("SubscriptionStats{total=%d, uniqueTopics=%d, shared=%d, wildcard=%d}", 
                               totalSubscriptions, uniqueTopicFilters, sharedSubscriptions, wildcardSubscriptions);
        }
    }
}