/**
 * 保留消息存储服务实现
 *
 * @author zhenglin
 * @date 2025/08/09
 */
package com.vmqtt.backend.service;

import com.vmqtt.backend.storage.StorageKeyspace;
import com.vmqtt.backend.storage.StorageOperations;
import com.vmqtt.backend.storage.engine.RocksDBStorageEngine;
import com.vmqtt.common.model.QueuedMessage;
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
 * 保留消息存储服务
 * 负责MQTT保留消息的持久化存储、检索和管理
 * 按照MQTT协议规范处理保留消息的生命周期
 */
@Slf4j
@Service
public class RetainedMessageService {
    
    @Autowired
    private StorageOperations storageOperations;
    
    // 内存缓存保留消息，提高匹配性能
    private final Map<String, RetainedMessage> retainedMessageCache = new ConcurrentHashMap<>();
    private volatile long cacheLastUpdated = 0L;
    private static final long CACHE_REFRESH_INTERVAL = 60000L; // 1分钟
    
    /**
     * 存储保留消息
     *
     * @param topic 主题
     * @param message 消息
     * @return 是否成功
     */
    public boolean storeRetainedMessage(String topic, QueuedMessage message) {
        if (topic == null || topic.trim().isEmpty()) {
            log.warn("Cannot store retained message with null or empty topic");
            return false;
        }
        
        // 如果消息为null或payload为空，表示删除保留消息
        if (message == null || message.getPayload() == null || message.getPayload().length == 0) {
            return removeRetainedMessage(topic);
        }
        
        try {
            // 创建保留消息对象
            RetainedMessage retainedMessage = new RetainedMessage();
            retainedMessage.setTopic(topic);
            retainedMessage.setPayload(message.getPayload());
            retainedMessage.setQos(message.getQos());
            retainedMessage.setCreatedAt(LocalDateTime.now());
            retainedMessage.setPublisherId(message.getClientId());
            retainedMessage.setProperties(message.getProperties());
            retainedMessage.setUserProperties(message.getUserProperties());
            
            // 设置过期时间（如果消息有TTL）
            if (message.getExpiresAt() != null) {
                retainedMessage.setExpiresAt(message.getExpiresAt());
            }
            
            StorageOperations.BatchOperationBuilder batch = storageOperations.batchOperation();
            
            // 存储主保留消息记录
            String retainedKey = StorageKeyspace.retainedMessageKey(topic);
            batch.put(RocksDBStorageEngine.RETAINED_CF, retainedKey, retainedMessage);
            
            // 添加时间索引
            long timestamp = System.currentTimeMillis();
            String indexKey = StorageKeyspace.retainedMessageIndexKey(timestamp, topic);
            batch.put(RocksDBStorageEngine.RETAINED_CF, indexKey, retainedMessage);
            
            batch.execute();
            
            // 更新缓存
            retainedMessageCache.put(topic, retainedMessage);
            
            log.debug("Retained message stored: topic={}, qos={}, payloadSize={}", 
                     topic, message.getQos(), message.getPayload().length);
            return true;
            
        } catch (Exception e) {
            log.error("Failed to store retained message: topic={}", topic, e);
            return false;
        }
    }
    
    /**
     * 获取保留消息
     *
     * @param topic 主题
     * @return 保留消息Optional
     */
    public Optional<RetainedMessage> getRetainedMessage(String topic) {
        if (topic == null || topic.trim().isEmpty()) {
            return Optional.empty();
        }
        
        try {
            // 首先尝试从缓存获取
            RetainedMessage cachedMessage = retainedMessageCache.get(topic);
            if (cachedMessage != null && !cachedMessage.isExpired()) {
                log.debug("Retained message retrieved from cache: topic={}", topic);
                return Optional.of(cachedMessage);
            }
            
            // 从存储获取
            String retainedKey = StorageKeyspace.retainedMessageKey(topic);
            Optional<RetainedMessage> messageOpt = storageOperations.get(
                RocksDBStorageEngine.RETAINED_CF, retainedKey, RetainedMessage.class);
            
            if (messageOpt.isPresent()) {
                RetainedMessage message = messageOpt.get();
                
                // 检查是否过期
                if (message.isExpired()) {
                    // 异步删除过期消息
                    removeRetainedMessageAsync(topic);
                    return Optional.empty();
                }
                
                // 更新缓存
                retainedMessageCache.put(topic, message);
                
                log.debug("Retained message retrieved from storage: topic={}", topic);
                return Optional.of(message);
            }
            
            // 从缓存中移除不存在的主题
            retainedMessageCache.remove(topic);
            
            return Optional.empty();
            
        } catch (Exception e) {
            log.error("Failed to get retained message: topic={}", topic, e);
            return Optional.empty();
        }
    }
    
    /**
     * 移除保留消息
     *
     * @param topic 主题
     * @return 是否成功
     */
    public boolean removeRetainedMessage(String topic) {
        if (topic == null || topic.trim().isEmpty()) {
            return false;
        }
        
        try {
            // 首先获取消息以获取时间戳信息
            Optional<RetainedMessage> messageOpt = getRetainedMessage(topic);
            
            StorageOperations.BatchOperationBuilder batch = storageOperations.batchOperation();
            
            // 删除主记录
            String retainedKey = StorageKeyspace.retainedMessageKey(topic);
            batch.delete(RocksDBStorageEngine.RETAINED_CF, retainedKey);
            
            // 删除时间索引
            if (messageOpt.isPresent()) {
                RetainedMessage message = messageOpt.get();
                long timestamp = java.time.ZoneId.systemDefault().getRules()
                        .getOffset(message.getCreatedAt()).getTotalSeconds() * 1000;
                String indexKey = StorageKeyspace.retainedMessageIndexKey(timestamp, topic);
                batch.delete(RocksDBStorageEngine.RETAINED_CF, indexKey);
            }
            
            batch.execute();
            
            // 从缓存移除
            retainedMessageCache.remove(topic);
            
            log.debug("Retained message removed: topic={}", topic);
            return true;
            
        } catch (Exception e) {
            log.error("Failed to remove retained message: topic={}", topic, e);
            return false;
        }
    }
    
    /**
     * 查找匹配主题过滤器的保留消息
     *
     * @param topicFilter 主题过滤器
     * @param limit 限制数量
     * @return 匹配的保留消息列表
     */
    public List<RetainedMessage> findMatchingRetainedMessages(String topicFilter, int limit) {
        if (topicFilter == null || topicFilter.trim().isEmpty()) {
            return List.of();
        }
        
        try {
            List<RetainedMessage> matchingMessages = new ArrayList<>();
            
            // 首先尝试从缓存查找
            if (!retainedMessageCache.isEmpty()) {
                for (Map.Entry<String, RetainedMessage> entry : retainedMessageCache.entrySet()) {
                    if (matchingMessages.size() >= limit) {
                        break;
                    }
                    
                    String topic = entry.getKey();
                    RetainedMessage message = entry.getValue();
                    
                    if (!message.isExpired() && isTopicMatch(topic, topicFilter)) {
                        matchingMessages.add(message);
                    }
                }
                
                if (!matchingMessages.isEmpty()) {
                    log.debug("Found {} matching retained messages from cache for filter: {}", 
                             matchingMessages.size(), topicFilter);
                    return matchingMessages.stream()
                            .sorted((m1, m2) -> m2.getCreatedAt().compareTo(m1.getCreatedAt()))
                            .collect(Collectors.toList());
                }
            }
            
            // 从存储中查找
            return findMatchingRetainedMessagesFromStorage(topicFilter, limit);
            
        } catch (Exception e) {
            log.error("Failed to find matching retained messages for filter: {}", topicFilter, e);
            return List.of();
        }
    }
    
    /**
     * 获取所有保留消息的主题列表
     *
     * @param limit 限制数量
     * @return 主题列表
     */
    public List<String> getRetainedMessageTopics(int limit) {
        try {
            String retainedPrefix = StorageKeyspace.RETAINED_MESSAGE_PREFIX + StorageKeyspace.KEY_SEPARATOR;
            List<StorageOperations.KeyValuePair<RetainedMessage>> retainedMessages = 
                storageOperations.scanByPrefix(RocksDBStorageEngine.RETAINED_CF, 
                                             retainedPrefix, limit, RetainedMessage.class);
            
            List<String> topics = retainedMessages.stream()
                    .map(kvp -> {
                        // 从键中提取主题名称
                        String key = kvp.getKey();
                        int separatorIndex = key.lastIndexOf(StorageKeyspace.KEY_SEPARATOR);
                        return separatorIndex > 0 ? key.substring(separatorIndex + 1) : key;
                    })
                    .filter(Objects::nonNull)
                    .distinct()
                    .collect(Collectors.toList());
            
            log.debug("Found {} retained message topics", topics.size());
            return topics;
            
        } catch (Exception e) {
            log.error("Failed to get retained message topics", e);
            return List.of();
        }
    }
    
    /**
     * 清理过期的保留消息
     *
     * @param batchSize 批处理大小
     * @return 清理的消息数量
     */
    public int cleanupExpiredRetainedMessages(int batchSize) {
        try {
            List<RetainedMessage> expiredMessages = getExpiredRetainedMessages(batchSize);
            
            int cleanedCount = 0;
            for (RetainedMessage message : expiredMessages) {
                if (removeRetainedMessage(message.getTopic())) {
                    cleanedCount++;
                }
            }
            
            if (cleanedCount > 0) {
                log.info("Cleaned up {} expired retained messages", cleanedCount);
            }
            
            return cleanedCount;
            
        } catch (Exception e) {
            log.error("Failed to cleanup expired retained messages", e);
            return 0;
        }
    }
    
    /**
     * 刷新保留消息缓存
     */
    public void refreshCache() {
        try {
            log.info("Refreshing retained message cache...");
            retainedMessageCache.clear();
            
            // 加载所有保留消息到缓存
            String retainedPrefix = StorageKeyspace.RETAINED_MESSAGE_PREFIX + StorageKeyspace.KEY_SEPARATOR;
            List<StorageOperations.KeyValuePair<RetainedMessage>> retainedMessages = 
                storageOperations.scanByPrefix(RocksDBStorageEngine.RETAINED_CF, 
                                             retainedPrefix, 10000, RetainedMessage.class);
            
            for (StorageOperations.KeyValuePair<RetainedMessage> kvp : retainedMessages) {
                RetainedMessage message = kvp.getValue();
                if (message != null && !message.isExpired()) {
                    retainedMessageCache.put(message.getTopic(), message);
                }
            }
            
            cacheLastUpdated = System.currentTimeMillis();
            log.info("Retained message cache refreshed with {} messages", retainedMessageCache.size());
            
        } catch (Exception e) {
            log.error("Failed to refresh retained message cache", e);
        }
    }
    
    /**
     * 异步移除保留消息
     *
     * @param topic 主题
     * @return CompletableFuture<Boolean>
     */
    @Async
    public CompletableFuture<Boolean> removeRetainedMessageAsync(String topic) {
        return CompletableFuture.completedFuture(removeRetainedMessage(topic));
    }
    
    /**
     * 异步查找匹配的保留消息
     *
     * @param topicFilter 主题过滤器
     * @param limit 限制数量
     * @return CompletableFuture<List<RetainedMessage>>
     */
    @Async
    public CompletableFuture<List<RetainedMessage>> findMatchingRetainedMessagesAsync(String topicFilter, int limit) {
        return CompletableFuture.completedFuture(findMatchingRetainedMessages(topicFilter, limit));
    }
    
    /**
     * 获取保留消息统计信息
     *
     * @return 保留消息统计
     */
    public RetainedMessageStatistics getRetainedMessageStatistics() {
        try {
            RetainedMessageStatistics stats = new RetainedMessageStatistics();
            
            // 从缓存获取统计信息
            if (System.currentTimeMillis() - cacheLastUpdated < CACHE_REFRESH_INTERVAL && !retainedMessageCache.isEmpty()) {
                stats.totalRetainedMessages = retainedMessageCache.size();
                stats.cachedMessages = retainedMessageCache.size();
                
                long qos0Count = retainedMessageCache.values().stream()
                        .mapToLong(msg -> msg.getQos() == MqttQos.AT_MOST_ONCE ? 1 : 0)
                        .sum();
                long qos1Count = retainedMessageCache.values().stream()
                        .mapToLong(msg -> msg.getQos() == MqttQos.AT_LEAST_ONCE ? 1 : 0)
                        .sum();
                long qos2Count = retainedMessageCache.values().stream()
                        .mapToLong(msg -> msg.getQos() == MqttQos.EXACTLY_ONCE ? 1 : 0)
                        .sum();
                
                stats.qos0Messages = qos0Count;
                stats.qos1Messages = qos1Count;
                stats.qos2Messages = qos2Count;
            } else {
                // 从存储获取统计信息
                long totalMessages = storageOperations.getApproximateKeyCount(RocksDBStorageEngine.RETAINED_CF) / 2; // 除以2因为有索引
                stats.totalRetainedMessages = totalMessages;
                stats.cachedMessages = retainedMessageCache.size();
            }
            
            return stats;
            
        } catch (Exception e) {
            log.error("Failed to get retained message statistics", e);
            return new RetainedMessageStatistics();
        }
    }
    
    /**
     * 从存储中查找匹配的保留消息
     */
    private List<RetainedMessage> findMatchingRetainedMessagesFromStorage(String topicFilter, int limit) {
        try {
            List<RetainedMessage> matchingMessages = new ArrayList<>();
            
            // 扫描所有保留消息
            String retainedPrefix = StorageKeyspace.RETAINED_MESSAGE_PREFIX + StorageKeyspace.KEY_SEPARATOR;
            List<StorageOperations.KeyValuePair<RetainedMessage>> retainedMessages = 
                storageOperations.scanByPrefix(RocksDBStorageEngine.RETAINED_CF, 
                                             retainedPrefix, limit * 2, RetainedMessage.class);
            
            for (StorageOperations.KeyValuePair<RetainedMessage> kvp : retainedMessages) {
                if (matchingMessages.size() >= limit) {
                    break;
                }
                
                RetainedMessage message = kvp.getValue();
                if (message != null && !message.isExpired() && 
                    isTopicMatch(message.getTopic(), topicFilter)) {
                    matchingMessages.add(message);
                }
            }
            
            log.debug("Found {} matching retained messages from storage for filter: {}", 
                     matchingMessages.size(), topicFilter);
            
            return matchingMessages.stream()
                    .sorted((m1, m2) -> m2.getCreatedAt().compareTo(m1.getCreatedAt()))
                    .collect(Collectors.toList());
            
        } catch (Exception e) {
            log.error("Failed to find matching retained messages from storage for filter: {}", topicFilter, e);
            return List.of();
        }
    }
    
    /**
     * 获取过期的保留消息
     */
    private List<RetainedMessage> getExpiredRetainedMessages(int limit) {
        try {
            LocalDateTime currentTime = LocalDateTime.now();
            List<RetainedMessage> expiredMessages = new ArrayList<>();
            
            // 从时间索引中查找过期消息
            long currentTimestamp = System.currentTimeMillis();
            String startKey = StorageKeyspace.RETAINED_MESSAGE_INDEX_PREFIX + StorageKeyspace.KEY_SEPARATOR;
            String endKey = StorageKeyspace.retainedMessageIndexKey(currentTimestamp, "");
            
            List<StorageOperations.KeyValuePair<RetainedMessage>> indexedMessages = 
                storageOperations.scan(RocksDBStorageEngine.RETAINED_CF, 
                                     startKey, endKey, limit, RetainedMessage.class);
            
            for (StorageOperations.KeyValuePair<RetainedMessage> kvp : indexedMessages) {
                RetainedMessage message = kvp.getValue();
                if (message != null && message.isExpired()) {
                    expiredMessages.add(message);
                }
            }
            
            return expiredMessages;
            
        } catch (Exception e) {
            log.error("Failed to get expired retained messages", e);
            return List.of();
        }
    }
    
    /**
     * 主题匹配算法
     * 注意：对于保留消息，是用具体主题匹配订阅过滤器
     */
    private boolean isTopicMatch(String topic, String filter) {
        if (topic == null || filter == null) {
            return false;
        }
        
        // 完全匹配
        if (topic.equals(filter)) {
            return true;
        }
        
        // 多级通配符匹配
        if ("#".equals(filter)) {
            return true;
        }
        
        // 系统主题不能被通配符匹配
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
        // 处理多级通配符
        if (filterIndex < filterLevels.length && "#".equals(filterLevels[filterIndex])) {
            return filterIndex == filterLevels.length - 1;
        }
        
        // 过滤器处理完成
        if (filterIndex >= filterLevels.length) {
            return topicIndex >= topicLevels.length;
        }
        
        // 主题处理完成但过滤器还有
        if (topicIndex >= topicLevels.length) {
            return false;
        }
        
        String currentFilter = filterLevels[filterIndex];
        String currentTopic = topicLevels[topicIndex];
        
        // 单级通配符或完全匹配
        if ("+".equals(currentFilter) || currentTopic.equals(currentFilter)) {
            return matchTopicLevels(topicLevels, filterLevels, topicIndex + 1, filterIndex + 1);
        }
        
        return false;
    }
    
    /**
     * 保留消息内部类
     */
    public static class RetainedMessage {
        private String topic;
        private byte[] payload;
        private MqttQos qos;
        private LocalDateTime createdAt;
        private LocalDateTime expiresAt;
        private String publisherId;
        private Map<String, Object> properties = new HashMap<>();
        private Map<String, String> userProperties = new HashMap<>();
        
        public String getTopic() { return topic; }
        public void setTopic(String topic) { this.topic = topic; }
        
        public byte[] getPayload() { return payload; }
        public void setPayload(byte[] payload) { this.payload = payload; }
        
        public MqttQos getQos() { return qos; }
        public void setQos(MqttQos qos) { this.qos = qos; }
        
        public LocalDateTime getCreatedAt() { return createdAt; }
        public void setCreatedAt(LocalDateTime createdAt) { this.createdAt = createdAt; }
        
        public LocalDateTime getExpiresAt() { return expiresAt; }
        public void setExpiresAt(LocalDateTime expiresAt) { this.expiresAt = expiresAt; }
        
        public String getPublisherId() { return publisherId; }
        public void setPublisherId(String publisherId) { this.publisherId = publisherId; }
        
        public Map<String, Object> getProperties() { return properties; }
        public void setProperties(Map<String, Object> properties) { this.properties = properties; }
        
        public Map<String, String> getUserProperties() { return userProperties; }
        public void setUserProperties(Map<String, String> userProperties) { this.userProperties = userProperties; }
        
        public boolean isExpired() {
            return expiresAt != null && LocalDateTime.now().isAfter(expiresAt);
        }
        
        public int getPayloadSize() {
            return payload != null ? payload.length : 0;
        }
        
        public long getAge() {
            if (createdAt == null) return 0;
            return java.time.Duration.between(createdAt, LocalDateTime.now()).toMillis();
        }
        
        @Override
        public String toString() {
            return String.format("RetainedMessage{topic='%s', qos=%s, payloadSize=%d, age=%dms}", 
                               topic, qos, getPayloadSize(), getAge());
        }
    }
    
    /**
     * 保留消息统计信息
     */
    public static class RetainedMessageStatistics {
        public long totalRetainedMessages = 0;
        public long cachedMessages = 0;
        public long qos0Messages = 0;
        public long qos1Messages = 0;
        public long qos2Messages = 0;
        public long expiredMessages = 0;
        public LocalDateTime lastUpdated = LocalDateTime.now();
        
        @Override
        public String toString() {
            return String.format("RetainedMessageStats{total=%d, cached=%d, qos0=%d, qos1=%d, qos2=%d, expired=%d}", 
                               totalRetainedMessages, cachedMessages, qos0Messages, qos1Messages, qos2Messages, expiredMessages);
        }
    }
}