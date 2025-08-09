/**
 * 遗嘱消息处理服务实现
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
import java.util.stream.Collectors;

/**
 * 遗嘱消息处理服务
 * 负责MQTT遗嘱消息的存储、管理和发布
 * 在客户端异常断开时触发遗嘱消息的发送
 */
@Slf4j
@Service
public class WillMessageService {
    
    @Autowired
    private StorageOperations storageOperations;
    
    @Autowired
    private RetainedMessageService retainedMessageService;
    
    @Autowired
    private SubscriptionPersistenceService subscriptionService;
    
    @Autowired
    private MessagePersistenceService messageService;
    
    /**
     * 存储遗嘱消息
     *
     * @param clientId 客户端ID
     * @param willMessage 遗嘱消息
     * @return 是否成功
     */
    public boolean storeWillMessage(String clientId, WillMessage willMessage) {
        if (clientId == null || clientId.trim().isEmpty() || willMessage == null) {
            log.warn("Cannot store will message with null clientId or willMessage");
            return false;
        }
        
        try {
            // 设置存储时间
            willMessage.setStoredAt(LocalDateTime.now());
            willMessage.setClientId(clientId);
            
            // 计算延迟时间（MQTT 5.0特性）
            if (willMessage.getWillDelayInterval() > 0) {
                LocalDateTime executeAt = LocalDateTime.now().plusSeconds(willMessage.getWillDelayInterval());
                willMessage.setScheduledExecuteAt(executeAt);
            }
            
            String willKey = StorageKeyspace.willMessageKey(clientId);
            storageOperations.put(RocksDBStorageEngine.METADATA_CF, willKey, willMessage);
            
            log.debug("Will message stored: clientId={}, topic={}, qos={}, delayInterval={}", 
                     clientId, willMessage.getTopic(), willMessage.getQos(), willMessage.getWillDelayInterval());
            return true;
            
        } catch (Exception e) {
            log.error("Failed to store will message: clientId={}", clientId, e);
            return false;
        }
    }
    
    /**
     * 获取遗嘱消息
     *
     * @param clientId 客户端ID
     * @return 遗嘱消息Optional
     */
    public Optional<WillMessage> getWillMessage(String clientId) {
        if (clientId == null || clientId.trim().isEmpty()) {
            return Optional.empty();
        }
        
        try {
            String willKey = StorageKeyspace.willMessageKey(clientId);
            Optional<WillMessage> willMessage = storageOperations.get(
                RocksDBStorageEngine.METADATA_CF, willKey, WillMessage.class);
            
            if (willMessage.isPresent()) {
                log.debug("Will message retrieved: clientId={}", clientId);
            }
            
            return willMessage;
            
        } catch (Exception e) {
            log.error("Failed to get will message: clientId={}", clientId, e);
            return Optional.empty();
        }
    }
    
    /**
     * 移除遗嘱消息
     *
     * @param clientId 客户端ID
     * @return 被移除的遗嘱消息
     */
    public Optional<WillMessage> removeWillMessage(String clientId) {
        if (clientId == null || clientId.trim().isEmpty()) {
            return Optional.empty();
        }
        
        try {
            // 首先获取遗嘱消息
            Optional<WillMessage> willMessageOpt = getWillMessage(clientId);
            
            if (willMessageOpt.isPresent()) {
                // 删除遗嘱消息
                String willKey = StorageKeyspace.willMessageKey(clientId);
                storageOperations.delete(RocksDBStorageEngine.METADATA_CF, willKey);
                
                log.debug("Will message removed: clientId={}", clientId);
                return willMessageOpt;
            }
            
            return Optional.empty();
            
        } catch (Exception e) {
            log.error("Failed to remove will message: clientId={}", clientId, e);
            return Optional.empty();
        }
    }
    
    /**
     * 执行遗嘱消息发布
     * 当客户端异常断开时调用
     *
     * @param clientId 客户端ID
     * @param reason 断开原因
     * @return 发布结果
     */
    public WillPublishResult publishWillMessage(String clientId, DisconnectReason reason) {
        if (clientId == null || clientId.trim().isEmpty()) {
            return WillPublishResult.failed("Invalid clientId");
        }
        
        try {
            // 获取遗嘱消息
            Optional<WillMessage> willMessageOpt = getWillMessage(clientId);
            if (!willMessageOpt.isPresent()) {
                log.debug("No will message found for client: {}", clientId);
                return WillPublishResult.noWillMessage();
            }
            
            WillMessage willMessage = willMessageOpt.get();
            
            // 检查是否需要发布遗嘱消息
            if (!shouldPublishWillMessage(willMessage, reason)) {
                log.debug("Will message should not be published for client: {} reason: {}", clientId, reason);
                removeWillMessage(clientId); // 清理遗嘱消息
                return WillPublishResult.suppressed(reason);
            }
            
            // 检查延迟发布
            if (willMessage.getWillDelayInterval() > 0 && willMessage.getScheduledExecuteAt() != null) {
                LocalDateTime now = LocalDateTime.now();
                if (now.isBefore(willMessage.getScheduledExecuteAt())) {
                    log.debug("Will message delayed until: {} for client: {}", 
                             willMessage.getScheduledExecuteAt(), clientId);
                    // 这里应该有定时任务来处理延迟发布
                    return WillPublishResult.delayed(willMessage.getScheduledExecuteAt());
                }
            }
            
            // 执行遗嘱消息发布
            PublishResult publishResult = executeWillMessagePublish(willMessage);
            
            // 清理遗嘱消息
            removeWillMessage(clientId);
            
            log.info("Will message published: clientId={}, topic={}, subscribers={}, reason={}", 
                    clientId, willMessage.getTopic(), publishResult.getSubscriberCount(), reason);
            
            return WillPublishResult.success(publishResult);
            
        } catch (Exception e) {
            log.error("Failed to publish will message: clientId={}, reason={}", clientId, reason, e);
            return WillPublishResult.failed("Internal error: " + e.getMessage());
        }
    }
    
    /**
     * 批量清理过期的遗嘱消息
     *
     * @param batchSize 批处理大小
     * @return 清理的数量
     */
    public int cleanupExpiredWillMessages(int batchSize) {
        try {
            List<String> expiredClients = getExpiredWillMessageClients(batchSize);
            
            int cleanedCount = 0;
            for (String clientId : expiredClients) {
                if (removeWillMessage(clientId).isPresent()) {
                    cleanedCount++;
                }
            }
            
            if (cleanedCount > 0) {
                log.info("Cleaned up {} expired will messages", cleanedCount);
            }
            
            return cleanedCount;
            
        } catch (Exception e) {
            log.error("Failed to cleanup expired will messages", e);
            return 0;
        }
    }
    
    /**
     * 获取待发布的延迟遗嘱消息
     *
     * @param limit 限制数量
     * @return 待发布的遗嘱消息列表
     */
    public List<WillMessage> getPendingDelayedWillMessages(int limit) {
        try {
            LocalDateTime now = LocalDateTime.now();
            List<WillMessage> pendingMessages = new ArrayList<>();
            
            // 扫描所有遗嘱消息
            String willPrefix = StorageKeyspace.WILL_MESSAGE_PREFIX + StorageKeyspace.KEY_SEPARATOR;
            List<StorageOperations.KeyValuePair<WillMessage>> willMessages = 
                storageOperations.scanByPrefix(RocksDBStorageEngine.METADATA_CF, 
                                             willPrefix, limit * 2, WillMessage.class);
            
            for (StorageOperations.KeyValuePair<WillMessage> kvp : willMessages) {
                if (pendingMessages.size() >= limit) {
                    break;
                }
                
                WillMessage willMessage = kvp.getValue();
                if (willMessage != null && willMessage.getScheduledExecuteAt() != null &&
                    now.isAfter(willMessage.getScheduledExecuteAt())) {
                    pendingMessages.add(willMessage);
                }
            }
            
            log.debug("Found {} pending delayed will messages", pendingMessages.size());
            return pendingMessages;
            
        } catch (Exception e) {
            log.error("Failed to get pending delayed will messages", e);
            return List.of();
        }
    }
    
    /**
     * 处理延迟遗嘱消息的发布
     *
     * @return 处理的消息数量
     */
    public int processDelayedWillMessages() {
        try {
            List<WillMessage> pendingMessages = getPendingDelayedWillMessages(100);
            
            int processedCount = 0;
            for (WillMessage willMessage : pendingMessages) {
                try {
                    // 执行发布
                    PublishResult publishResult = executeWillMessagePublish(willMessage);
                    
                    // 清理遗嘱消息
                    removeWillMessage(willMessage.getClientId());
                    
                    log.info("Delayed will message published: clientId={}, topic={}, subscribers={}", 
                            willMessage.getClientId(), willMessage.getTopic(), publishResult.getSubscriberCount());
                    
                    processedCount++;
                    
                } catch (Exception e) {
                    log.error("Failed to process delayed will message: clientId={}", 
                             willMessage.getClientId(), e);
                }
            }
            
            if (processedCount > 0) {
                log.info("Processed {} delayed will messages", processedCount);
            }
            
            return processedCount;
            
        } catch (Exception e) {
            log.error("Failed to process delayed will messages", e);
            return 0;
        }
    }
    
    /**
     * 异步发布遗嘱消息
     *
     * @param clientId 客户端ID
     * @param reason 断开原因
     * @return CompletableFuture<WillPublishResult>
     */
    @Async
    public CompletableFuture<WillPublishResult> publishWillMessageAsync(String clientId, DisconnectReason reason) {
        return CompletableFuture.completedFuture(publishWillMessage(clientId, reason));
    }
    
    /**
     * 获取遗嘱消息统计信息
     *
     * @return 遗嘱消息统计
     */
    public WillMessageStatistics getWillMessageStatistics() {
        try {
            WillMessageStatistics stats = new WillMessageStatistics();
            
            // 扫描所有遗嘱消息进行统计
            String willPrefix = StorageKeyspace.WILL_MESSAGE_PREFIX + StorageKeyspace.KEY_SEPARATOR;
            List<StorageOperations.KeyValuePair<WillMessage>> willMessages = 
                storageOperations.scanByPrefix(RocksDBStorageEngine.METADATA_CF, 
                                             willPrefix, 10000, WillMessage.class);
            
            stats.totalWillMessages = willMessages.size();
            
            LocalDateTime now = LocalDateTime.now();
            for (StorageOperations.KeyValuePair<WillMessage> kvp : willMessages) {
                WillMessage willMessage = kvp.getValue();
                if (willMessage != null) {
                    if (willMessage.getWillDelayInterval() > 0) {
                        stats.delayedWillMessages++;
                    }
                    
                    if (willMessage.getScheduledExecuteAt() != null && 
                        now.isAfter(willMessage.getScheduledExecuteAt())) {
                        stats.pendingWillMessages++;
                    }
                    
                    switch (willMessage.getQos()) {
                        case AT_MOST_ONCE:
                            stats.qos0WillMessages++;
                            break;
                        case AT_LEAST_ONCE:
                            stats.qos1WillMessages++;
                            break;
                        case EXACTLY_ONCE:
                            stats.qos2WillMessages++;
                            break;
                    }
                }
            }
            
            return stats;
            
        } catch (Exception e) {
            log.error("Failed to get will message statistics", e);
            return new WillMessageStatistics();
        }
    }
    
    /**
     * 执行遗嘱消息发布的具体逻辑
     */
    private PublishResult executeWillMessagePublish(WillMessage willMessage) throws Exception {
        // 查找匹配的订阅
        List<com.vmqtt.common.model.TopicSubscription> matchingSubscriptions = 
            subscriptionService.findMatchingSubscriptions(willMessage.getTopic(), 1000);
        
        PublishResult result = new PublishResult();
        result.setTopic(willMessage.getTopic());
        result.setSubscriberCount(matchingSubscriptions.size());
        result.setPublishedAt(LocalDateTime.now());
        
        if (matchingSubscriptions.isEmpty()) {
            log.debug("No subscribers found for will message topic: {}", willMessage.getTopic());
        } else {
            // 为每个订阅者创建消息
            List<QueuedMessage> messages = new ArrayList<>();
            
            for (com.vmqtt.common.model.TopicSubscription subscription : matchingSubscriptions) {
                // 检查noLocal选项
                if (!subscription.shouldReceive(willMessage.getClientId())) {
                    continue;
                }
                
                // 创建消息
                QueuedMessage message = QueuedMessage.builder()
                        .messageId(UUID.randomUUID().toString())
                        .clientId(subscription.getClientId())
                        .sessionId(subscription.getSessionId())
                        .topic(willMessage.getTopic())
                        .payload(willMessage.getPayload())
                        .qos(subscription.getEffectiveQos(willMessage.getQos()))
                        .retain(willMessage.isRetain())
                        .createdAt(LocalDateTime.now())
                        .queuedAt(LocalDateTime.now())
                        .state(QueuedMessage.MessageState.QUEUED)
                        .properties(willMessage.getProperties())
                        .userProperties(willMessage.getUserProperties())
                        .build();
                
                messages.add(message);
                result.incrementDeliveredCount();
            }
            
            // 批量存储消息
            if (!messages.isEmpty()) {
                int storedCount = messageService.batchStoreMessages(messages);
                result.setStoredMessageCount(storedCount);
            }
        }
        
        // 如果是保留消息，存储到保留消息存储
        if (willMessage.isRetain()) {
            QueuedMessage retainedMessage = QueuedMessage.builder()
                    .messageId(UUID.randomUUID().toString())
                    .clientId(willMessage.getClientId())
                    .topic(willMessage.getTopic())
                    .payload(willMessage.getPayload())
                    .qos(willMessage.getQos())
                    .retain(true)
                    .createdAt(LocalDateTime.now())
                    .properties(willMessage.getProperties())
                    .userProperties(willMessage.getUserProperties())
                    .build();
            
            boolean retainedStored = retainedMessageService.storeRetainedMessage(
                willMessage.getTopic(), retainedMessage);
            result.setRetainedMessageStored(retainedStored);
        }
        
        return result;
    }
    
    /**
     * 判断是否应该发布遗嘱消息
     */
    private boolean shouldPublishWillMessage(WillMessage willMessage, DisconnectReason reason) {
        // 根据MQTT协议规范，只有在异常断开时才发布遗嘱消息
        switch (reason) {
            case NORMAL_DISCONNECT:
                return false; // 正常断开不发布遗嘱消息
            case NETWORK_ERROR:
            case KEEP_ALIVE_TIMEOUT:
            case PROTOCOL_ERROR:
            case SERVER_SHUTDOWN:
            case SESSION_TAKEOVER:
            case AUTHENTICATION_ERROR:
            case AUTHORIZATION_ERROR:
                return true; // 异常断开发布遗嘱消息
            default:
                return true;
        }
    }
    
    /**
     * 获取过期的遗嘱消息客户端列表
     */
    private List<String> getExpiredWillMessageClients(int limit) throws Exception {
        // 这里应该实现基于会话生命周期的过期逻辑
        // 简化实现：返回存储时间超过24小时的遗嘱消息
        LocalDateTime expiredBefore = LocalDateTime.now().minusHours(24);
        
        List<String> expiredClients = new ArrayList<>();
        
        String willPrefix = StorageKeyspace.WILL_MESSAGE_PREFIX + StorageKeyspace.KEY_SEPARATOR;
        List<StorageOperations.KeyValuePair<WillMessage>> willMessages = 
            storageOperations.scanByPrefix(RocksDBStorageEngine.METADATA_CF, 
                                         willPrefix, limit * 2, WillMessage.class);
        
        for (StorageOperations.KeyValuePair<WillMessage> kvp : willMessages) {
            if (expiredClients.size() >= limit) {
                break;
            }
            
            WillMessage willMessage = kvp.getValue();
            if (willMessage != null && willMessage.getStoredAt() != null &&
                willMessage.getStoredAt().isBefore(expiredBefore)) {
                expiredClients.add(willMessage.getClientId());
            }
        }
        
        return expiredClients;
    }
    
    /**
     * 遗嘱消息内部类
     */
    public static class WillMessage {
        private String clientId;
        private String topic;
        private byte[] payload;
        private MqttQos qos;
        private boolean retain;
        private long willDelayInterval; // MQTT 5.0特性
        private LocalDateTime storedAt;
        private LocalDateTime scheduledExecuteAt;
        private Map<String, Object> properties = new HashMap<>();
        private Map<String, String> userProperties = new HashMap<>();
        
        // Getters and Setters
        public String getClientId() { return clientId; }
        public void setClientId(String clientId) { this.clientId = clientId; }
        
        public String getTopic() { return topic; }
        public void setTopic(String topic) { this.topic = topic; }
        
        public byte[] getPayload() { return payload; }
        public void setPayload(byte[] payload) { this.payload = payload; }
        
        public MqttQos getQos() { return qos; }
        public void setQos(MqttQos qos) { this.qos = qos; }
        
        public boolean isRetain() { return retain; }
        public void setRetain(boolean retain) { this.retain = retain; }
        
        public long getWillDelayInterval() { return willDelayInterval; }
        public void setWillDelayInterval(long willDelayInterval) { this.willDelayInterval = willDelayInterval; }
        
        public LocalDateTime getStoredAt() { return storedAt; }
        public void setStoredAt(LocalDateTime storedAt) { this.storedAt = storedAt; }
        
        public LocalDateTime getScheduledExecuteAt() { return scheduledExecuteAt; }
        public void setScheduledExecuteAt(LocalDateTime scheduledExecuteAt) { this.scheduledExecuteAt = scheduledExecuteAt; }
        
        public Map<String, Object> getProperties() { return properties; }
        public void setProperties(Map<String, Object> properties) { this.properties = properties; }
        
        public Map<String, String> getUserProperties() { return userProperties; }
        public void setUserProperties(Map<String, String> userProperties) { this.userProperties = userProperties; }
        
        @Override
        public String toString() {
            return String.format("WillMessage{clientId='%s', topic='%s', qos=%s, retain=%s, delayInterval=%d}", 
                               clientId, topic, qos, retain, willDelayInterval);
        }
    }
    
    /**
     * 断开原因枚举
     */
    public enum DisconnectReason {
        NORMAL_DISCONNECT,
        NETWORK_ERROR,
        KEEP_ALIVE_TIMEOUT,
        PROTOCOL_ERROR,
        SERVER_SHUTDOWN,
        SESSION_TAKEOVER,
        AUTHENTICATION_ERROR,
        AUTHORIZATION_ERROR
    }
    
    /**
     * 遗嘱消息发布结果
     */
    public static class WillPublishResult {
        private boolean success;
        private String errorMessage;
        private PublishResult publishResult;
        private LocalDateTime delayedUntil;
        private boolean suppressed;
        private DisconnectReason suppressReason;
        
        public static WillPublishResult success(PublishResult publishResult) {
            WillPublishResult result = new WillPublishResult();
            result.success = true;
            result.publishResult = publishResult;
            return result;
        }
        
        public static WillPublishResult failed(String errorMessage) {
            WillPublishResult result = new WillPublishResult();
            result.success = false;
            result.errorMessage = errorMessage;
            return result;
        }
        
        public static WillPublishResult noWillMessage() {
            WillPublishResult result = new WillPublishResult();
            result.success = true;
            result.errorMessage = "No will message";
            return result;
        }
        
        public static WillPublishResult delayed(LocalDateTime delayedUntil) {
            WillPublishResult result = new WillPublishResult();
            result.success = true;
            result.delayedUntil = delayedUntil;
            return result;
        }
        
        public static WillPublishResult suppressed(DisconnectReason reason) {
            WillPublishResult result = new WillPublishResult();
            result.success = true;
            result.suppressed = true;
            result.suppressReason = reason;
            return result;
        }
        
        // Getters
        public boolean isSuccess() { return success; }
        public String getErrorMessage() { return errorMessage; }
        public PublishResult getPublishResult() { return publishResult; }
        public LocalDateTime getDelayedUntil() { return delayedUntil; }
        public boolean isSuppressed() { return suppressed; }
        public DisconnectReason getSuppressReason() { return suppressReason; }
        
        @Override
        public String toString() {
            if (!success) {
                return String.format("WillPublishResult{success=false, error='%s'}", errorMessage);
            } else if (suppressed) {
                return String.format("WillPublishResult{suppressed=true, reason=%s}", suppressReason);
            } else if (delayedUntil != null) {
                return String.format("WillPublishResult{delayed=true, until=%s}", delayedUntil);
            } else {
                return String.format("WillPublishResult{success=true, subscribers=%d}", 
                                   publishResult != null ? publishResult.getSubscriberCount() : 0);
            }
        }
    }
    
    /**
     * 发布结果
     */
    public static class PublishResult {
        private String topic;
        private int subscriberCount;
        private int deliveredCount;
        private int storedMessageCount;
        private boolean retainedMessageStored;
        private LocalDateTime publishedAt;
        
        public String getTopic() { return topic; }
        public void setTopic(String topic) { this.topic = topic; }
        
        public int getSubscriberCount() { return subscriberCount; }
        public void setSubscriberCount(int subscriberCount) { this.subscriberCount = subscriberCount; }
        
        public int getDeliveredCount() { return deliveredCount; }
        public void setDeliveredCount(int deliveredCount) { this.deliveredCount = deliveredCount; }
        public void incrementDeliveredCount() { this.deliveredCount++; }
        
        public int getStoredMessageCount() { return storedMessageCount; }
        public void setStoredMessageCount(int storedMessageCount) { this.storedMessageCount = storedMessageCount; }
        
        public boolean isRetainedMessageStored() { return retainedMessageStored; }
        public void setRetainedMessageStored(boolean retainedMessageStored) { this.retainedMessageStored = retainedMessageStored; }
        
        public LocalDateTime getPublishedAt() { return publishedAt; }
        public void setPublishedAt(LocalDateTime publishedAt) { this.publishedAt = publishedAt; }
    }
    
    /**
     * 遗嘱消息统计信息
     */
    public static class WillMessageStatistics {
        public long totalWillMessages = 0;
        public long delayedWillMessages = 0;
        public long pendingWillMessages = 0;
        public long qos0WillMessages = 0;
        public long qos1WillMessages = 0;
        public long qos2WillMessages = 0;
        public LocalDateTime lastUpdated = LocalDateTime.now();
        
        @Override
        public String toString() {
            return String.format("WillMessageStats{total=%d, delayed=%d, pending=%d, qos0=%d, qos1=%d, qos2=%d}", 
                               totalWillMessages, delayedWillMessages, pendingWillMessages, 
                               qos0WillMessages, qos1WillMessages, qos2WillMessages);
        }
    }
}