/**
 * 消息持久化服务实现
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
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

/**
 * 消息持久化服务
 * 负责MQTT消息的持久化存储、检索和管理
 * 支持QoS 0/1/2消息的不同处理策略
 */
@Slf4j
@Service
public class MessagePersistenceService {
    
    @Autowired
    private StorageOperations storageOperations;
    
    private static final int DEFAULT_MESSAGE_BATCH_SIZE = 1000;
    private static final int DEFAULT_EXPIRY_CLEANUP_BATCH_SIZE = 500;
    
    /**
     * 存储消息
     *
     * @param message 排队消息
     * @return 是否成功
     */
    public boolean storeMessage(QueuedMessage message) {
        if (message == null || message.getMessageId() == null) {
            log.warn("Cannot store null message or message without ID");
            return false;
        }
        
        try {
            // 设置创建时间
            if (message.getCreatedAt() == null) {
                message.setCreatedAt(LocalDateTime.now());
            }
            
            // 根据消息类型选择存储策略
            return storeMessageByType(message);
            
        } catch (Exception e) {
            log.error("Failed to store message: messageId={}", message.getMessageId(), e);
            return false;
        }
    }
    
    /**
     * 批量存储消息
     *
     * @param messages 消息列表
     * @return 成功存储的消息数量
     */
    public int batchStoreMessages(List<QueuedMessage> messages) {
        if (messages == null || messages.isEmpty()) {
            return 0;
        }
        
        try {
            StorageOperations.BatchOperationBuilder batch = storageOperations.batchOperation();
            int successCount = 0;
            
            for (QueuedMessage message : messages) {
                if (message != null && message.getMessageId() != null) {
                    if (message.getCreatedAt() == null) {
                        message.setCreatedAt(LocalDateTime.now());
                    }
                    
                    addMessageToBatch(message, batch);
                    successCount++;
                }
            }
            
            if (successCount > 0) {
                batch.execute();
                log.info("Batch stored {} messages", successCount);
            }
            
            return successCount;
            
        } catch (Exception e) {
            log.error("Failed to batch store messages", e);
            return 0;
        }
    }
    
    /**
     * 获取消息
     *
     * @param messageId 消息ID
     * @return 消息Optional
     */
    public Optional<QueuedMessage> getMessage(String messageId) {
        if (messageId == null || messageId.trim().isEmpty()) {
            return Optional.empty();
        }
        
        try {
            String messageKey = StorageKeyspace.messageKey(messageId);
            Optional<QueuedMessage> message = storageOperations.get(
                RocksDBStorageEngine.MESSAGE_CF, messageKey, QueuedMessage.class);
            
            if (message.isPresent()) {
                log.debug("Message retrieved: messageId={}", messageId);
            } else {
                log.debug("Message not found: messageId={}", messageId);
            }
            
            return message;
            
        } catch (Exception e) {
            log.error("Failed to get message: messageId={}", messageId, e);
            return Optional.empty();
        }
    }
    
    /**
     * 删除消息
     *
     * @param messageId 消息ID
     * @return 是否成功
     */
    public boolean deleteMessage(String messageId) {
        if (messageId == null || messageId.trim().isEmpty()) {
            return false;
        }
        
        try {
            // 首先获取消息以了解其类型和位置
            Optional<QueuedMessage> messageOpt = getMessage(messageId);
            if (!messageOpt.isPresent()) {
                log.debug("Message not found for deletion: messageId={}", messageId);
                return true; // 消息不存在，认为删除成功
            }
            
            QueuedMessage message = messageOpt.get();
            
            // 使用批量操作删除消息及其索引
            StorageOperations.BatchOperationBuilder batch = storageOperations.batchOperation();
            
            // 删除主消息记录
            String messageKey = StorageKeyspace.messageKey(messageId);
            batch.delete(RocksDBStorageEngine.MESSAGE_CF, messageKey);
            
            // 删除相关索引
            deleteMessageIndexes(message, batch);
            
            batch.execute();
            
            log.debug("Message deleted successfully: messageId={}", messageId);
            return true;
            
        } catch (Exception e) {
            log.error("Failed to delete message: messageId={}", messageId, e);
            return false;
        }
    }
    
    /**
     * 获取客户端排队消息
     *
     * @param clientId 客户端ID
     * @param limit 限制数量
     * @return 排队消息列表
     */
    public List<QueuedMessage> getQueuedMessages(String clientId, int limit) {
        if (clientId == null || clientId.trim().isEmpty()) {
            return List.of();
        }
        
        try {
            String queuePrefix = StorageKeyspace.MESSAGE_QUEUE_PREFIX + 
                               StorageKeyspace.KEY_SEPARATOR + clientId;
            
            List<StorageOperations.KeyValuePair<QueuedMessage>> queuedMessages = 
                storageOperations.scanByPrefix(RocksDBStorageEngine.MESSAGE_CF, 
                                             queuePrefix, limit, QueuedMessage.class);
            
            List<QueuedMessage> result = queuedMessages.stream()
                    .map(StorageOperations.KeyValuePair::getValue)
                    .filter(msg -> msg.getState() == QueuedMessage.MessageState.QUEUED)
                    .sorted() // 使用消息的自然排序（优先级和时间）
                    .collect(Collectors.toList());
            
            log.debug("Found {} queued messages for client: {}", result.size(), clientId);
            return result;
            
        } catch (Exception e) {
            log.error("Failed to get queued messages: clientId={}", clientId, e);
            return List.of();
        }
    }
    
    /**
     * 获取客户端传输中消息
     *
     * @param clientId 客户端ID
     * @param limit 限制数量
     * @return 传输中消息列表
     */
    public List<QueuedMessage> getInflightMessages(String clientId, int limit) {
        if (clientId == null || clientId.trim().isEmpty()) {
            return List.of();
        }
        
        try {
            String inflightPrefix = StorageKeyspace.MESSAGE_INFLIGHT_PREFIX + 
                                  StorageKeyspace.KEY_SEPARATOR + clientId;
            
            List<StorageOperations.KeyValuePair<QueuedMessage>> inflightMessages = 
                storageOperations.scanByPrefix(RocksDBStorageEngine.MESSAGE_CF, 
                                             inflightPrefix, limit, QueuedMessage.class);
            
            List<QueuedMessage> result = inflightMessages.stream()
                    .map(StorageOperations.KeyValuePair::getValue)
                    .filter(msg -> isInflightState(msg.getState()))
                    .sorted() // 使用消息的自然排序
                    .collect(Collectors.toList());
            
            log.debug("Found {} inflight messages for client: {}", result.size(), clientId);
            return result;
            
        } catch (Exception e) {
            log.error("Failed to get inflight messages: clientId={}", clientId, e);
            return List.of();
        }
    }
    
    /**
     * 将排队消息移动到传输中
     *
     * @param clientId 客户端ID
     * @param packetId 包ID
     * @param message 消息
     * @return 是否成功
     */
    public boolean moveToInflight(String clientId, int packetId, QueuedMessage message) {
        if (clientId == null || message == null || message.getMessageId() == null) {
            log.warn("Invalid parameters for moveToInflight");
            return false;
        }
        
        try {
            StorageOperations.BatchOperationBuilder batch = storageOperations.batchOperation();
            
            // 删除旧的排队消息索引
            deleteQueuedMessageIndex(message, batch);
            
            // 更新消息状态
            message.setPacketId(packetId);
            message.markAsSent(); // 这将根据QoS设置正确的传输中状态
            
            // 添加传输中消息索引
            addInflightMessageIndex(clientId, packetId, message, batch);
            
            // 更新主消息记录
            String messageKey = StorageKeyspace.messageKey(message.getMessageId());
            batch.put(RocksDBStorageEngine.MESSAGE_CF, messageKey, message);
            
            batch.execute();
            
            log.debug("Message moved to inflight: clientId={}, messageId={}, packetId={}", 
                     clientId, message.getMessageId(), packetId);
            return true;
            
        } catch (Exception e) {
            log.error("Failed to move message to inflight: clientId={}, messageId={}", 
                     clientId, message.getMessageId(), e);
            return false;
        }
    }
    
    /**
     * 确认消息（从传输中移除）
     *
     * @param clientId 客户端ID
     * @param packetId 包ID
     * @return 被确认的消息
     */
    public Optional<QueuedMessage> acknowledgeMessage(String clientId, int packetId) {
        if (clientId == null || clientId.trim().isEmpty()) {
            return Optional.empty();
        }
        
        try {
            // 获取传输中消息
            String inflightKey = StorageKeyspace.messageInflightKey(clientId, packetId);
            Optional<QueuedMessage> messageOpt = storageOperations.get(
                RocksDBStorageEngine.MESSAGE_CF, inflightKey, QueuedMessage.class);
            
            if (!messageOpt.isPresent()) {
                log.debug("Inflight message not found: clientId={}, packetId={}", clientId, packetId);
                return Optional.empty();
            }
            
            QueuedMessage message = messageOpt.get();
            
            // 标记为已确认
            message.markAsAcknowledged();
            
            // 删除传输中索引和主消息记录
            StorageOperations.BatchOperationBuilder batch = storageOperations.batchOperation();
            
            // 删除传输中索引
            batch.delete(RocksDBStorageEngine.MESSAGE_CF, inflightKey);
            
            // 删除主消息记录（已完成传递）
            String messageKey = StorageKeyspace.messageKey(message.getMessageId());
            batch.delete(RocksDBStorageEngine.MESSAGE_CF, messageKey);
            
            // 删除其他索引
            deleteMessageIndexes(message, batch);
            
            batch.execute();
            
            log.debug("Message acknowledged: clientId={}, messageId={}, packetId={}", 
                     clientId, message.getMessageId(), packetId);
            return Optional.of(message);
            
        } catch (Exception e) {
            log.error("Failed to acknowledge message: clientId={}, packetId={}", clientId, packetId, e);
            return Optional.empty();
        }
    }
    
    /**
     * 处理QoS 2消息的PUBREC响应
     *
     * @param clientId 客户端ID
     * @param packetId 包ID
     * @return 是否成功
     */
    public boolean handlePubRec(String clientId, int packetId) {
        if (clientId == null || clientId.trim().isEmpty()) {
            return false;
        }
        
        try {
            String inflightKey = StorageKeyspace.messageInflightKey(clientId, packetId);
            Optional<QueuedMessage> messageOpt = storageOperations.get(
                RocksDBStorageEngine.MESSAGE_CF, inflightKey, QueuedMessage.class);
            
            if (!messageOpt.isPresent()) {
                log.debug("Inflight message not found for PUBREC: clientId={}, packetId={}", clientId, packetId);
                return false;
            }
            
            QueuedMessage message = messageOpt.get();
            
            // 更新消息状态
            message.handlePubRec();
            
            // 更新存储
            storageOperations.put(RocksDBStorageEngine.MESSAGE_CF, inflightKey, message);
            
            log.debug("PUBREC handled: clientId={}, messageId={}, packetId={}", 
                     clientId, message.getMessageId(), packetId);
            return true;
            
        } catch (Exception e) {
            log.error("Failed to handle PUBREC: clientId={}, packetId={}", clientId, packetId, e);
            return false;
        }
    }
    
    /**
     * 处理QoS 2消息的PUBCOMP响应
     *
     * @param clientId 客户端ID
     * @param packetId 包ID
     * @return 被完成的消息
     */
    public Optional<QueuedMessage> handlePubComp(String clientId, int packetId) {
        if (clientId == null || clientId.trim().isEmpty()) {
            return Optional.empty();
        }
        
        try {
            String inflightKey = StorageKeyspace.messageInflightKey(clientId, packetId);
            Optional<QueuedMessage> messageOpt = storageOperations.get(
                RocksDBStorageEngine.MESSAGE_CF, inflightKey, QueuedMessage.class);
            
            if (!messageOpt.isPresent()) {
                log.debug("Inflight message not found for PUBCOMP: clientId={}, packetId={}", clientId, packetId);
                return Optional.empty();
            }
            
            QueuedMessage message = messageOpt.get();
            
            // 更新消息状态
            message.handlePubComp();
            
            // 删除传输中消息（QoS 2完成）
            StorageOperations.BatchOperationBuilder batch = storageOperations.batchOperation();
            
            batch.delete(RocksDBStorageEngine.MESSAGE_CF, inflightKey);
            
            String messageKey = StorageKeyspace.messageKey(message.getMessageId());
            batch.delete(RocksDBStorageEngine.MESSAGE_CF, messageKey);
            
            deleteMessageIndexes(message, batch);
            
            batch.execute();
            
            log.debug("PUBCOMP handled: clientId={}, messageId={}, packetId={}", 
                     clientId, message.getMessageId(), packetId);
            return Optional.of(message);
            
        } catch (Exception e) {
            log.error("Failed to handle PUBCOMP: clientId={}, packetId={}", clientId, packetId, e);
            return Optional.empty();
        }
    }
    
    /**
     * 获取过期消息
     *
     * @param currentTime 当前时间
     * @param limit 限制数量
     * @return 过期消息列表
     */
    public List<QueuedMessage> getExpiredMessages(LocalDateTime currentTime, int limit) {
        try {
            long currentTimestamp = System.currentTimeMillis();
            String startKey = StorageKeyspace.MESSAGE_EXPIRY_PREFIX + StorageKeyspace.KEY_SEPARATOR;
            String endKey = StorageKeyspace.messageExpiryKey(currentTimestamp, "");
            
            List<StorageOperations.KeyValuePair<QueuedMessage>> expiredMessages = 
                storageOperations.scan(RocksDBStorageEngine.MESSAGE_CF, 
                                     startKey, endKey, limit, QueuedMessage.class);
            
            List<QueuedMessage> result = expiredMessages.stream()
                    .map(StorageOperations.KeyValuePair::getValue)
                    .filter(msg -> msg.isExpired())
                    .collect(Collectors.toList());
            
            log.debug("Found {} expired messages", result.size());
            return result;
            
        } catch (Exception e) {
            log.error("Failed to get expired messages", e);
            return List.of();
        }
    }
    
    /**
     * 清理过期消息
     *
     * @param batchSize 批处理大小
     * @return 清理的消息数量
     */
    public int cleanupExpiredMessages(int batchSize) {
        List<QueuedMessage> expiredMessages = getExpiredMessages(LocalDateTime.now(), batchSize);
        
        int cleanedCount = 0;
        for (QueuedMessage message : expiredMessages) {
            if (deleteMessage(message.getMessageId())) {
                cleanedCount++;
            }
        }
        
        if (cleanedCount > 0) {
            log.info("Cleaned up {} expired messages", cleanedCount);
        }
        
        return cleanedCount;
    }
    
    /**
     * 异步存储消息
     *
     * @param message 消息
     * @return CompletableFuture<Boolean>
     */
    @Async
    public CompletableFuture<Boolean> storeMessageAsync(QueuedMessage message) {
        return CompletableFuture.completedFuture(storeMessage(message));
    }
    
    /**
     * 异步获取消息
     *
     * @param messageId 消息ID
     * @return CompletableFuture<Optional<QueuedMessage>>
     */
    @Async
    public CompletableFuture<Optional<QueuedMessage>> getMessageAsync(String messageId) {
        return CompletableFuture.completedFuture(getMessage(messageId));
    }
    
    /**
     * 获取消息统计信息
     *
     * @return 消息统计
     */
    public MessageStatistics getMessageStatistics() {
        try {
            MessageStatistics stats = new MessageStatistics();
            
            // 获取总消息数
            stats.totalMessages = storageOperations.getApproximateKeyCount(RocksDBStorageEngine.MESSAGE_CF);
            
            // 这里可以添加更详细的统计逻辑
            // 由于性能考虑，暂时使用简化实现
            
            return stats;
            
        } catch (Exception e) {
            log.error("Failed to get message statistics", e);
            return new MessageStatistics();
        }
    }
    
    /**
     * 根据消息类型存储消息
     */
    private boolean storeMessageByType(QueuedMessage message) throws StorageOperations.StorageException {
        StorageOperations.BatchOperationBuilder batch = storageOperations.batchOperation();
        
        // 存储主消息记录
        String messageKey = StorageKeyspace.messageKey(message.getMessageId());
        batch.put(RocksDBStorageEngine.MESSAGE_CF, messageKey, message);
        
        // 根据消息状态添加相应索引
        switch (message.getState()) {
            case QUEUED:
                addQueuedMessageIndex(message, batch);
                break;
            case SENDING:
            case SENT_WAITING_PUBACK:
            case SENT_WAITING_PUBREC:
            case RECEIVED_PUBREC_WAITING_PUBCOMP:
                if (message.getClientId() != null && message.getPacketId() != null) {
                    addInflightMessageIndex(message.getClientId(), message.getPacketId(), message, batch);
                }
                break;
            default:
                // 其他状态的消息只存储主记录
                break;
        }
        
        // 如果消息有过期时间，添加过期索引
        if (message.getExpiresAt() != null) {
            addMessageExpiryIndex(message, batch);
        }
        
        batch.execute();
        return true;
    }
    
    /**
     * 添加消息到批量操作
     */
    private void addMessageToBatch(QueuedMessage message, StorageOperations.BatchOperationBuilder batch) {
        String messageKey = StorageKeyspace.messageKey(message.getMessageId());
        batch.put(RocksDBStorageEngine.MESSAGE_CF, messageKey, message);
        
        // 添加索引
        switch (message.getState()) {
            case QUEUED:
                addQueuedMessageIndexToBatch(message, batch);
                break;
            case SENDING:
            case SENT_WAITING_PUBACK:
            case SENT_WAITING_PUBREC:
            case RECEIVED_PUBREC_WAITING_PUBCOMP:
                if (message.getClientId() != null && message.getPacketId() != null) {
                    addInflightMessageIndexToBatch(message.getClientId(), message.getPacketId(), message, batch);
                }
                break;
        }
        
        if (message.getExpiresAt() != null) {
            addMessageExpiryIndexToBatch(message, batch);
        }
    }
    
    /**
     * 添加排队消息索引
     */
    private void addQueuedMessageIndex(QueuedMessage message, StorageOperations.BatchOperationBuilder batch) 
            throws StorageOperations.StorageException {
        if (message.getClientId() != null) {
            long timestamp = System.currentTimeMillis();
            String queueKey = StorageKeyspace.messageQueueKey(
                message.getClientId(), message.getPriority(), timestamp, message.getMessageId());
            batch.put(RocksDBStorageEngine.MESSAGE_CF, queueKey, message);
        }
    }
    
    /**
     * 添加排队消息索引到批量操作
     */
    private void addQueuedMessageIndexToBatch(QueuedMessage message, StorageOperations.BatchOperationBuilder batch) {
        if (message.getClientId() != null) {
            long timestamp = System.currentTimeMillis();
            String queueKey = StorageKeyspace.messageQueueKey(
                message.getClientId(), message.getPriority(), timestamp, message.getMessageId());
            batch.put(RocksDBStorageEngine.MESSAGE_CF, queueKey, message);
        }
    }
    
    /**
     * 添加传输中消息索引
     */
    private void addInflightMessageIndex(String clientId, int packetId, QueuedMessage message, 
                                       StorageOperations.BatchOperationBuilder batch) {
        String inflightKey = StorageKeyspace.messageInflightKey(clientId, packetId);
        batch.put(RocksDBStorageEngine.MESSAGE_CF, inflightKey, message);
    }
    
    /**
     * 添加传输中消息索引到批量操作
     */
    private void addInflightMessageIndexToBatch(String clientId, int packetId, QueuedMessage message, 
                                              StorageOperations.BatchOperationBuilder batch) {
        String inflightKey = StorageKeyspace.messageInflightKey(clientId, packetId);
        batch.put(RocksDBStorageEngine.MESSAGE_CF, inflightKey, message);
    }
    
    /**
     * 添加消息过期索引
     */
    private void addMessageExpiryIndex(QueuedMessage message, StorageOperations.BatchOperationBuilder batch) {
        if (message.getExpiresAt() != null) {
            long expiryTimestamp = java.time.ZoneId.systemDefault().getRules()
                    .getOffset(message.getExpiresAt()).getTotalSeconds() * 1000;
            String expiryKey = StorageKeyspace.messageExpiryKey(expiryTimestamp, message.getMessageId());
            batch.put(RocksDBStorageEngine.MESSAGE_CF, expiryKey, message);
        }
    }
    
    /**
     * 添加消息过期索引到批量操作
     */
    private void addMessageExpiryIndexToBatch(QueuedMessage message, StorageOperations.BatchOperationBuilder batch) {
        if (message.getExpiresAt() != null) {
            long expiryTimestamp = java.time.ZoneId.systemDefault().getRules()
                    .getOffset(message.getExpiresAt()).getTotalSeconds() * 1000;
            String expiryKey = StorageKeyspace.messageExpiryKey(expiryTimestamp, message.getMessageId());
            batch.put(RocksDBStorageEngine.MESSAGE_CF, expiryKey, message);
        }
    }
    
    /**
     * 删除排队消息索引
     */
    private void deleteQueuedMessageIndex(QueuedMessage message, StorageOperations.BatchOperationBuilder batch) {
        if (message.getClientId() != null) {
            // 需要扫描并删除对应的排队索引
            // 这里简化实现，实际项目中可能需要维护反向索引
            try {
                String queuePrefix = StorageKeyspace.MESSAGE_QUEUE_PREFIX + 
                                   StorageKeyspace.KEY_SEPARATOR + message.getClientId();
                List<StorageOperations.KeyValuePair<QueuedMessage>> queuedMessages = 
                    storageOperations.scanByPrefix(RocksDBStorageEngine.MESSAGE_CF, queuePrefix, 100, QueuedMessage.class);
                
                for (StorageOperations.KeyValuePair<QueuedMessage> kvp : queuedMessages) {
                    if (message.getMessageId().equals(kvp.getValue().getMessageId())) {
                        batch.delete(RocksDBStorageEngine.MESSAGE_CF, kvp.getKey());
                        break;
                    }
                }
            } catch (Exception e) {
                log.warn("Failed to delete queued message index for messageId: {}", message.getMessageId(), e);
            }
        }
    }
    
    /**
     * 删除消息相关索引
     */
    private void deleteMessageIndexes(QueuedMessage message, StorageOperations.BatchOperationBuilder batch) {
        // 删除过期索引
        if (message.getExpiresAt() != null) {
            long expiryTimestamp = java.time.ZoneId.systemDefault().getRules()
                    .getOffset(message.getExpiresAt()).getTotalSeconds() * 1000;
            String expiryKey = StorageKeyspace.messageExpiryKey(expiryTimestamp, message.getMessageId());
            batch.delete(RocksDBStorageEngine.MESSAGE_CF, expiryKey);
        }
        
        // 删除其他可能的索引
        // 根据消息状态删除相应索引
        switch (message.getState()) {
            case QUEUED:
                deleteQueuedMessageIndex(message, batch);
                break;
            case SENDING:
            case SENT_WAITING_PUBACK:
            case SENT_WAITING_PUBREC:
            case RECEIVED_PUBREC_WAITING_PUBCOMP:
                if (message.getClientId() != null && message.getPacketId() != null) {
                    String inflightKey = StorageKeyspace.messageInflightKey(message.getClientId(), message.getPacketId());
                    batch.delete(RocksDBStorageEngine.MESSAGE_CF, inflightKey);
                }
                break;
        }
    }
    
    /**
     * 检查是否为传输中状态
     */
    private boolean isInflightState(QueuedMessage.MessageState state) {
        return state == QueuedMessage.MessageState.SENDING ||
               state == QueuedMessage.MessageState.SENT_WAITING_PUBACK ||
               state == QueuedMessage.MessageState.SENT_WAITING_PUBREC ||
               state == QueuedMessage.MessageState.RECEIVED_PUBREC_WAITING_PUBCOMP;
    }
    
    /**
     * 消息统计信息
     */
    public static class MessageStatistics {
        public long totalMessages = 0;
        public long queuedMessages = 0;
        public long inflightMessages = 0;
        public long expiredMessages = 0;
        public long failedMessages = 0;
        public LocalDateTime lastUpdated = LocalDateTime.now();
        
        @Override
        public String toString() {
            return String.format("MessageStats{total=%d, queued=%d, inflight=%d, expired=%d, failed=%d}", 
                               totalMessages, queuedMessages, inflightMessages, expiredMessages, failedMessages);
        }
    }
}