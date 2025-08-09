/**
 * QoS消息管理器
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
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

/**
 * QoS消息管理器
 * 负责处理MQTT不同QoS级别消息的可靠传递
 * 实现QoS 0、QoS 1、QoS 2的完整处理流程
 */
@Slf4j
@Service
public class QoSMessageManager {
    
    @Autowired
    private StorageOperations storageOperations;
    
    @Autowired
    private MessagePersistenceService messagePersistenceService;
    
    // QoS状态缓存，提高处理性能
    private final Map<String, QoSState> qosStateCache = new ConcurrentHashMap<>();
    
    // 重试配置
    private static final int DEFAULT_MAX_RETRIES = 3;
    private static final long DEFAULT_RETRY_INTERVAL_MS = 5000L; // 5秒
    private static final long DEFAULT_QOS2_TIMEOUT_MS = 60000L; // 1分钟
    
    /**
     * 处理QoS 0消息（At Most Once）
     * 不需要确认，直接发送
     *
     * @param message 消息
     * @return 处理结果
     */
    public QoSProcessResult processQoS0Message(QueuedMessage message) {
        if (message == null || message.getQos() != MqttQos.AT_MOST_ONCE) {
            return QoSProcessResult.failed("Invalid QoS 0 message");
        }
        
        try {
            // QoS 0消息直接标记为已确认
            message.markAsAcknowledged();
            
            log.debug("QoS 0 message processed: messageId={}, clientId={}", 
                     message.getMessageId(), message.getClientId());
            
            return QoSProcessResult.success(message, null);
            
        } catch (Exception e) {
            log.error("Failed to process QoS 0 message: messageId={}", message.getMessageId(), e);
            return QoSProcessResult.failed("Processing error: " + e.getMessage());
        }
    }
    
    /**
     * 处理QoS 1消息（At Least Once）
     * 需要PUBACK确认
     *
     * @param clientId 客户端ID
     * @param packetId 包ID
     * @param message 消息
     * @return 处理结果
     */
    public QoSProcessResult processQoS1Message(String clientId, int packetId, QueuedMessage message) {
        if (message == null || message.getQos() != MqttQos.AT_LEAST_ONCE) {
            return QoSProcessResult.failed("Invalid QoS 1 message");
        }
        
        try {
            String qosKey = StorageKeyspace.qosStateKey(clientId, packetId);
            
            // 创建QoS状态
            QoSState qosState = new QoSState();
            qosState.setClientId(clientId);
            qosState.setPacketId(packetId);
            qosState.setMessageId(message.getMessageId());
            qosState.setQos(MqttQos.AT_LEAST_ONCE);
            qosState.setState(QoSState.State.WAITING_PUBACK);
            qosState.setCreatedAt(LocalDateTime.now());
            qosState.setTimeoutAt(LocalDateTime.now().plusSeconds(30)); // 30秒超时
            
            // 存储QoS状态
            storageOperations.put(RocksDBStorageEngine.QOS_STATE_CF, qosKey, qosState);
            qosStateCache.put(qosKey, qosState);
            
            // 添加超时索引
            addTimeoutIndex(qosState);
            
            // 标记消息为等待确认
            message.markAsSent();
            
            log.debug("QoS 1 message processed: messageId={}, clientId={}, packetId={}", 
                     message.getMessageId(), clientId, packetId);
            
            return QoSProcessResult.success(message, qosState);
            
        } catch (Exception e) {
            log.error("Failed to process QoS 1 message: messageId={}, clientId={}, packetId={}", 
                     message.getMessageId(), clientId, packetId, e);
            return QoSProcessResult.failed("Processing error: " + e.getMessage());
        }
    }
    
    /**
     * 处理QoS 2消息（Exactly Once）
     * 需要PUBREC/PUBREL/PUBCOMP四次握手
     *
     * @param clientId 客户端ID
     * @param packetId 包ID
     * @param message 消息
     * @return 处理结果
     */
    public QoSProcessResult processQoS2Message(String clientId, int packetId, QueuedMessage message) {
        if (message == null || message.getQos() != MqttQos.EXACTLY_ONCE) {
            return QoSProcessResult.failed("Invalid QoS 2 message");
        }
        
        try {
            String qosKey = StorageKeyspace.qosStateKey(clientId, packetId);
            
            // 创建QoS状态
            QoSState qosState = new QoSState();
            qosState.setClientId(clientId);
            qosState.setPacketId(packetId);
            qosState.setMessageId(message.getMessageId());
            qosState.setQos(MqttQos.EXACTLY_ONCE);
            qosState.setState(QoSState.State.WAITING_PUBREC);
            qosState.setCreatedAt(LocalDateTime.now());
            qosState.setTimeoutAt(LocalDateTime.now().plusSeconds(60)); // 1分钟超时
            
            // 存储QoS状态
            storageOperations.put(RocksDBStorageEngine.QOS_STATE_CF, qosKey, qosState);
            qosStateCache.put(qosKey, qosState);
            
            // 添加超时索引
            addTimeoutIndex(qosState);
            
            // 标记消息为等待接收确认
            message.markAsSent();
            
            log.debug("QoS 2 message processed: messageId={}, clientId={}, packetId={}", 
                     message.getMessageId(), clientId, packetId);
            
            return QoSProcessResult.success(message, qosState);
            
        } catch (Exception e) {
            log.error("Failed to process QoS 2 message: messageId={}, clientId={}, packetId={}", 
                     message.getMessageId(), clientId, packetId, e);
            return QoSProcessResult.failed("Processing error: " + e.getMessage());
        }
    }
    
    /**
     * 处理PUBACK响应（QoS 1）
     *
     * @param clientId 客户端ID
     * @param packetId 包ID
     * @return 处理结果
     */
    public QoSAckResult handlePubAck(String clientId, int packetId) {
        try {
            String qosKey = StorageKeyspace.qosStateKey(clientId, packetId);
            
            // 获取QoS状态
            QoSState qosState = getQoSState(qosKey);
            if (qosState == null) {
                log.debug("QoS state not found for PUBACK: clientId={}, packetId={}", clientId, packetId);
                return QoSAckResult.notFound();
            }
            
            // 检查状态
            if (qosState.getState() != QoSState.State.WAITING_PUBACK) {
                log.warn("Invalid state for PUBACK: expected=WAITING_PUBACK, actual={}, clientId={}, packetId={}", 
                        qosState.getState(), clientId, packetId);
                return QoSAckResult.invalidState(qosState.getState());
            }
            
            // 更新状态为完成
            qosState.setState(QoSState.State.COMPLETED);
            qosState.setCompletedAt(LocalDateTime.now());
            
            // 确认消息
            messagePersistenceService.acknowledgeMessage(clientId, packetId);
            
            // 清理QoS状态
            cleanupQoSState(qosKey, qosState);
            
            log.debug("PUBACK handled successfully: clientId={}, packetId={}, messageId={}", 
                     clientId, packetId, qosState.getMessageId());
            
            return QoSAckResult.success(qosState);
            
        } catch (Exception e) {
            log.error("Failed to handle PUBACK: clientId={}, packetId={}", clientId, packetId, e);
            return QoSAckResult.failed("Error handling PUBACK: " + e.getMessage());
        }
    }
    
    /**
     * 处理PUBREC响应（QoS 2第一步）
     *
     * @param clientId 客户端ID
     * @param packetId 包ID
     * @return 处理结果
     */
    public QoSAckResult handlePubRec(String clientId, int packetId) {
        try {
            String qosKey = StorageKeyspace.qosStateKey(clientId, packetId);
            
            // 获取QoS状态
            QoSState qosState = getQoSState(qosKey);
            if (qosState == null) {
                log.debug("QoS state not found for PUBREC: clientId={}, packetId={}", clientId, packetId);
                return QoSAckResult.notFound();
            }
            
            // 检查状态
            if (qosState.getState() != QoSState.State.WAITING_PUBREC) {
                log.warn("Invalid state for PUBREC: expected=WAITING_PUBREC, actual={}, clientId={}, packetId={}", 
                        qosState.getState(), clientId, packetId);
                return QoSAckResult.invalidState(qosState.getState());
            }
            
            // 更新状态为等待PUBCOMP
            qosState.setState(QoSState.State.WAITING_PUBCOMP);
            qosState.setUpdatedAt(LocalDateTime.now());
            qosState.setTimeoutAt(LocalDateTime.now().plusSeconds(60)); // 重新设置超时
            
            // 更新存储
            storageOperations.put(RocksDBStorageEngine.QOS_STATE_CF, qosKey, qosState);
            qosStateCache.put(qosKey, qosState);
            
            // 更新消息状态
            messagePersistenceService.handlePubRec(clientId, packetId);
            
            // 更新超时索引
            updateTimeoutIndex(qosState);
            
            log.debug("PUBREC handled successfully: clientId={}, packetId={}, messageId={}", 
                     clientId, packetId, qosState.getMessageId());
            
            return QoSAckResult.success(qosState);
            
        } catch (Exception e) {
            log.error("Failed to handle PUBREC: clientId={}, packetId={}", clientId, packetId, e);
            return QoSAckResult.failed("Error handling PUBREC: " + e.getMessage());
        }
    }
    
    /**
     * 处理PUBCOMP响应（QoS 2第二步）
     *
     * @param clientId 客户端ID
     * @param packetId 包ID
     * @return 处理结果
     */
    public QoSAckResult handlePubComp(String clientId, int packetId) {
        try {
            String qosKey = StorageKeyspace.qosStateKey(clientId, packetId);
            
            // 获取QoS状态
            QoSState qosState = getQoSState(qosKey);
            if (qosState == null) {
                log.debug("QoS state not found for PUBCOMP: clientId={}, packetId={}", clientId, packetId);
                return QoSAckResult.notFound();
            }
            
            // 检查状态
            if (qosState.getState() != QoSState.State.WAITING_PUBCOMP) {
                log.warn("Invalid state for PUBCOMP: expected=WAITING_PUBCOMP, actual={}, clientId={}, packetId={}", 
                        qosState.getState(), clientId, packetId);
                return QoSAckResult.invalidState(qosState.getState());
            }
            
            // 更新状态为完成
            qosState.setState(QoSState.State.COMPLETED);
            qosState.setCompletedAt(LocalDateTime.now());
            
            // 确认消息（QoS 2完成）
            messagePersistenceService.handlePubComp(clientId, packetId);
            
            // 清理QoS状态
            cleanupQoSState(qosKey, qosState);
            
            log.debug("PUBCOMP handled successfully: clientId={}, packetId={}, messageId={}", 
                     clientId, packetId, qosState.getMessageId());
            
            return QoSAckResult.success(qosState);
            
        } catch (Exception e) {
            log.error("Failed to handle PUBCOMP: clientId={}, packetId={}", clientId, packetId, e);
            return QoSAckResult.failed("Error handling PUBCOMP: " + e.getMessage());
        }
    }
    
    /**
     * 获取客户端的QoS状态列表
     *
     * @param clientId 客户端ID
     * @param limit 限制数量
     * @return QoS状态列表
     */
    public List<QoSState> getClientQoSStates(String clientId, int limit) {
        if (clientId == null || clientId.trim().isEmpty()) {
            return List.of();
        }
        
        try {
            String qosPrefix = StorageKeyspace.QOS_STATE_PREFIX + StorageKeyspace.KEY_SEPARATOR + clientId;
            List<StorageOperations.KeyValuePair<QoSState>> qosStates = 
                storageOperations.scanByPrefix(RocksDBStorageEngine.QOS_STATE_CF, 
                                             qosPrefix, limit, QoSState.class);
            
            List<QoSState> result = qosStates.stream()
                    .map(StorageOperations.KeyValuePair::getValue)
                    .filter(Objects::nonNull)
                    .sorted((s1, s2) -> s1.getCreatedAt().compareTo(s2.getCreatedAt()))
                    .collect(Collectors.toList());
            
            log.debug("Found {} QoS states for client: {}", result.size(), clientId);
            return result;
            
        } catch (Exception e) {
            log.error("Failed to get QoS states for client: {}", clientId, e);
            return List.of();
        }
    }
    
    /**
     * 重试超时的QoS消息
     *
     * @param batchSize 批处理大小
     * @return 重试的消息数量
     */
    @Scheduled(fixedDelay = 30000) // 每30秒执行一次
    public int retryTimeoutQoSMessages(int batchSize) {
        try {
            List<QoSState> timeoutStates = getTimeoutQoSStates(batchSize);
            
            int retryCount = 0;
            for (QoSState qosState : timeoutStates) {
                try {
                    if (retryQoSMessage(qosState)) {
                        retryCount++;
                    }
                } catch (Exception e) {
                    log.error("Failed to retry QoS message: clientId={}, packetId={}", 
                             qosState.getClientId(), qosState.getPacketId(), e);
                }
            }
            
            if (retryCount > 0) {
                log.info("Retried {} timeout QoS messages", retryCount);
            }
            
            return retryCount;
            
        } catch (Exception e) {
            log.error("Failed to retry timeout QoS messages", e);
            return 0;
        }
    }
    
    /**
     * 清理过期的QoS状态
     *
     * @param batchSize 批处理大小
     * @return 清理的数量
     */
    @Scheduled(fixedDelay = 300000) // 每5分钟执行一次
    public int cleanupExpiredQoSStates(int batchSize) {
        try {
            List<QoSState> expiredStates = getExpiredQoSStates(batchSize);
            
            int cleanedCount = 0;
            for (QoSState qosState : expiredStates) {
                try {
                    String qosKey = StorageKeyspace.qosStateKey(qosState.getClientId(), qosState.getPacketId());
                    cleanupQoSState(qosKey, qosState);
                    cleanedCount++;
                } catch (Exception e) {
                    log.error("Failed to cleanup expired QoS state: clientId={}, packetId={}", 
                             qosState.getClientId(), qosState.getPacketId(), e);
                }
            }
            
            if (cleanedCount > 0) {
                log.info("Cleaned up {} expired QoS states", cleanedCount);
            }
            
            return cleanedCount;
            
        } catch (Exception e) {
            log.error("Failed to cleanup expired QoS states", e);
            return 0;
        }
    }
    
    /**
     * 获取QoS统计信息
     *
     * @return QoS统计
     */
    public QoSStatistics getQoSStatistics() {
        try {
            QoSStatistics stats = new QoSStatistics();
            
            // 从缓存获取部分统计信息
            stats.cachedStates = qosStateCache.size();
            
            long waitingPuback = qosStateCache.values().stream()
                    .mapToLong(state -> state.getState() == QoSState.State.WAITING_PUBACK ? 1 : 0)
                    .sum();
            long waitingPubrec = qosStateCache.values().stream()
                    .mapToLong(state -> state.getState() == QoSState.State.WAITING_PUBREC ? 1 : 0)
                    .sum();
            long waitingPubcomp = qosStateCache.values().stream()
                    .mapToLong(state -> state.getState() == QoSState.State.WAITING_PUBCOMP ? 1 : 0)
                    .sum();
            
            stats.waitingPubackCount = waitingPuback;
            stats.waitingPubrecCount = waitingPubrec;
            stats.waitingPubcompCount = waitingPubcomp;
            
            // 从存储获取总数
            stats.totalQoSStates = storageOperations.getApproximateKeyCount(RocksDBStorageEngine.QOS_STATE_CF);
            
            return stats;
            
        } catch (Exception e) {
            log.error("Failed to get QoS statistics", e);
            return new QoSStatistics();
        }
    }
    
    /**
     * 异步处理QoS消息
     */
    @Async
    public CompletableFuture<QoSProcessResult> processQoSMessageAsync(MqttQos qos, String clientId, 
                                                                     int packetId, QueuedMessage message) {
        QoSProcessResult result;
        switch (qos) {
            case AT_MOST_ONCE:
                result = processQoS0Message(message);
                break;
            case AT_LEAST_ONCE:
                result = processQoS1Message(clientId, packetId, message);
                break;
            case EXACTLY_ONCE:
                result = processQoS2Message(clientId, packetId, message);
                break;
            default:
                result = QoSProcessResult.failed("Unsupported QoS: " + qos);
        }
        return CompletableFuture.completedFuture(result);
    }
    
    // 私有方法实现
    
    private QoSState getQoSState(String qosKey) throws Exception {
        // 首先从缓存获取
        QoSState cachedState = qosStateCache.get(qosKey);
        if (cachedState != null) {
            return cachedState;
        }
        
        // 从存储获取
        Optional<QoSState> stateOpt = storageOperations.get(
            RocksDBStorageEngine.QOS_STATE_CF, qosKey, QoSState.class);
        
        if (stateOpt.isPresent()) {
            QoSState state = stateOpt.get();
            qosStateCache.put(qosKey, state);
            return state;
        }
        
        return null;
    }
    
    private void addTimeoutIndex(QoSState qosState) throws Exception {
        if (qosState.getTimeoutAt() != null) {
            long timeoutTimestamp = java.time.ZoneId.systemDefault().getRules()
                    .getOffset(qosState.getTimeoutAt()).getTotalSeconds() * 1000;
            String timeoutKey = StorageKeyspace.qosTimeoutKey(
                timeoutTimestamp, qosState.getClientId(), qosState.getPacketId());
            storageOperations.put(RocksDBStorageEngine.QOS_STATE_CF, timeoutKey, qosState);
        }
    }
    
    private void updateTimeoutIndex(QoSState qosState) throws Exception {
        // 删除旧的超时索引，添加新的
        // 这里简化处理，实际实现可能需要更精确的索引管理
        addTimeoutIndex(qosState);
    }
    
    private void cleanupQoSState(String qosKey, QoSState qosState) throws Exception {
        // 删除主记录
        storageOperations.delete(RocksDBStorageEngine.QOS_STATE_CF, qosKey);
        
        // 从缓存移除
        qosStateCache.remove(qosKey);
        
        // 删除超时索引
        if (qosState.getTimeoutAt() != null) {
            long timeoutTimestamp = java.time.ZoneId.systemDefault().getRules()
                    .getOffset(qosState.getTimeoutAt()).getTotalSeconds() * 1000;
            String timeoutKey = StorageKeyspace.qosTimeoutKey(
                timeoutTimestamp, qosState.getClientId(), qosState.getPacketId());
            storageOperations.delete(RocksDBStorageEngine.QOS_STATE_CF, timeoutKey);
        }
    }
    
    private List<QoSState> getTimeoutQoSStates(int limit) throws Exception {
        long currentTimestamp = System.currentTimeMillis();
        String startKey = StorageKeyspace.QOS_TIMEOUT_PREFIX + StorageKeyspace.KEY_SEPARATOR;
        String endKey = StorageKeyspace.qosTimeoutKey(currentTimestamp, "", 0);
        
        List<StorageOperations.KeyValuePair<QoSState>> timeoutStates = 
            storageOperations.scan(RocksDBStorageEngine.QOS_STATE_CF, 
                                 startKey, endKey, limit, QoSState.class);
        
        return timeoutStates.stream()
                .map(StorageOperations.KeyValuePair::getValue)
                .filter(Objects::nonNull)
                .filter(state -> state.getTimeoutAt() != null && 
                               LocalDateTime.now().isAfter(state.getTimeoutAt()))
                .collect(Collectors.toList());
    }
    
    private List<QoSState> getExpiredQoSStates(int limit) throws Exception {
        // 获取创建时间超过1小时的QoS状态
        LocalDateTime expiredBefore = LocalDateTime.now().minusHours(1);
        
        String qosPrefix = StorageKeyspace.QOS_STATE_PREFIX + StorageKeyspace.KEY_SEPARATOR;
        List<StorageOperations.KeyValuePair<QoSState>> allStates = 
            storageOperations.scanByPrefix(RocksDBStorageEngine.QOS_STATE_CF, 
                                         qosPrefix, limit * 2, QoSState.class);
        
        return allStates.stream()
                .map(StorageOperations.KeyValuePair::getValue)
                .filter(Objects::nonNull)
                .filter(state -> state.getCreatedAt().isBefore(expiredBefore))
                .limit(limit)
                .collect(Collectors.toList());
    }
    
    private boolean retryQoSMessage(QoSState qosState) throws Exception {
        // 检查重试次数
        if (qosState.getRetryCount() >= DEFAULT_MAX_RETRIES) {
            log.warn("QoS message exceeded max retries: clientId={}, packetId={}, retries={}", 
                    qosState.getClientId(), qosState.getPacketId(), qosState.getRetryCount());
            
            // 标记为失败并清理
            qosState.setState(QoSState.State.FAILED);
            String qosKey = StorageKeyspace.qosStateKey(qosState.getClientId(), qosState.getPacketId());
            cleanupQoSState(qosKey, qosState);
            return false;
        }
        
        // 增加重试次数
        qosState.setRetryCount(qosState.getRetryCount() + 1);
        qosState.setUpdatedAt(LocalDateTime.now());
        qosState.setTimeoutAt(LocalDateTime.now().plusSeconds(DEFAULT_RETRY_INTERVAL_MS * qosState.getRetryCount() / 1000));
        
        // 更新存储
        String qosKey = StorageKeyspace.qosStateKey(qosState.getClientId(), qosState.getPacketId());
        storageOperations.put(RocksDBStorageEngine.QOS_STATE_CF, qosKey, qosState);
        qosStateCache.put(qosKey, qosState);
        
        // 更新超时索引
        updateTimeoutIndex(qosState);
        
        log.debug("QoS message retry scheduled: clientId={}, packetId={}, retry={}", 
                 qosState.getClientId(), qosState.getPacketId(), qosState.getRetryCount());
        
        return true;
    }
    
    // 内部类定义
    
    /**
     * QoS状态
     */
    public static class QoSState {
        private String clientId;
        private int packetId;
        private String messageId;
        private MqttQos qos;
        private State state;
        private LocalDateTime createdAt;
        private LocalDateTime updatedAt;
        private LocalDateTime completedAt;
        private LocalDateTime timeoutAt;
        private int retryCount = 0;
        
        public enum State {
            WAITING_PUBACK,     // QoS 1: 等待PUBACK
            WAITING_PUBREC,     // QoS 2: 等待PUBREC
            WAITING_PUBCOMP,    // QoS 2: 等待PUBCOMP
            COMPLETED,          // 完成
            FAILED,             // 失败
            EXPIRED             // 过期
        }
        
        // Getters and Setters
        public String getClientId() { return clientId; }
        public void setClientId(String clientId) { this.clientId = clientId; }
        
        public int getPacketId() { return packetId; }
        public void setPacketId(int packetId) { this.packetId = packetId; }
        
        public String getMessageId() { return messageId; }
        public void setMessageId(String messageId) { this.messageId = messageId; }
        
        public MqttQos getQos() { return qos; }
        public void setQos(MqttQos qos) { this.qos = qos; }
        
        public State getState() { return state; }
        public void setState(State state) { this.state = state; }
        
        public LocalDateTime getCreatedAt() { return createdAt; }
        public void setCreatedAt(LocalDateTime createdAt) { this.createdAt = createdAt; }
        
        public LocalDateTime getUpdatedAt() { return updatedAt; }
        public void setUpdatedAt(LocalDateTime updatedAt) { this.updatedAt = updatedAt; }
        
        public LocalDateTime getCompletedAt() { return completedAt; }
        public void setCompletedAt(LocalDateTime completedAt) { this.completedAt = completedAt; }
        
        public LocalDateTime getTimeoutAt() { return timeoutAt; }
        public void setTimeoutAt(LocalDateTime timeoutAt) { this.timeoutAt = timeoutAt; }
        
        public int getRetryCount() { return retryCount; }
        public void setRetryCount(int retryCount) { this.retryCount = retryCount; }
        
        @Override
        public String toString() {
            return String.format("QoSState{clientId='%s', packetId=%d, qos=%s, state=%s, retries=%d}", 
                               clientId, packetId, qos, state, retryCount);
        }
    }
    
    /**
     * QoS处理结果
     */
    public static class QoSProcessResult {
        private boolean success;
        private String errorMessage;
        private QueuedMessage message;
        private QoSState qosState;
        
        public static QoSProcessResult success(QueuedMessage message, QoSState qosState) {
            QoSProcessResult result = new QoSProcessResult();
            result.success = true;
            result.message = message;
            result.qosState = qosState;
            return result;
        }
        
        public static QoSProcessResult failed(String errorMessage) {
            QoSProcessResult result = new QoSProcessResult();
            result.success = false;
            result.errorMessage = errorMessage;
            return result;
        }
        
        public boolean isSuccess() { return success; }
        public String getErrorMessage() { return errorMessage; }
        public QueuedMessage getMessage() { return message; }
        public QoSState getQosState() { return qosState; }
    }
    
    /**
     * QoS确认结果
     */
    public static class QoSAckResult {
        private boolean success;
        private String errorMessage;
        private QoSState qosState;
        private boolean notFound;
        private QoSState.State invalidState;
        
        public static QoSAckResult success(QoSState qosState) {
            QoSAckResult result = new QoSAckResult();
            result.success = true;
            result.qosState = qosState;
            return result;
        }
        
        public static QoSAckResult failed(String errorMessage) {
            QoSAckResult result = new QoSAckResult();
            result.success = false;
            result.errorMessage = errorMessage;
            return result;
        }
        
        public static QoSAckResult notFound() {
            QoSAckResult result = new QoSAckResult();
            result.success = false;
            result.notFound = true;
            result.errorMessage = "QoS state not found";
            return result;
        }
        
        public static QoSAckResult invalidState(QoSState.State state) {
            QoSAckResult result = new QoSAckResult();
            result.success = false;
            result.invalidState = state;
            result.errorMessage = "Invalid state: " + state;
            return result;
        }
        
        public boolean isSuccess() { return success; }
        public String getErrorMessage() { return errorMessage; }
        public QoSState getQosState() { return qosState; }
        public boolean isNotFound() { return notFound; }
        public QoSState.State getInvalidState() { return invalidState; }
    }
    
    /**
     * QoS统计信息
     */
    public static class QoSStatistics {
        public long totalQoSStates = 0;
        public long cachedStates = 0;
        public long waitingPubackCount = 0;
        public long waitingPubrecCount = 0;
        public long waitingPubcompCount = 0;
        public long timeoutRetries = 0;
        public long expiredStates = 0;
        public LocalDateTime lastUpdated = LocalDateTime.now();
        
        @Override
        public String toString() {
            return String.format("QoSStats{total=%d, cached=%d, puback=%d, pubrec=%d, pubcomp=%d, retries=%d, expired=%d}", 
                               totalQoSStates, cachedStates, waitingPubackCount, waitingPubrecCount, 
                               waitingPubcompCount, timeoutRetries, expiredStates);
        }
    }
}