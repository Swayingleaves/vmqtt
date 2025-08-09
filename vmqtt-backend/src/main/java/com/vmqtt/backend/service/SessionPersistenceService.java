/**
 * 会话持久化服务实现
 *
 * @author zhenglin
 * @date 2025/08/09
 */
package com.vmqtt.backend.service;

import com.vmqtt.backend.storage.StorageKeyspace;
import com.vmqtt.backend.storage.StorageOperations;
import com.vmqtt.backend.storage.engine.RocksDBStorageEngine;
import com.vmqtt.common.model.ClientSession;
import com.vmqtt.common.model.QueuedMessage;
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
 * 会话持久化服务
 * 负责客户端会话状态的持久化存储和恢复
 */
@Slf4j
@Service
public class SessionPersistenceService {
    
    @Autowired
    private StorageOperations storageOperations;
    
    /**
     * 保存会话状态
     *
     * @param session 客户端会话
     * @return 是否成功
     */
    public boolean saveSession(ClientSession session) {
        if (session == null || session.getClientId() == null) {
            log.warn("Cannot save null session or session without client ID");
            return false;
        }
        
        try {
            String sessionKey = StorageKeyspace.sessionKey(session.getClientId());
            
            // 更新最后活动时间
            session.setLastActivity(LocalDateTime.now());
            
            // 保存到会话列族
            storageOperations.put(RocksDBStorageEngine.SESSION_CF, sessionKey, session);
            
            // 更新会话状态索引
            updateSessionStateIndex(session);
            
            // 如果会话有过期时间，更新过期索引
            updateSessionExpiryIndex(session);
            
            log.debug("Session saved successfully: clientId={}, sessionId={}", 
                     session.getClientId(), session.getSessionId());
            return true;
            
        } catch (Exception e) {
            log.error("Failed to save session: clientId={}", session.getClientId(), e);
            return false;
        }
    }
    
    /**
     * 加载会话状态
     *
     * @param clientId 客户端ID
     * @return 客户端会话
     */
    public Optional<ClientSession> loadSession(String clientId) {
        if (clientId == null || clientId.trim().isEmpty()) {
            log.warn("Cannot load session with null or empty client ID");
            return Optional.empty();
        }
        
        try {
            String sessionKey = StorageKeyspace.sessionKey(clientId);
            Optional<ClientSession> session = storageOperations.get(
                RocksDBStorageEngine.SESSION_CF, sessionKey, ClientSession.class);
            
            if (session.isPresent()) {
                log.debug("Session loaded successfully: clientId={}, sessionId={}", 
                         clientId, session.get().getSessionId());
                
                // 加载排队消息
                loadQueuedMessages(session.get());
                
                // 加载传输中消息
                loadInflightMessages(session.get());
            } else {
                log.debug("No session found for client: {}", clientId);
            }
            
            return session;
            
        } catch (Exception e) {
            log.error("Failed to load session: clientId={}", clientId, e);
            return Optional.empty();
        }
    }
    
    /**
     * 删除会话
     *
     * @param clientId 客户端ID
     * @return 是否成功
     */
    public boolean deleteSession(String clientId) {
        if (clientId == null || clientId.trim().isEmpty()) {
            log.warn("Cannot delete session with null or empty client ID");
            return false;
        }
        
        try {
            // 首先加载会话以获取完整信息
            Optional<ClientSession> sessionOpt = loadSession(clientId);
            if (!sessionOpt.isPresent()) {
                log.debug("Session not found for deletion: clientId={}", clientId);
                return true; // 会话不存在，认为删除成功
            }
            
            ClientSession session = sessionOpt.get();
            
            // 使用批量操作删除所有相关数据
            StorageOperations.BatchOperationBuilder batch = storageOperations.batchOperation();
            
            // 删除主会话记录
            String sessionKey = StorageKeyspace.sessionKey(clientId);
            batch.delete(RocksDBStorageEngine.SESSION_CF, sessionKey);
            
            // 删除会话状态索引
            removeSessionStateIndex(session, batch);
            
            // 删除会话过期索引
            removeSessionExpiryIndex(session, batch);
            
            // 删除排队消息
            deleteQueuedMessages(session, batch);
            
            // 删除传输中消息
            deleteInflightMessages(session, batch);
            
            // 执行批量删除
            batch.execute();
            
            log.info("Session deleted successfully: clientId={}, sessionId={}", 
                    clientId, session.getSessionId());
            return true;
            
        } catch (Exception e) {
            log.error("Failed to delete session: clientId={}", clientId, e);
            return false;
        }
    }
    
    /**
     * 检查会话是否存在
     *
     * @param clientId 客户端ID
     * @return 是否存在
     */
    public boolean sessionExists(String clientId) {
        if (clientId == null || clientId.trim().isEmpty()) {
            return false;
        }
        
        try {
            String sessionKey = StorageKeyspace.sessionKey(clientId);
            return storageOperations.exists(RocksDBStorageEngine.SESSION_CF, sessionKey);
        } catch (Exception e) {
            log.error("Failed to check session existence: clientId={}", clientId, e);
            return false;
        }
    }
    
    /**
     * 获取所有活跃会话
     *
     * @param limit 限制数量
     * @return 活跃会话列表
     */
    public List<ClientSession> getActiveSessions(int limit) {
        try {
            String stateIndexPrefix = StorageKeyspace.sessionIndexKey("ACTIVE", "");
            List<StorageOperations.KeyValuePair<ClientSession>> sessions = 
                storageOperations.scanByPrefix(RocksDBStorageEngine.SESSION_CF, 
                                             stateIndexPrefix, limit, ClientSession.class);
            
            List<ClientSession> result = sessions.stream()
                    .map(StorageOperations.KeyValuePair::getValue)
                    .filter(session -> session.getState() == ClientSession.SessionState.ACTIVE)
                    .collect(Collectors.toList());
            
            log.debug("Found {} active sessions", result.size());
            return result;
            
        } catch (Exception e) {
            log.error("Failed to get active sessions", e);
            return List.of();
        }
    }
    
    /**
     * 获取过期会话
     *
     * @param currentTime 当前时间
     * @param limit 限制数量
     * @return 过期会话列表
     */
    public List<ClientSession> getExpiredSessions(LocalDateTime currentTime, int limit) {
        try {
            // 扫描过期索引
            long currentTimestamp = System.currentTimeMillis();
            String startKey = StorageKeyspace.SESSION_EXPIRY_PREFIX + StorageKeyspace.KEY_SEPARATOR;
            String endKey = StorageKeyspace.sessionExpiryKey(currentTimestamp, "");
            
            List<StorageOperations.KeyValuePair<ClientSession>> expiredSessions = 
                storageOperations.scan(RocksDBStorageEngine.SESSION_CF, 
                                     startKey, endKey, limit, ClientSession.class);
            
            List<ClientSession> result = expiredSessions.stream()
                    .map(StorageOperations.KeyValuePair::getValue)
                    .filter(session -> isSessionExpired(session, currentTime))
                    .collect(Collectors.toList());
            
            log.debug("Found {} expired sessions", result.size());
            return result;
            
        } catch (Exception e) {
            log.error("Failed to get expired sessions", e);
            return List.of();
        }
    }
    
    /**
     * 批量保存会话
     *
     * @param sessions 会话列表
     * @return 成功保存的数量
     */
    public int batchSaveSessions(List<ClientSession> sessions) {
        if (sessions == null || sessions.isEmpty()) {
            return 0;
        }
        
        try {
            StorageOperations.BatchOperationBuilder batch = storageOperations.batchOperation();
            
            for (ClientSession session : sessions) {
                if (session.getClientId() != null) {
                    String sessionKey = StorageKeyspace.sessionKey(session.getClientId());
                    session.setLastActivity(LocalDateTime.now());
                    batch.put(RocksDBStorageEngine.SESSION_CF, sessionKey, session);
                }
            }
            
            batch.execute();
            
            log.info("Batch saved {} sessions", sessions.size());
            return sessions.size();
            
        } catch (Exception e) {
            log.error("Failed to batch save sessions", e);
            return 0;
        }
    }
    
    /**
     * 异步保存会话
     *
     * @param session 客户端会话
     * @return CompletableFuture<Boolean>
     */
    @Async
    public CompletableFuture<Boolean> saveSessionAsync(ClientSession session) {
        return CompletableFuture.completedFuture(saveSession(session));
    }
    
    /**
     * 异步加载会话
     *
     * @param clientId 客户端ID
     * @return CompletableFuture<Optional<ClientSession>>
     */
    @Async
    public CompletableFuture<Optional<ClientSession>> loadSessionAsync(String clientId) {
        return CompletableFuture.completedFuture(loadSession(clientId));
    }
    
    /**
     * 清理过期会话
     *
     * @param batchSize 批处理大小
     * @return 清理的会话数量
     */
    public int cleanupExpiredSessions(int batchSize) {
        LocalDateTime currentTime = LocalDateTime.now();
        List<ClientSession> expiredSessions = getExpiredSessions(currentTime, batchSize);
        
        int cleanedCount = 0;
        for (ClientSession session : expiredSessions) {
            if (deleteSession(session.getClientId())) {
                cleanedCount++;
            }
        }
        
        if (cleanedCount > 0) {
            log.info("Cleaned up {} expired sessions", cleanedCount);
        }
        
        return cleanedCount;
    }
    
    /**
     * 获取会话统计信息
     *
     * @return 会话统计
     */
    public SessionStatistics getSessionStatistics() {
        try {
            SessionStatistics stats = new SessionStatistics();
            
            // 获取总会话数
            stats.totalSessions = storageOperations.getApproximateKeyCount(RocksDBStorageEngine.SESSION_CF);
            
            // 计算活跃会话数（这里简化实现，实际可能需要更精确的统计）
            List<ClientSession> activeSessions = getActiveSessions(1000);
            stats.activeSessions = activeSessions.size();
            
            // 计算过期会话数
            List<ClientSession> expiredSessions = getExpiredSessions(LocalDateTime.now(), 1000);
            stats.expiredSessions = expiredSessions.size();
            
            // 计算其他统计信息
            stats.inactiveSessions = Math.max(0, stats.totalSessions - stats.activeSessions - stats.expiredSessions);
            
            return stats;
            
        } catch (Exception e) {
            log.error("Failed to get session statistics", e);
            return new SessionStatistics();
        }
    }
    
    /**
     * 更新会话状态索引
     */
    private void updateSessionStateIndex(ClientSession session) throws StorageOperations.StorageException {
        String stateIndexKey = StorageKeyspace.sessionIndexKey(
            session.getState().name(), session.getClientId());
        storageOperations.put(RocksDBStorageEngine.SESSION_CF, stateIndexKey, session);
    }
    
    /**
     * 更新会话过期索引
     */
    private void updateSessionExpiryIndex(ClientSession session) throws StorageOperations.StorageException {
        // 这里简化处理，实际应根据keepalive计算过期时间
        LocalDateTime expiryTime = session.getLastActivity().plusHours(2); // 2小时过期
        long expiryTimestamp = java.time.ZoneId.systemDefault().getRules().getOffset(expiryTime).getTotalSeconds() * 1000;
        
        String expiryIndexKey = StorageKeyspace.sessionExpiryKey(expiryTimestamp, session.getClientId());
        storageOperations.put(RocksDBStorageEngine.SESSION_CF, expiryIndexKey, session);
    }
    
    /**
     * 加载排队消息
     */
    private void loadQueuedMessages(ClientSession session) {
        try {
            String queuePrefix = StorageKeyspace.MESSAGE_QUEUE_PREFIX + 
                               StorageKeyspace.KEY_SEPARATOR + session.getClientId();
            
            List<StorageOperations.KeyValuePair<QueuedMessage>> queuedMessages = 
                storageOperations.scanByPrefix(RocksDBStorageEngine.MESSAGE_CF, 
                                             queuePrefix, 1000, QueuedMessage.class);
            
            for (StorageOperations.KeyValuePair<QueuedMessage> kvp : queuedMessages) {
                session.addQueuedMessage(kvp.getValue());
            }
            
            log.debug("Loaded {} queued messages for session: {}", 
                     queuedMessages.size(), session.getClientId());
                     
        } catch (Exception e) {
            log.error("Failed to load queued messages for session: {}", session.getClientId(), e);
        }
    }
    
    /**
     * 加载传输中消息
     */
    private void loadInflightMessages(ClientSession session) {
        try {
            String inflightPrefix = StorageKeyspace.MESSAGE_INFLIGHT_PREFIX + 
                                  StorageKeyspace.KEY_SEPARATOR + session.getClientId();
            
            List<StorageOperations.KeyValuePair<QueuedMessage>> inflightMessages = 
                storageOperations.scanByPrefix(RocksDBStorageEngine.MESSAGE_CF, 
                                             inflightPrefix, 1000, QueuedMessage.class);
            
            for (StorageOperations.KeyValuePair<QueuedMessage> kvp : inflightMessages) {
                QueuedMessage message = kvp.getValue();
                if (message.getPacketId() != null) {
                    session.addInflightMessage(message.getPacketId(), message);
                }
            }
            
            log.debug("Loaded {} inflight messages for session: {}", 
                     inflightMessages.size(), session.getClientId());
                     
        } catch (Exception e) {
            log.error("Failed to load inflight messages for session: {}", session.getClientId(), e);
        }
    }
    
    /**
     * 删除排队消息
     */
    private void deleteQueuedMessages(ClientSession session, StorageOperations.BatchOperationBuilder batch) {
        String queuePrefix = StorageKeyspace.MESSAGE_QUEUE_PREFIX + 
                           StorageKeyspace.KEY_SEPARATOR + session.getClientId();
        
        try {
            List<StorageOperations.KeyValuePair<QueuedMessage>> queuedMessages = 
                storageOperations.scanByPrefix(RocksDBStorageEngine.MESSAGE_CF, 
                                             queuePrefix, 10000, QueuedMessage.class);
            
            for (StorageOperations.KeyValuePair<QueuedMessage> kvp : queuedMessages) {
                batch.delete(RocksDBStorageEngine.MESSAGE_CF, kvp.getKey());
            }
            
        } catch (Exception e) {
            log.error("Failed to delete queued messages for session: {}", session.getClientId(), e);
        }
    }
    
    /**
     * 删除传输中消息
     */
    private void deleteInflightMessages(ClientSession session, StorageOperations.BatchOperationBuilder batch) {
        String inflightPrefix = StorageKeyspace.MESSAGE_INFLIGHT_PREFIX + 
                              StorageKeyspace.KEY_SEPARATOR + session.getClientId();
        
        try {
            List<StorageOperations.KeyValuePair<QueuedMessage>> inflightMessages = 
                storageOperations.scanByPrefix(RocksDBStorageEngine.MESSAGE_CF, 
                                             inflightPrefix, 10000, QueuedMessage.class);
            
            for (StorageOperations.KeyValuePair<QueuedMessage> kvp : inflightMessages) {
                batch.delete(RocksDBStorageEngine.MESSAGE_CF, kvp.getKey());
            }
            
        } catch (Exception e) {
            log.error("Failed to delete inflight messages for session: {}", session.getClientId(), e);
        }
    }
    
    /**
     * 移除会话状态索引
     */
    private void removeSessionStateIndex(ClientSession session, StorageOperations.BatchOperationBuilder batch) {
        String stateIndexKey = StorageKeyspace.sessionIndexKey(
            session.getState().name(), session.getClientId());
        batch.delete(RocksDBStorageEngine.SESSION_CF, stateIndexKey);
    }
    
    /**
     * 移除会话过期索引
     */
    private void removeSessionExpiryIndex(ClientSession session, StorageOperations.BatchOperationBuilder batch) {
        // 这里需要找到对应的过期索引键并删除
        // 简化实现，实际应该维护更精确的过期索引
        LocalDateTime expiryTime = session.getLastActivity().plusHours(2);
        long expiryTimestamp = java.time.ZoneId.systemDefault().getRules().getOffset(expiryTime).getTotalSeconds() * 1000;
        
        String expiryIndexKey = StorageKeyspace.sessionExpiryKey(expiryTimestamp, session.getClientId());
        batch.delete(RocksDBStorageEngine.SESSION_CF, expiryIndexKey);
    }
    
    /**
     * 检查会话是否过期
     */
    private boolean isSessionExpired(ClientSession session, LocalDateTime currentTime) {
        if (session.getState() == ClientSession.SessionState.EXPIRED) {
            return true;
        }
        
        // 根据最后活动时间判断是否过期（这里简化为2小时）
        LocalDateTime expiryTime = session.getLastActivity().plusHours(2);
        return currentTime.isAfter(expiryTime);
    }
    
    /**
     * 会话统计信息
     */
    public static class SessionStatistics {
        public long totalSessions = 0;
        public long activeSessions = 0;
        public long inactiveSessions = 0;
        public long expiredSessions = 0;
        public LocalDateTime lastUpdated = LocalDateTime.now();
        
        @Override
        public String toString() {
            return String.format("SessionStats{total=%d, active=%d, inactive=%d, expired=%d}", 
                               totalSessions, activeSessions, inactiveSessions, expiredSessions);
        }
    }
}