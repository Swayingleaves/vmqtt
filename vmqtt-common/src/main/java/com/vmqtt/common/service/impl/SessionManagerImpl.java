/**
 * 会话管理器实现类
 *
 * @author zhenglin
 * @date 2025/08/06
 */
package com.vmqtt.common.service.impl;

import com.vmqtt.common.model.ClientSession;
import com.vmqtt.common.model.QueuedMessage;
import com.vmqtt.common.model.TopicSubscription;
import com.vmqtt.common.service.SessionManager;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;
import java.util.stream.Collectors;

/**
 * 会话管理器默认实现
 * 基于内存存储的高性能会话管理
 */
@Slf4j
@Service
public class SessionManagerImpl implements SessionManager {

    /**
     * 会话存储：clientId -> ClientSession
     */
    private final ConcurrentHashMap<String, ClientSession> sessions = new ConcurrentHashMap<>();
    
    /**
     * 会话ID映射：sessionId -> clientId
     */
    private final ConcurrentHashMap<String, String> sessionIdToClientId = new ConcurrentHashMap<>();
    
    /**
     * 主题订阅索引：topic -> Set<clientId>
     */
    private final ConcurrentHashMap<String, Set<String>> topicSubscriptions = new ConcurrentHashMap<>();
    
    /**
     * 会话事件监听器集合
     */
    private final ConcurrentLinkedQueue<SessionEventListener> listeners = new ConcurrentLinkedQueue<>();
    
    /**
     * 会话统计信息
     */
    private final SessionStatsImpl stats = new SessionStatsImpl();

    @Override
    public CompletableFuture<SessionResult> createOrGetSession(String clientId, boolean cleanSession, String connectionId) {
        return CompletableFuture.supplyAsync(() -> {
            try {
                log.info("创建或获取会话: clientId={}, cleanSession={}, connectionId={}", clientId, cleanSession, connectionId);
                
                ClientSession existingSession = sessions.get(clientId);
                boolean isNewSession = false;
                boolean isSessionPresent = existingSession != null;
                
                if (existingSession != null) {
                    if (cleanSession) {
                        log.info("清理现有会话: clientId={}", clientId);
                        // 清理现有会话
                        cleanSession(clientId).join();
                        existingSession = null;
                        isSessionPresent = false;
                    } else {
                        // 恢复现有会话
                        log.info("恢复现有会话: clientId={}, sessionId={}", clientId, existingSession.getSessionId());
                        existingSession.setConnectionId(connectionId);
                        existingSession.setState(ClientSession.SessionState.ACTIVE);
                        existingSession.updateLastActivity();
                        
                        // 触发会话状态变化事件
                        triggerSessionStateChanged(existingSession, ClientSession.SessionState.INACTIVE, ClientSession.SessionState.ACTIVE);
                    }
                }
                
                if (existingSession == null) {
                    // 创建新会话
                    String sessionId = generateSessionId();
                    existingSession = ClientSession.builder()
                            .sessionId(sessionId)
                            .clientId(clientId)
                            .connectionId(connectionId)
                            .cleanSession(cleanSession)
                            .state(ClientSession.SessionState.ACTIVE)
                            .createdAt(LocalDateTime.now())
                            .lastActivity(LocalDateTime.now())
                            .build();
                    
                    sessions.put(clientId, existingSession);
                    sessionIdToClientId.put(sessionId, clientId);
                    isNewSession = true;
                    
                    // 更新统计信息
                    stats.totalSessions.increment();
                    stats.activeSessions.increment();
                    
                    // 触发会话创建事件
                    triggerSessionCreated(existingSession);
                    
                    log.info("创建新会话成功: clientId={}, sessionId={}", clientId, sessionId);
                } else {
                    log.info("恢复会话成功: clientId={}, sessionId={}", clientId, existingSession.getSessionId());
                }
                
                return new SessionResultImpl(existingSession, isNewSession, isSessionPresent);
                
            } catch (Exception e) {
                log.error("创建或获取会话失败: clientId={}", clientId, e);
                throw new RuntimeException("创建或获取会话失败", e);
            }
        });
    }

    @Override
    public Optional<ClientSession> getSession(String clientId) {
        return Optional.ofNullable(sessions.get(clientId));
    }

    @Override
    public Optional<ClientSession> getSessionById(String sessionId) {
        String clientId = sessionIdToClientId.get(sessionId);
        if (clientId != null) {
            return Optional.ofNullable(sessions.get(clientId));
        }
        return Optional.empty();
    }

    @Override
    public CompletableFuture<Optional<ClientSession>> removeSession(String clientId) {
        return CompletableFuture.supplyAsync(() -> {
            try {
                log.info("移除会话: clientId={}", clientId);
                
                ClientSession session = sessions.remove(clientId);
                if (session == null) {
                    log.warn("会话不存在: clientId={}", clientId);
                    return Optional.empty();
                }
                
                // 清理映射关系
                sessionIdToClientId.remove(session.getSessionId());
                
                // 清理订阅索引
                cleanupSubscriptionIndex(clientId, session.getSubscriptions().values());
                
                // 更新会话状态
                session.setState(ClientSession.SessionState.CLEANED);
                
                // 更新统计信息
                stats.activeSessions.decrement();
                
                // 触发会话销毁事件
                triggerSessionDestroyed(session);
                
                log.info("会话移除成功: clientId={}, sessionId={}", clientId, session.getSessionId());
                return Optional.of(session);
                
            } catch (Exception e) {
                log.error("移除会话失败: clientId={}", clientId, e);
                throw new RuntimeException("移除会话失败", e);
            }
        });
    }

    @Override
    public CompletableFuture<Boolean> cleanSession(String clientId) {
        return CompletableFuture.supplyAsync(() -> {
            try {
                log.info("清理会话: clientId={}", clientId);
                
                ClientSession session = sessions.get(clientId);
                if (session == null) {
                    log.warn("会话不存在，无需清理: clientId={}", clientId);
                    return false;
                }
                
                // 清理订阅信息
                cleanupSubscriptionIndex(clientId, session.getSubscriptions().values());
                session.getSubscriptions().clear();
                
                // 清理排队消息
                session.getPendingMessages().clear();
                
                // 清理传输中消息
                session.getInflightMessages().clear();
                
                // 重置统计信息
                session.setStats(new ClientSession.SessionStats());
                
                // 更新最后活动时间
                session.updateLastActivity();
                
                log.info("会话清理成功: clientId={}, sessionId={}", clientId, session.getSessionId());
                return true;
                
            } catch (Exception e) {
                log.error("清理会话失败: clientId={}", clientId, e);
                return false;
            }
        });
    }

    @Override
    public CompletableFuture<Boolean> addSubscription(String clientId, TopicSubscription subscription) {
        return CompletableFuture.supplyAsync(() -> {
            try {
                log.debug("添加订阅: clientId={}, topicFilter={}, qos={}", 
                    clientId, subscription.getTopicFilter(), subscription.getQos());
                
                ClientSession session = sessions.get(clientId);
                if (session == null) {
                    log.warn("会话不存在，无法添加订阅: clientId={}", clientId);
                    return false;
                }
                
                // 添加到会话
                session.addSubscription(subscription);
                
                // 更新订阅索引
                topicSubscriptions.computeIfAbsent(subscription.getTopicFilter(), 
                    k -> ConcurrentHashMap.newKeySet()).add(clientId);
                
                // 更新统计信息
                stats.totalSubscriptions.increment();
                
                // 触发订阅添加事件
                triggerSubscriptionAdded(session, subscription);
                
                log.debug("订阅添加成功: clientId={}, topicFilter={}", clientId, subscription.getTopicFilter());
                return true;
                
            } catch (Exception e) {
                log.error("添加订阅失败: clientId={}, topicFilter={}", 
                    clientId, subscription.getTopicFilter(), e);
                return false;
            }
        });
    }

    @Override
    public CompletableFuture<Optional<TopicSubscription>> removeSubscription(String clientId, String topicFilter) {
        return CompletableFuture.supplyAsync(() -> {
            try {
                log.debug("移除订阅: clientId={}, topicFilter={}", clientId, topicFilter);
                
                ClientSession session = sessions.get(clientId);
                if (session == null) {
                    log.warn("会话不存在，无法移除订阅: clientId={}", clientId);
                    return Optional.empty();
                }
                
                // 从会话中移除
                TopicSubscription removed = session.removeSubscription(topicFilter);
                if (removed == null) {
                    log.warn("订阅不存在: clientId={}, topicFilter={}", clientId, topicFilter);
                    return Optional.empty();
                }
                
                // 更新订阅索引
                Set<String> subscribers = topicSubscriptions.get(topicFilter);
                if (subscribers != null) {
                    subscribers.remove(clientId);
                    if (subscribers.isEmpty()) {
                        topicSubscriptions.remove(topicFilter);
                    }
                }
                
                // 更新统计信息
                stats.totalSubscriptions.decrement();
                
                // 触发订阅移除事件
                triggerSubscriptionRemoved(session, removed);
                
                log.debug("订阅移除成功: clientId={}, topicFilter={}", clientId, topicFilter);
                return Optional.of(removed);
                
            } catch (Exception e) {
                log.error("移除订阅失败: clientId={}, topicFilter={}", clientId, topicFilter, e);
                return Optional.empty();
            }
        });
    }

    @Override
    public List<TopicSubscription> getSubscriptions(String clientId) {
        ClientSession session = sessions.get(clientId);
        if (session != null) {
            return new ArrayList<>(session.getSubscriptions().values());
        }
        return Collections.emptyList();
    }

    @Override
    public List<ClientSession> getSubscribedSessions(String topic) {
        Set<String> allSubscribers = new HashSet<>();
        
        // 匹配所有相关的主题过滤器
        for (String topicFilter : topicSubscriptions.keySet()) {
            if (matchesTopicFilter(topic, topicFilter)) {
                Set<String> subscribers = topicSubscriptions.get(topicFilter);
                if (subscribers != null) {
                    allSubscribers.addAll(subscribers);
                }
            }
        }
        
        // 获取对应的会话
        return allSubscribers.stream()
                .map(sessions::get)
                .filter(Objects::nonNull)
                .filter(session -> session.getState() == ClientSession.SessionState.ACTIVE)
                .collect(Collectors.toList());
    }

    @Override
    public CompletableFuture<Boolean> addQueuedMessage(String clientId, QueuedMessage message) {
        return CompletableFuture.supplyAsync(() -> {
            try {
                ClientSession session = sessions.get(clientId);
                if (session == null) {
                    log.warn("会话不存在，无法添加排队消息: clientId={}", clientId);
                    return false;
                }
                
                session.addQueuedMessage(message);
                
                // 更新统计信息
                stats.queuedMessageCount.increment();
                
                // 触发消息入队事件
                triggerMessageQueued(session, message);
                
                log.debug("排队消息添加成功: clientId={}, messageId={}", clientId, message.getMessageId());
                return true;
                
            } catch (Exception e) {
                log.error("添加排队消息失败: clientId={}, messageId={}", 
                    clientId, message.getMessageId(), e);
                return false;
            }
        });
    }

    @Override
    public List<QueuedMessage> getQueuedMessages(String clientId, int limit) {
        ClientSession session = sessions.get(clientId);
        if (session != null) {
            return session.getPendingMessages().stream()
                    .limit(limit)
                    .collect(Collectors.toList());
        }
        return Collections.emptyList();
    }

    @Override
    public CompletableFuture<Optional<QueuedMessage>> removeQueuedMessage(String clientId, String messageId) {
        return CompletableFuture.supplyAsync(() -> {
            try {
                ClientSession session = sessions.get(clientId);
                if (session == null) {
                    return Optional.empty();
                }
                
                QueuedMessage removed = session.removeQueuedMessage(messageId);
                if (removed != null) {
                    // 更新统计信息
                    stats.queuedMessageCount.decrement();
                    
                    // 触发消息出队事件
                    triggerMessageDequeued(session, removed);
                    
                    log.debug("排队消息移除成功: clientId={}, messageId={}", clientId, messageId);
                }
                
                return Optional.ofNullable(removed);
                
            } catch (Exception e) {
                log.error("移除排队消息失败: clientId={}, messageId={}", clientId, messageId, e);
                return Optional.empty();
            }
        });
    }

    @Override
    public CompletableFuture<Boolean> addInflightMessage(String clientId, int packetId, QueuedMessage message) {
        return CompletableFuture.supplyAsync(() -> {
            try {
                ClientSession session = sessions.get(clientId);
                if (session == null) {
                    return false;
                }
                
                session.addInflightMessage(packetId, message);
                
                // 更新统计信息
                stats.inflightMessageCount.increment();
                
                log.debug("传输中消息添加成功: clientId={}, packetId={}, messageId={}", 
                    clientId, packetId, message.getMessageId());
                return true;
                
            } catch (Exception e) {
                log.error("添加传输中消息失败: clientId={}, packetId={}", clientId, packetId, e);
                return false;
            }
        });
    }

    @Override
    public CompletableFuture<Optional<QueuedMessage>> removeInflightMessage(String clientId, int packetId) {
        return CompletableFuture.supplyAsync(() -> {
            try {
                ClientSession session = sessions.get(clientId);
                if (session == null) {
                    return Optional.empty();
                }
                
                QueuedMessage removed = session.removeInflightMessage(packetId);
                if (removed != null) {
                    // 更新统计信息
                    stats.inflightMessageCount.decrement();
                    
                    log.debug("传输中消息移除成功: clientId={}, packetId={}, messageId={}", 
                        clientId, packetId, removed.getMessageId());
                }
                
                return Optional.ofNullable(removed);
                
            } catch (Exception e) {
                log.error("移除传输中消息失败: clientId={}, packetId={}", clientId, packetId, e);
                return Optional.empty();
            }
        });
    }

    @Override
    public Optional<QueuedMessage> getInflightMessage(String clientId, int packetId) {
        ClientSession session = sessions.get(clientId);
        if (session != null) {
            return Optional.ofNullable(session.getInflightMessages().get(packetId));
        }
        return Optional.empty();
    }

    @Override
    public void updateSessionActivity(String clientId) {
        ClientSession session = sessions.get(clientId);
        if (session != null) {
            session.updateLastActivity();
        }
    }

    @Override
    public boolean hasSession(String clientId) {
        return sessions.containsKey(clientId);
    }

    @Override
    public Collection<ClientSession> getActiveSessions() {
        return sessions.values().stream()
                .filter(session -> session.getState() == ClientSession.SessionState.ACTIVE)
                .collect(Collectors.toList());
    }

    @Override
    public long getSessionCount() {
        return sessions.size();
    }

    @Override
    public CompletableFuture<Integer> cleanupExpiredSessions() {
        return CompletableFuture.supplyAsync(() -> {
            log.info("开始清理过期会话");
            
            int cleanupCount = 0;
            LocalDateTime now = LocalDateTime.now();
            
            for (ClientSession session : sessions.values()) {
                try {
                    // 检查会话是否过期（简单实现：非活跃状态且空闲超过1小时）
                    if (session.getState() != ClientSession.SessionState.ACTIVE && 
                        session.getIdleTimeMs() > 60 * 60 * 1000) {
                        
                        log.info("清理过期会话: clientId={}, sessionId={}, idleTime={}ms", 
                            session.getClientId(), session.getSessionId(), session.getIdleTimeMs());
                        
                        removeSession(session.getClientId()).join();
                        cleanupCount++;
                    }
                } catch (Exception e) {
                    log.warn("清理会话异常: clientId={}", session.getClientId(), e);
                }
            }
            
            log.info("过期会话清理完成，清理数量: {}", cleanupCount);
            return cleanupCount;
        });
    }

    @Override
    public SessionStats getSessionStats() {
        return stats;
    }

    @Override
    public void addSessionListener(SessionEventListener listener) {
        listeners.add(listener);
        log.debug("添加会话事件监听器: {}", listener.getClass().getSimpleName());
    }

    @Override
    public void removeSessionListener(SessionEventListener listener) {
        listeners.remove(listener);
        log.debug("移除会话事件监听器: {}", listener.getClass().getSimpleName());
    }

    /**
     * 生成唯一会话ID
     *
     * @return 会话ID
     */
    private String generateSessionId() {
        return "sess_" + UUID.randomUUID().toString().replace("-", "");
    }

    /**
     * 清理订阅索引
     *
     * @param clientId 客户端ID
     * @param subscriptions 订阅集合
     */
    private void cleanupSubscriptionIndex(String clientId, Collection<TopicSubscription> subscriptions) {
        for (TopicSubscription subscription : subscriptions) {
            Set<String> subscribers = topicSubscriptions.get(subscription.getTopicFilter());
            if (subscribers != null) {
                subscribers.remove(clientId);
                if (subscribers.isEmpty()) {
                    topicSubscriptions.remove(subscription.getTopicFilter());
                }
            }
        }
    }

    /**
     * 主题过滤器匹配
     * TODO: 这里需要实现完整的MQTT主题匹配逻辑
     *
     * @param topic 主题
     * @param filter 过滤器
     * @return 是否匹配
     */
    private boolean matchesTopicFilter(String topic, String filter) {
        // 简单实现，后续需要完善通配符匹配逻辑
        if (filter.equals("#")) {
            return true;
        }
        if (filter.equals(topic)) {
            return true;
        }
        if (filter.endsWith("/#")) {
            String prefix = filter.substring(0, filter.length() - 2);
            return topic.startsWith(prefix);
        }
        if (filter.contains("+")) {
            // TODO: 实现单级通配符匹配
            return false;
        }
        return false;
    }

    // 事件触发方法
    private void triggerSessionCreated(ClientSession session) {
        listeners.forEach(listener -> {
            try {
                listener.onSessionCreated(session);
            } catch (Exception e) {
                log.warn("会话事件监听器异常", e);
            }
        });
    }

    private void triggerSessionDestroyed(ClientSession session) {
        listeners.forEach(listener -> {
            try {
                listener.onSessionDestroyed(session);
            } catch (Exception e) {
                log.warn("会话事件监听器异常", e);
            }
        });
    }

    private void triggerSessionStateChanged(ClientSession session, 
                                          ClientSession.SessionState oldState, 
                                          ClientSession.SessionState newState) {
        listeners.forEach(listener -> {
            try {
                listener.onSessionStateChanged(session, oldState, newState);
            } catch (Exception e) {
                log.warn("会话事件监听器异常", e);
            }
        });
    }

    private void triggerSubscriptionAdded(ClientSession session, TopicSubscription subscription) {
        listeners.forEach(listener -> {
            try {
                listener.onSubscriptionAdded(session, subscription);
            } catch (Exception e) {
                log.warn("会话事件监听器异常", e);
            }
        });
    }

    private void triggerSubscriptionRemoved(ClientSession session, TopicSubscription subscription) {
        listeners.forEach(listener -> {
            try {
                listener.onSubscriptionRemoved(session, subscription);
            } catch (Exception e) {
                log.warn("会话事件监听器异常", e);
            }
        });
    }

    private void triggerMessageQueued(ClientSession session, QueuedMessage message) {
        listeners.forEach(listener -> {
            try {
                listener.onMessageQueued(session, message);
            } catch (Exception e) {
                log.warn("会话事件监听器异常", e);
            }
        });
    }

    private void triggerMessageDequeued(ClientSession session, QueuedMessage message) {
        listeners.forEach(listener -> {
            try {
                listener.onMessageDequeued(session, message);
            } catch (Exception e) {
                log.warn("会话事件监听器异常", e);
            }
        });
    }

    /**
     * 会话创建结果实现
     */
    private static class SessionResultImpl implements SessionResult {
        private final ClientSession session;
        private final boolean isNewSession;
        private final boolean isSessionPresent;
        
        public SessionResultImpl(ClientSession session, boolean isNewSession, boolean isSessionPresent) {
            this.session = session;
            this.isNewSession = isNewSession;
            this.isSessionPresent = isSessionPresent;
        }

        @Override
        public ClientSession getSession() {
            return session;
        }

        @Override
        public boolean isNewSession() {
            return isNewSession;
        }

        @Override
        public boolean isSessionPresent() {
            return isSessionPresent;
        }
    }

    /**
     * 会话统计信息实现
     */
    private static class SessionStatsImpl implements SessionStats {
        private final LongAdder totalSessions = new LongAdder();
        private final LongAdder activeSessions = new LongAdder();
        private final LongAdder totalSubscriptions = new LongAdder();
        private final LongAdder queuedMessageCount = new LongAdder();
        private final LongAdder inflightMessageCount = new LongAdder();
        private final AtomicLong startTime = new AtomicLong(System.currentTimeMillis());

        @Override
        public long getTotalSessions() {
            return totalSessions.sum();
        }

        @Override
        public long getActiveSessions() {
            return activeSessions.sum();
        }

        @Override
        public long getInactiveSessions() {
            return getTotalSessions() - getActiveSessions();
        }

        @Override
        public long getTotalSubscriptions() {
            return totalSubscriptions.sum();
        }

        @Override
        public long getQueuedMessageCount() {
            return queuedMessageCount.sum();
        }

        @Override
        public long getInflightMessageCount() {
            return inflightMessageCount.sum();
        }

        @Override
        public long getAverageSessionDuration() {
            long totalTime = System.currentTimeMillis() - startTime.get();
            long totalSess = getTotalSessions();
            return totalSess > 0 ? totalTime / totalSess : 0;
        }
    }
}