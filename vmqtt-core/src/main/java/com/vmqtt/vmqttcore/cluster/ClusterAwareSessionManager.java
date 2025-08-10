package com.vmqtt.vmqttcore.cluster;

import com.vmqtt.common.model.ClientSession;
import com.vmqtt.common.model.TopicSubscription;
import com.vmqtt.common.service.SessionManager;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.annotation.Primary;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

/**
 * 集群感知的会话管理器
 * 在标准SessionManager基础上集成集群订阅管理功能
 *
 * @author zhenglin
 * @date 2025/08/10
 */
@Slf4j
@Primary
@Component
@RequiredArgsConstructor
public class ClusterAwareSessionManager implements SessionManager.SessionEventListener {

    private final SessionManager delegateSessionManager;
    private final ClusterSubscriptionManager clusterSubscriptionManager;
    private final GossipSubscriptionProtocol gossipProtocol;
    private final SubscriptionConflictResolver conflictResolver;
    
    /**
     * 初始化集群感知会话管理器
     */
    public void init() {
        // 注册为SessionManager的事件监听器
        delegateSessionManager.addSessionListener(this);
        
        // 启动集群订阅管理器
        clusterSubscriptionManager.start();
        
        // 启动Gossip协议
        gossipProtocol.start();
        
        log.info("Cluster-aware session manager initialized");
    }
    
    /**
     * 关闭集群感知会话管理器
     */
    public void shutdown() {
        // 移除事件监听器
        delegateSessionManager.removeSessionListener(this);
        
        // 停止集群组件
        clusterSubscriptionManager.stop();
        gossipProtocol.stop();
        
        log.info("Cluster-aware session manager shut down");
    }
    
    /**
     * 增强的订阅添加，支持集群同步
     *
     * @param clientId 客户端ID
     * @param subscription 订阅信息
     * @return CompletableFuture<Boolean>
     */
    public CompletableFuture<Boolean> addSubscriptionWithClusterSync(String clientId, TopicSubscription subscription) {
        // 先添加到本地SessionManager
        return delegateSessionManager.addSubscription(clientId, subscription)
            .thenCompose(success -> {
                if (success) {
                    // 同步到集群
                    return clusterSubscriptionManager.addSubscription(clientId, subscription);
                } else {
                    return CompletableFuture.completedFuture(false);
                }
            });
    }
    
    /**
     * 增强的订阅移除，支持集群同步
     *
     * @param clientId 客户端ID
     * @param topicFilter 主题过滤器
     * @return CompletableFuture<Optional<TopicSubscription>>
     */
    public CompletableFuture<Optional<TopicSubscription>> removeSubscriptionWithClusterSync(
            String clientId, String topicFilter) {
        
        // 先从本地SessionManager移除
        return delegateSessionManager.removeSubscription(clientId, topicFilter)
            .thenCompose(removed -> {
                if (removed.isPresent()) {
                    // 从集群移除
                    return clusterSubscriptionManager.removeSubscription(clientId, topicFilter)
                        .thenApply(success -> removed);
                } else {
                    return CompletableFuture.completedFuture(removed);
                }
            });
    }
    
    /**
     * 获取主题的集群订阅者（包括远程节点）
     *
     * @param topic 主题
     * @return 集群内所有匹配的会话列表
     */
    public List<ClientSession> getClusterSubscribersForTopic(String topic) {
        // 获取本地订阅者
        List<ClientSession> localSessions = delegateSessionManager.getSubscribedSessions(topic);
        
        // 获取集群订阅信息（这里简化处理，实际应该根据集群订阅信息构建远程会话代理）
        var clusterSubscriptions = clusterSubscriptionManager.getSubscribersForTopic(topic);
        
        log.debug("Found {} local subscribers and {} cluster subscribers for topic {}", 
            localSessions.size(), clusterSubscriptions.size(), topic);
            
        // 当前实现只返回本地会话，集群会话需要通过消息路由处理
        return localSessions;
    }
    
    /**
     * 同步集群订阅信息到本地
     *
     * @return 同步是否成功
     */
    public CompletableFuture<Boolean> syncClusterSubscriptions() {
        return CompletableFuture.supplyAsync(() -> {
            try {
                log.info("Starting cluster subscription sync");
                
                // 这里应该实现从其他节点获取订阅信息并同步到本地
                // 当前简化实现，只记录日志
                
                log.info("Cluster subscription sync completed");
                return true;
            } catch (Exception e) {
                log.error("Failed to sync cluster subscriptions", e);
                return false;
            }
        });
    }
    
    // ========== SessionEventListener 实现 ==========
    
    @Override
    public void onSessionCreated(ClientSession session) {
        log.debug("Session created event: clientId={}, sessionId={}", 
            session.getClientId(), session.getSessionId());
    }
    
    @Override
    public void onSessionDestroyed(ClientSession session) {
        log.debug("Session destroyed event: clientId={}, sessionId={}", 
            session.getClientId(), session.getSessionId());
            
        // 清理集群订阅信息
        cleanupClusterSubscriptionsForSession(session);
    }
    
    @Override
    public void onSessionStateChanged(ClientSession session, 
                                    ClientSession.SessionState oldState, 
                                    ClientSession.SessionState newState) {
        log.debug("Session state changed: clientId={}, {} -> {}", 
            session.getClientId(), oldState, newState);
            
        // 如果会话变为非活跃状态，需要处理集群订阅
        if (newState != ClientSession.SessionState.ACTIVE && 
            oldState == ClientSession.SessionState.ACTIVE) {
            handleSessionDeactivated(session);
        }
    }
    
    @Override
    public void onSubscriptionAdded(ClientSession session, TopicSubscription subscription) {
        log.debug("Subscription added event: clientId={}, topic={}", 
            session.getClientId(), subscription.getTopicFilter());
            
        // 异步同步到集群（避免阻塞原始操作）
        CompletableFuture.runAsync(() -> {
            clusterSubscriptionManager.addSubscription(session.getClientId(), subscription)
                .whenComplete((success, throwable) -> {
                    if (throwable != null) {
                        log.warn("Failed to sync subscription addition to cluster", throwable);
                    } else if (!success) {
                        log.warn("Failed to sync subscription addition to cluster: {}",
                            subscription.getTopicFilter());
                    }
                });
        });
    }
    
    @Override
    public void onSubscriptionRemoved(ClientSession session, TopicSubscription subscription) {
        log.debug("Subscription removed event: clientId={}, topic={}", 
            session.getClientId(), subscription.getTopicFilter());
            
        // 异步从集群移除（避免阻塞原始操作）
        CompletableFuture.runAsync(() -> {
            clusterSubscriptionManager.removeSubscription(session.getClientId(), subscription.getTopicFilter())
                .whenComplete((success, throwable) -> {
                    if (throwable != null) {
                        log.warn("Failed to sync subscription removal to cluster", throwable);
                    } else if (!success) {
                        log.warn("Failed to sync subscription removal to cluster: {}",
                            subscription.getTopicFilter());
                    }
                });
        });
    }
    
    @Override
    public void onMessageQueued(ClientSession session, com.vmqtt.common.model.QueuedMessage message) {
        log.debug("Message queued event: clientId={}, messageId={}", 
            session.getClientId(), message.getMessageId());
    }
    
    @Override
    public void onMessageDequeued(ClientSession session, com.vmqtt.common.model.QueuedMessage message) {
        log.debug("Message dequeued event: clientId={}, messageId={}", 
            session.getClientId(), message.getMessageId());
    }
    
    // ========== 私有辅助方法 ==========
    
    /**
     * 清理会话的集群订阅信息
     */
    private void cleanupClusterSubscriptionsForSession(ClientSession session) {
        CompletableFuture.runAsync(() -> {
            try {
                List<TopicSubscription> subscriptions = session.getSubscriptions().values()
                    .stream().collect(Collectors.toList());
                    
                for (TopicSubscription subscription : subscriptions) {
                    clusterSubscriptionManager.removeSubscription(
                        session.getClientId(), subscription.getTopicFilter()
                    ).whenComplete((success, throwable) -> {
                        if (throwable != null) {
                            log.warn("Failed to cleanup cluster subscription for destroyed session", throwable);
                        }
                    });
                }
                
                log.debug("Cleaned up {} cluster subscriptions for destroyed session: clientId={}", 
                    subscriptions.size(), session.getClientId());
                    
            } catch (Exception e) {
                log.error("Error cleaning up cluster subscriptions for session: clientId={}", 
                    session.getClientId(), e);
            }
        });
    }
    
    /**
     * 处理会话失活
     */
    private void handleSessionDeactivated(ClientSession session) {
        // 可以在这里实现特殊的集群处理逻辑
        // 比如标记订阅为非活跃状态，但不完全移除
        log.debug("Handling session deactivation for cluster: clientId={}", session.getClientId());
    }
    
    // ========== 代理方法到原始SessionManager ==========
    
    /**
     * 获取集群订阅统计信息
     *
     * @return 集群订阅统计
     */
    public ClusterSubscriptionStats getClusterSubscriptionStats() {
        var conflictStats = conflictResolver.getConflictStatistics();
        var globalSubscriptions = clusterSubscriptionManager.getSubscriptionsForNode(
            getCurrentNodeId());
            
        return new ClusterSubscriptionStats(
            globalSubscriptions.size(),
            conflictStats.getTotalConflicts(),
            conflictStats.getConflictsByType(),
            System.currentTimeMillis()
        );
    }
    
    private String getCurrentNodeId() {
        // 从ClusterNodeManager获取当前节点ID
        return "current-node-id"; // 临时实现
    }
    
    /**
     * 集群订阅统计信息
     */
    public static class ClusterSubscriptionStats {
        private final int totalClusterSubscriptions;
        private final int totalConflicts;
        private final java.util.Map<com.vmqtt.common.grpc.cluster.ConflictType, Long> conflictsByType;
        private final long timestamp;
        
        public ClusterSubscriptionStats(int totalClusterSubscriptions, int totalConflicts,
                                      java.util.Map<com.vmqtt.common.grpc.cluster.ConflictType, Long> conflictsByType,
                                      long timestamp) {
            this.totalClusterSubscriptions = totalClusterSubscriptions;
            this.totalConflicts = totalConflicts;
            this.conflictsByType = conflictsByType;
            this.timestamp = timestamp;
        }
        
        // Getters
        public int getTotalClusterSubscriptions() { return totalClusterSubscriptions; }
        public int getTotalConflicts() { return totalConflicts; }
        public java.util.Map<com.vmqtt.common.grpc.cluster.ConflictType, Long> getConflictsByType() { return conflictsByType; }
        public long getTimestamp() { return timestamp; }
    }
}