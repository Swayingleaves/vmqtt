/**
 * 会话管理器接口
 *
 * @author zhenglin
 * @date 2025/08/06
 */
package com.vmqtt.common.service;

import com.vmqtt.common.model.ClientSession;
import com.vmqtt.common.model.TopicSubscription;
import com.vmqtt.common.model.QueuedMessage;
import com.vmqtt.common.protocol.MqttQos;

import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

/**
 * 会话管理器接口
 * 负责管理MQTT客户端会话的生命周期和状态
 */
public interface SessionManager {
    
    /**
     * 创建或获取客户端会话
     *
     * @param clientId 客户端ID
     * @param cleanSession 清理会话标志
     * @param connectionId 连接ID
     * @return 会话信息和是否为新会话的标志
     */
    CompletableFuture<SessionResult> createOrGetSession(String clientId, boolean cleanSession, String connectionId);
    
    /**
     * 获取客户端会话
     *
     * @param clientId 客户端ID
     * @return 会话信息
     */
    Optional<ClientSession> getSession(String clientId);
    
    /**
     * 获取会话通过会话ID
     *
     * @param sessionId 会话ID
     * @return 会话信息
     */
    Optional<ClientSession> getSessionById(String sessionId);
    
    /**
     * 移除客户端会话
     *
     * @param clientId 客户端ID
     * @return 被移除的会话信息
     */
    CompletableFuture<Optional<ClientSession>> removeSession(String clientId);
    
    /**
     * 清理会话
     *
     * @param clientId 客户端ID
     * @return 异步操作结果
     */
    CompletableFuture<Boolean> cleanSession(String clientId);
    
    /**
     * 添加订阅
     *
     * @param clientId 客户端ID
     * @param subscription 订阅信息
     * @return 异步操作结果
     */
    CompletableFuture<Boolean> addSubscription(String clientId, TopicSubscription subscription);
    
    /**
     * 移除订阅
     *
     * @param clientId 客户端ID
     * @param topicFilter 主题过滤器
     * @return 被移除的订阅信息
     */
    CompletableFuture<Optional<TopicSubscription>> removeSubscription(String clientId, String topicFilter);
    
    /**
     * 获取客户端的所有订阅
     *
     * @param clientId 客户端ID
     * @return 订阅列表
     */
    List<TopicSubscription> getSubscriptions(String clientId);
    
    /**
     * 获取订阅了指定主题的所有会话
     *
     * @param topic 主题
     * @return 匹配的会话列表
     */
    List<ClientSession> getSubscribedSessions(String topic);
    
    /**
     * 添加排队消息
     *
     * @param clientId 客户端ID
     * @param message 消息
     * @return 异步操作结果
     */
    CompletableFuture<Boolean> addQueuedMessage(String clientId, QueuedMessage message);
    
    /**
     * 获取排队消息
     *
     * @param clientId 客户端ID
     * @param limit 获取数量限制
     * @return 排队消息列表
     */
    List<QueuedMessage> getQueuedMessages(String clientId, int limit);
    
    /**
     * 移除排队消息
     *
     * @param clientId 客户端ID
     * @param messageId 消息ID
     * @return 被移除的消息
     */
    CompletableFuture<Optional<QueuedMessage>> removeQueuedMessage(String clientId, String messageId);
    
    /**
     * 添加传输中消息
     *
     * @param clientId 客户端ID
     * @param packetId 包ID
     * @param message 消息
     * @return 异步操作结果
     */
    CompletableFuture<Boolean> addInflightMessage(String clientId, int packetId, QueuedMessage message);
    
    /**
     * 移除传输中消息
     *
     * @param clientId 客户端ID
     * @param packetId 包ID
     * @return 被移除的消息
     */
    CompletableFuture<Optional<QueuedMessage>> removeInflightMessage(String clientId, int packetId);
    
    /**
     * 获取传输中消息
     *
     * @param clientId 客户端ID
     * @param packetId 包ID
     * @return 传输中消息
     */
    Optional<QueuedMessage> getInflightMessage(String clientId, int packetId);
    
    /**
     * 更新会话的最后活动时间
     *
     * @param clientId 客户端ID
     */
    void updateSessionActivity(String clientId);
    
    /**
     * 检查客户端是否有会话
     *
     * @param clientId 客户端ID
     * @return 如果存在会话返回true
     */
    boolean hasSession(String clientId);
    
    /**
     * 获取所有活跃会话
     *
     * @return 活跃会话集合
     */
    Collection<ClientSession> getActiveSessions();
    
    /**
     * 获取会话数量
     *
     * @return 会话数量
     */
    long getSessionCount();
    
    /**
     * 清理过期会话
     *
     * @return 清理的会话数量
     */
    CompletableFuture<Integer> cleanupExpiredSessions();
    
    /**
     * 获取会话统计信息
     *
     * @return 会话统计信息
     */
    SessionStats getSessionStats();
    
    /**
     * 注册会话事件监听器
     *
     * @param listener 会话事件监听器
     */
    void addSessionListener(SessionEventListener listener);
    
    /**
     * 移除会话事件监听器
     *
     * @param listener 会话事件监听器
     */
    void removeSessionListener(SessionEventListener listener);
    
    /**
     * 会话创建结果
     */
    interface SessionResult {
        /**
         * 获取会话信息
         *
         * @return 会话信息
         */
        ClientSession getSession();
        
        /**
         * 是否为新会话
         *
         * @return 如果是新会话返回true
         */
        boolean isNewSession();
        
        /**
         * 是否已存在会话
         *
         * @return 如果已存在会话返回true
         */
        boolean isSessionPresent();
    }
    
    /**
     * 会话统计信息
     */
    interface SessionStats {
        /**
         * 获取总会话数
         *
         * @return 总会话数
         */
        long getTotalSessions();
        
        /**
         * 获取活跃会话数
         *
         * @return 活跃会话数
         */
        long getActiveSessions();
        
        /**
         * 获取非活跃会话数
         *
         * @return 非活跃会话数
         */
        long getInactiveSessions();
        
        /**
         * 获取总订阅数
         *
         * @return 总订阅数
         */
        long getTotalSubscriptions();
        
        /**
         * 获取排队消息数
         *
         * @return 排队消息数
         */
        long getQueuedMessageCount();
        
        /**
         * 获取传输中消息数
         *
         * @return 传输中消息数
         */
        long getInflightMessageCount();
        
        /**
         * 获取平均会话持续时间
         *
         * @return 平均会话持续时间（毫秒）
         */
        long getAverageSessionDuration();
    }
    
    /**
     * 会话事件监听器
     */
    interface SessionEventListener {
        /**
         * 会话创建事件
         *
         * @param session 会话信息
         */
        void onSessionCreated(ClientSession session);
        
        /**
         * 会话销毁事件
         *
         * @param session 会话信息
         */
        void onSessionDestroyed(ClientSession session);
        
        /**
         * 会话状态变化事件
         *
         * @param session 会话信息
         * @param oldState 旧状态
         * @param newState 新状态
         */
        void onSessionStateChanged(ClientSession session, 
                                 ClientSession.SessionState oldState, 
                                 ClientSession.SessionState newState);
        
        /**
         * 订阅添加事件
         *
         * @param session 会话信息
         * @param subscription 订阅信息
         */
        void onSubscriptionAdded(ClientSession session, TopicSubscription subscription);
        
        /**
         * 订阅移除事件
         *
         * @param session 会话信息
         * @param subscription 订阅信息
         */
        void onSubscriptionRemoved(ClientSession session, TopicSubscription subscription);
        
        /**
         * 消息入队事件
         *
         * @param session 会话信息
         * @param message 消息
         */
        void onMessageQueued(ClientSession session, QueuedMessage message);
        
        /**
         * 消息出队事件
         *
         * @param session 会话信息
         * @param message 消息
         */
        void onMessageDequeued(ClientSession session, QueuedMessage message);
    }
}