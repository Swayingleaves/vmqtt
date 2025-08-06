/**
 * 客户端会话信息
 *
 * @author zhenglin
 * @date 2025/08/06
 */
package com.vmqtt.common.model;

import com.vmqtt.common.protocol.MqttQos;
import lombok.Data;
import lombok.Builder;
import lombok.AllArgsConstructor;
import lombok.NoArgsConstructor;

import java.time.LocalDateTime;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListSet;

/**
 * 客户端会话模型
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class ClientSession {
    
    /**
     * 会话唯一标识
     */
    private String sessionId;
    
    /**
     * 客户端ID
     */
    private String clientId;
    
    /**
     * 关联的连接ID
     */
    private String connectionId;
    
    /**
     * 会话创建时间
     */
    @Builder.Default
    private LocalDateTime createdAt = LocalDateTime.now();
    
    /**
     * 最后活动时间
     */
    @Builder.Default
    private LocalDateTime lastActivity = LocalDateTime.now();
    
    /**
     * 清理会话标志
     */
    @Builder.Default
    private boolean cleanSession = true;
    
    /**
     * 会话状态
     */
    @Builder.Default
    private SessionState state = SessionState.ACTIVE;
    
    /**
     * 订阅信息
     */
    @Builder.Default
    private Map<String, TopicSubscription> subscriptions = new ConcurrentHashMap<>();
    
    /**
     * 等待发送的消息队列
     */
    @Builder.Default
    private Set<QueuedMessage> pendingMessages = new ConcurrentSkipListSet<>();
    
    /**
     * 已发送待确认的消息（QoS 1和2）
     */
    @Builder.Default
    private Map<Integer, QueuedMessage> inflightMessages = new ConcurrentHashMap<>();
    
    /**
     * 会话属性（MQTT 5.0）
     */
    @Builder.Default
    private Map<String, Object> attributes = new ConcurrentHashMap<>();
    
    /**
     * 会话统计信息
     */
    @Builder.Default
    private SessionStats stats = new SessionStats();
    
    /**
     * 会话状态枚举
     */
    public enum SessionState {
        /**
         * 活跃状态
         */
        ACTIVE,
        
        /**
         * 非活跃状态（连接断开但会话保留）
         */
        INACTIVE,
        
        /**
         * 过期状态
         */
        EXPIRED,
        
        /**
         * 已清理状态
         */
        CLEANED
    }
    
    /**
     * 会话统计信息
     */
    @Data
    @Builder
    @NoArgsConstructor
    @AllArgsConstructor
    public static class SessionStats {
        /**
         * 发布消息数
         */
        @Builder.Default
        private long messagesPublished = 0L;
        
        /**
         * 接收消息数
         */
        @Builder.Default
        private long messagesReceived = 0L;
        
        /**
         * 订阅数
         */
        @Builder.Default
        private int subscriptionCount = 0;
        
        /**
         * 排队消息数
         */
        @Builder.Default
        private int queuedMessageCount = 0;
        
        /**
         * 传输中消息数
         */
        @Builder.Default
        private int inflightMessageCount = 0;
        
        /**
         * 丢弃消息数
         */
        @Builder.Default
        private long messagesDropped = 0L;
    }
    
    /**
     * 添加订阅
     *
     * @param subscription 订阅信息
     */
    public void addSubscription(TopicSubscription subscription) {
        subscriptions.put(subscription.getTopicFilter(), subscription);
        stats.subscriptionCount = subscriptions.size();
        updateLastActivity();
    }
    
    /**
     * 移除订阅
     *
     * @param topicFilter 主题过滤器
     * @return 被移除的订阅信息
     */
    public TopicSubscription removeSubscription(String topicFilter) {
        TopicSubscription removed = subscriptions.remove(topicFilter);
        if (removed != null) {
            stats.subscriptionCount = subscriptions.size();
            updateLastActivity();
        }
        return removed;
    }
    
    /**
     * 检查是否订阅了指定主题
     *
     * @param topic 主题
     * @return 如果订阅了返回true
     */
    public boolean isSubscribedTo(String topic) {
        return subscriptions.values().stream()
                .anyMatch(sub -> matchesTopicFilter(topic, sub.getTopicFilter()));
    }
    
    /**
     * 获取匹配的订阅信息
     *
     * @param topic 主题
     * @return 匹配的订阅信息
     */
    public TopicSubscription getMatchingSubscription(String topic) {
        return subscriptions.values().stream()
                .filter(sub -> matchesTopicFilter(topic, sub.getTopicFilter()))
                .findFirst()
                .orElse(null);
    }
    
    /**
     * 添加排队消息
     *
     * @param message 消息
     */
    public void addQueuedMessage(QueuedMessage message) {
        pendingMessages.add(message);
        stats.queuedMessageCount = pendingMessages.size();
        updateLastActivity();
    }
    
    /**
     * 移除排队消息
     *
     * @param messageId 消息ID
     * @return 被移除的消息
     */
    public QueuedMessage removeQueuedMessage(String messageId) {
        QueuedMessage removed = pendingMessages.stream()
                .filter(msg -> messageId.equals(msg.getMessageId()))
                .findFirst()
                .orElse(null);
        
        if (removed != null) {
            pendingMessages.remove(removed);
            stats.queuedMessageCount = pendingMessages.size();
        }
        
        return removed;
    }
    
    /**
     * 添加传输中消息
     *
     * @param packetId 包ID
     * @param message 消息
     */
    public void addInflightMessage(int packetId, QueuedMessage message) {
        inflightMessages.put(packetId, message);
        stats.inflightMessageCount = inflightMessages.size();
    }
    
    /**
     * 移除传输中消息
     *
     * @param packetId 包ID
     * @return 被移除的消息
     */
    public QueuedMessage removeInflightMessage(int packetId) {
        QueuedMessage removed = inflightMessages.remove(packetId);
        if (removed != null) {
            stats.inflightMessageCount = inflightMessages.size();
        }
        return removed;
    }
    
    /**
     * 更新最后活动时间
     */
    public void updateLastActivity() {
        this.lastActivity = LocalDateTime.now();
    }
    
    /**
     * 增加发布消息计数
     */
    public void incrementMessagesPublished() {
        stats.messagesPublished++;
        updateLastActivity();
    }
    
    /**
     * 增加接收消息计数
     */
    public void incrementMessagesReceived() {
        stats.messagesReceived++;
        updateLastActivity();
    }
    
    /**
     * 增加丢弃消息计数
     */
    public void incrementMessagesDropped() {
        stats.messagesDropped++;
    }
    
    /**
     * 检查会话是否为空
     *
     * @return 如果会话为空返回true
     */
    public boolean isEmpty() {
        return subscriptions.isEmpty() && 
               pendingMessages.isEmpty() && 
               inflightMessages.isEmpty();
    }
    
    /**
     * 获取会话存活时间（毫秒）
     *
     * @return 存活时间
     */
    public long getLifetimeMs() {
        return java.time.Duration.between(createdAt, LocalDateTime.now()).toMillis();
    }
    
    /**
     * 获取空闲时间（毫秒）
     *
     * @return 空闲时间
     */
    public long getIdleTimeMs() {
        return java.time.Duration.between(lastActivity, LocalDateTime.now()).toMillis();
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
        return false;
    }
}