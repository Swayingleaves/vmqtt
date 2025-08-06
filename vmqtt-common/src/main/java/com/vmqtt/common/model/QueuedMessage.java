/**
 * 排队消息信息
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
import java.util.concurrent.ConcurrentHashMap;

/**
 * 排队消息模型
 * 用于存储待发送、传输中或需要确认的消息
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class QueuedMessage implements Comparable<QueuedMessage> {
    
    /**
     * 消息唯一标识
     */
    private String messageId;
    
    /**
     * 包ID（用于QoS 1和2的消息确认）
     */
    private Integer packetId;
    
    /**
     * 目标客户端ID
     */
    private String clientId;
    
    /**
     * 会话ID
     */
    private String sessionId;
    
    /**
     * 主题
     */
    private String topic;
    
    /**
     * 消息负载
     */
    private byte[] payload;
    
    /**
     * QoS等级
     */
    private MqttQos qos;
    
    /**
     * 保留消息标志
     */
    @Builder.Default
    private boolean retain = false;
    
    /**
     * 重复消息标志
     */
    @Builder.Default
    private boolean duplicate = false;
    
    /**
     * 消息创建时间
     */
    @Builder.Default
    private LocalDateTime createdAt = LocalDateTime.now();
    
    /**
     * 入队时间
     */
    @Builder.Default
    private LocalDateTime queuedAt = LocalDateTime.now();
    
    /**
     * 最后发送时间
     */
    private LocalDateTime lastSentAt;
    
    /**
     * 过期时间
     */
    private LocalDateTime expiresAt;
    
    /**
     * 消息状态
     */
    @Builder.Default
    private MessageState state = MessageState.QUEUED;
    
    /**
     * 重试次数
     */
    @Builder.Default
    private int retryCount = 0;
    
    /**
     * 最大重试次数
     */
    @Builder.Default
    private int maxRetries = 3;
    
    /**
     * 优先级（数值越小优先级越高）
     */
    @Builder.Default
    private int priority = 0;
    
    /**
     * 消息属性（MQTT 5.0）
     */
    @Builder.Default
    private Map<String, Object> properties = new ConcurrentHashMap<>();
    
    /**
     * 用户属性（MQTT 5.0）
     */
    @Builder.Default
    private Map<String, String> userProperties = new ConcurrentHashMap<>();
    
    /**
     * 消息状态枚举
     */
    public enum MessageState {
        /**
         * 已排队，等待发送
         */
        QUEUED,
        
        /**
         * 发送中
         */
        SENDING,
        
        /**
         * 已发送，等待确认（QoS 1）
         */
        SENT_WAITING_PUBACK,
        
        /**
         * 已发送，等待接收确认（QoS 2第一步）
         */
        SENT_WAITING_PUBREC,
        
        /**
         * 已接收确认，等待完成确认（QoS 2第二步）
         */
        RECEIVED_PUBREC_WAITING_PUBCOMP,
        
        /**
         * 消息已确认完成
         */
        ACKNOWLEDGED,
        
        /**
         * 消息过期
         */
        EXPIRED,
        
        /**
         * 消息发送失败
         */
        FAILED,
        
        /**
         * 消息被丢弃
         */
        DROPPED
    }
    
    /**
     * 检查消息是否已过期
     *
     * @return 如果消息已过期返回true
     */
    public boolean isExpired() {
        if (expiresAt == null) {
            return false;
        }
        return LocalDateTime.now().isAfter(expiresAt);
    }
    
    /**
     * 检查消息是否需要确认
     *
     * @return 如果需要确认返回true
     */
    public boolean requiresAcknowledgment() {
        return qos.requiresAcknowledgment();
    }
    
    /**
     * 检查是否可以重试
     *
     * @return 如果可以重试返回true
     */
    public boolean canRetry() {
        return retryCount < maxRetries && !isExpired();
    }
    
    /**
     * 增加重试次数
     */
    public void incrementRetryCount() {
        this.retryCount++;
        this.lastSentAt = LocalDateTime.now();
        
        // 如果达到最大重试次数，标记为失败
        if (retryCount >= maxRetries) {
            this.state = MessageState.FAILED;
        }
    }
    
    /**
     * 标记消息为发送中
     */
    public void markAsSending() {
        this.state = MessageState.SENDING;
        this.lastSentAt = LocalDateTime.now();
    }
    
    /**
     * 标记消息为已发送，等待确认
     */
    public void markAsSent() {
        this.lastSentAt = LocalDateTime.now();
        
        switch (qos) {
            case AT_MOST_ONCE:
                this.state = MessageState.ACKNOWLEDGED;
                break;
            case AT_LEAST_ONCE:
                this.state = MessageState.SENT_WAITING_PUBACK;
                break;
            case EXACTLY_ONCE:
                this.state = MessageState.SENT_WAITING_PUBREC;
                break;
        }
    }
    
    /**
     * 标记消息为已确认
     */
    public void markAsAcknowledged() {
        this.state = MessageState.ACKNOWLEDGED;
    }
    
    /**
     * 标记消息为失败
     */
    public void markAsFailed() {
        this.state = MessageState.FAILED;
    }
    
    /**
     * 标记消息为过期
     */
    public void markAsExpired() {
        this.state = MessageState.EXPIRED;
    }
    
    /**
     * 标记消息为丢弃
     */
    public void markAsDropped() {
        this.state = MessageState.DROPPED;
    }
    
    /**
     * 处理PUBREC响应（QoS 2第一步）
     */
    public void handlePubRec() {
        if (state == MessageState.SENT_WAITING_PUBREC) {
            this.state = MessageState.RECEIVED_PUBREC_WAITING_PUBCOMP;
        }
    }
    
    /**
     * 处理PUBCOMP响应（QoS 2第二步）
     */
    public void handlePubComp() {
        if (state == MessageState.RECEIVED_PUBREC_WAITING_PUBCOMP) {
            this.state = MessageState.ACKNOWLEDGED;
        }
    }
    
    /**
     * 获取消息大小
     *
     * @return 消息大小（字节）
     */
    public int getMessageSize() {
        int size = 0;
        
        // 主题长度
        if (topic != null) {
            size += topic.getBytes().length;
        }
        
        // 负载长度
        if (payload != null) {
            size += payload.length;
        }
        
        // 固定头部和可变头部的估计大小
        size += 10; // 大致估计
        
        return size;
    }
    
    /**
     * 获取消息在队列中的等待时间（毫秒）
     *
     * @return 等待时间
     */
    public long getQueueWaitTimeMs() {
        LocalDateTime compareTime = lastSentAt != null ? lastSentAt : LocalDateTime.now();
        return java.time.Duration.between(queuedAt, compareTime).toMillis();
    }
    
    /**
     * 获取消息的生存时间（毫秒）
     *
     * @return 生存时间
     */
    public long getLifetimeMs() {
        return java.time.Duration.between(createdAt, LocalDateTime.now()).toMillis();
    }
    
    /**
     * 设置消息过期时间
     *
     * @param ttlSeconds 生存时间（秒）
     */
    public void setTtl(long ttlSeconds) {
        if (ttlSeconds > 0) {
            this.expiresAt = LocalDateTime.now().plusSeconds(ttlSeconds);
        }
    }
    
    /**
     * 检查消息是否处于最终状态
     *
     * @return 如果处于最终状态返回true
     */
    public boolean isFinalState() {
        return state == MessageState.ACKNOWLEDGED || 
               state == MessageState.EXPIRED || 
               state == MessageState.FAILED || 
               state == MessageState.DROPPED;
    }
    
    /**
     * 比较方法，用于排序
     * 优先级高的消息排在前面，优先级相同时按创建时间排序
     */
    @Override
    public int compareTo(QueuedMessage other) {
        // 首先按优先级排序（数值小的优先级高）
        int priorityComparison = Integer.compare(this.priority, other.priority);
        if (priorityComparison != 0) {
            return priorityComparison;
        }
        
        // 优先级相同时按创建时间排序（早创建的优先）
        return this.createdAt.compareTo(other.createdAt);
    }
    
    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null || getClass() != obj.getClass()) return false;
        
        QueuedMessage that = (QueuedMessage) obj;
        return messageId != null ? messageId.equals(that.messageId) : that.messageId == null;
    }
    
    @Override
    public int hashCode() {
        return messageId != null ? messageId.hashCode() : 0;
    }
}