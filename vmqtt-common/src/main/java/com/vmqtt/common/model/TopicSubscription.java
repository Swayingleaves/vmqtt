/**
 * 主题订阅信息
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
 * 主题订阅模型
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class TopicSubscription {
    
    /**
     * 客户端ID
     */
    private String clientId;
    
    /**
     * 会话ID
     */
    private String sessionId;
    
    /**
     * 主题过滤器
     */
    private String topicFilter;
    
    /**
     * 订阅QoS等级
     */
    private MqttQos qos;
    
    /**
     * 订阅时间
     */
    @Builder.Default
    private LocalDateTime subscribedAt = LocalDateTime.now();
    
    /**
     * 最后匹配时间
     */
    private LocalDateTime lastMatchedAt;
    
    /**
     * 订阅选项（MQTT 5.0）
     */
    @Builder.Default
    private SubscriptionOptions options = new SubscriptionOptions();
    
    /**
     * 订阅属性（MQTT 5.0）
     */
    @Builder.Default
    private Map<String, Object> properties = new ConcurrentHashMap<>();
    
    /**
     * 订阅统计信息
     */
    @Builder.Default
    private SubscriptionStats stats = new SubscriptionStats();
    
    /**
     * 订阅选项
     */
    @Data
    @Builder
    @NoArgsConstructor
    @AllArgsConstructor
    public static class SubscriptionOptions {
        /**
         * 不接收本客户端发送的消息
         */
        @Builder.Default
        private boolean noLocal = false;
        
        /**
         * 保留消息的处理方式
         */
        @Builder.Default
        private RetainHandling retainHandling = RetainHandling.SEND_ON_SUBSCRIBE;
        
        /**
         * 保持发布时的Retain标志
         */
        @Builder.Default
        private boolean retainAsPublished = false;
        
        /**
         * 订阅标识符（MQTT 5.0）
         */
        private Integer subscriptionIdentifier;
    }
    
    /**
     * 保留消息处理方式
     */
    public enum RetainHandling {
        /**
         * 订阅时发送保留消息
         */
        SEND_ON_SUBSCRIBE(0),
        
        /**
         * 仅在新订阅时发送保留消息
         */
        SEND_ON_SUBSCRIBE_NEW(1),
        
        /**
         * 不发送保留消息
         */
        DO_NOT_SEND(2);
        
        private final int value;
        
        RetainHandling(int value) {
            this.value = value;
        }
        
        public int getValue() {
            return value;
        }
        
        public static RetainHandling fromValue(int value) {
            for (RetainHandling handling : values()) {
                if (handling.value == value) {
                    return handling;
                }
            }
            throw new IllegalArgumentException("Invalid retain handling value: " + value);
        }
    }
    
    /**
     * 订阅统计信息
     */
    @Data
    @Builder
    @NoArgsConstructor
    @AllArgsConstructor
    public static class SubscriptionStats {
        /**
         * 匹配消息数
         */
        @Builder.Default
        private long messagesMatched = 0L;
        
        /**
         * 传递消息数
         */
        @Builder.Default
        private long messagesDelivered = 0L;
        
        /**
         * 丢弃消息数
         */
        @Builder.Default
        private long messagesDropped = 0L;
        
        /**
         * 最大QoS传递数
         */
        @Builder.Default
        private long qos0Delivered = 0L;
        
        /**
         * QoS1传递数
         */
        @Builder.Default
        private long qos1Delivered = 0L;
        
        /**
         * QoS2传递数
         */
        @Builder.Default
        private long qos2Delivered = 0L;
    }
    
    /**
     * 检查主题是否匹配当前订阅
     *
     * @param topic 要检查的主题
     * @return 如果匹配返回true
     */
    public boolean matches(String topic) {
        return matchesTopicFilter(topic, this.topicFilter);
    }
    
    /**
     * 更新匹配统计
     */
    public void updateMatchStats() {
        stats.messagesMatched++;
        lastMatchedAt = LocalDateTime.now();
    }
    
    /**
     * 更新传递统计
     *
     * @param deliveredQos 传递使用的QoS等级
     */
    public void updateDeliveryStats(MqttQos deliveredQos) {
        stats.messagesDelivered++;
        
        switch (deliveredQos) {
            case AT_MOST_ONCE:
                stats.qos0Delivered++;
                break;
            case AT_LEAST_ONCE:
                stats.qos1Delivered++;
                break;
            case EXACTLY_ONCE:
                stats.qos2Delivered++;
                break;
        }
    }
    
    /**
     * 更新丢弃统计
     */
    public void updateDropStats() {
        stats.messagesDropped++;
    }
    
    /**
     * 获取有效的传递QoS等级
     * 取订阅QoS和发布QoS的较小值
     *
     * @param publishQos 发布QoS等级
     * @return 有效的传递QoS等级
     */
    public MqttQos getEffectiveQos(MqttQos publishQos) {
        return qos.min(publishQos);
    }
    
    /**
     * 检查是否应该接收消息（基于noLocal选项）
     *
     * @param publisherClientId 发布者客户端ID
     * @return 如果应该接收返回true
     */
    public boolean shouldReceive(String publisherClientId) {
        if (options.noLocal && clientId.equals(publisherClientId)) {
            return false;
        }
        return true;
    }
    
    /**
     * 检查是否为共享订阅
     *
     * @return 如果是共享订阅返回true
     */
    public boolean isSharedSubscription() {
        return topicFilter.startsWith("$share/");
    }
    
    /**
     * 获取共享订阅的组名
     *
     * @return 共享订阅组名，如果不是共享订阅返回null
     */
    public String getSharedSubscriptionGroup() {
        if (!isSharedSubscription()) {
            return null;
        }
        
        // $share/group/topic格式
        String[] parts = topicFilter.split("/", 3);
        if (parts.length >= 2) {
            return parts[1];
        }
        return null;
    }
    
    /**
     * 获取共享订阅的实际主题过滤器
     *
     * @return 实际主题过滤器
     */
    public String getActualTopicFilter() {
        if (!isSharedSubscription()) {
            return topicFilter;
        }
        
        // $share/group/topic格式，返回topic部分
        String[] parts = topicFilter.split("/", 3);
        if (parts.length >= 3) {
            return parts[2];
        }
        return topicFilter;
    }
    
    /**
     * 获取订阅存活时间（毫秒）
     *
     * @return 存活时间
     */
    public long getLifetimeMs() {
        return java.time.Duration.between(subscribedAt, LocalDateTime.now()).toMillis();
    }
    
    /**
     * MQTT主题过滤器匹配算法
     * TODO: 实现完整的MQTT主题匹配逻辑，包括+和#通配符
     *
     * @param topic 主题
     * @param filter 过滤器
     * @return 是否匹配
     */
    private boolean matchesTopicFilter(String topic, String filter) {
        // 如果过滤器是多级通配符，匹配所有主题
        if ("#".equals(filter)) {
            return true;
        }
        
        // 完全匹配
        if (topic.equals(filter)) {
            return true;
        }
        
        // 处理通配符匹配
        String[] topicLevels = topic.split("/");
        String[] filterLevels = filter.split("/");
        
        return matchLevels(topicLevels, filterLevels, 0, 0);
    }
    
    /**
     * 递归匹配主题级别
     *
     * @param topicLevels 主题级别数组
     * @param filterLevels 过滤器级别数组
     * @param topicIndex 主题索引
     * @param filterIndex 过滤器索引
     * @return 是否匹配
     */
    private boolean matchLevels(String[] topicLevels, String[] filterLevels, 
                               int topicIndex, int filterIndex) {
        // 如果过滤器索引到达末尾
        if (filterIndex >= filterLevels.length) {
            return topicIndex >= topicLevels.length;
        }
        
        // 如果当前过滤器级别是多级通配符
        if ("#".equals(filterLevels[filterIndex])) {
            // 多级通配符必须是最后一个级别
            return filterIndex == filterLevels.length - 1;
        }
        
        // 如果主题索引到达末尾
        if (topicIndex >= topicLevels.length) {
            return false;
        }
        
        // 如果当前过滤器级别是单级通配符或完全匹配
        if ("+".equals(filterLevels[filterIndex]) || 
            topicLevels[topicIndex].equals(filterLevels[filterIndex])) {
            return matchLevels(topicLevels, filterLevels, topicIndex + 1, filterIndex + 1);
        }
        
        return false;
    }
}