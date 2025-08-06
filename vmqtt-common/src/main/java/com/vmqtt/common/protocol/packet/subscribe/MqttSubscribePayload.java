/**
 * MQTT订阅包负载
 *
 * @author zhenglin
 * @date 2025/08/06
 */
package com.vmqtt.common.protocol.packet.subscribe;

import java.util.List;
import java.util.ArrayList;
import java.util.Collections;

/**
 * MQTT SUBSCRIBE包负载定义
 * 
 * 包含一个或多个主题过滤器和对应的QoS等级
 */
public record MqttSubscribePayload(List<MqttTopicSubscription> subscriptions) {
    
    /**
     * 构造函数验证
     */
    public MqttSubscribePayload {
        if (subscriptions == null) {
            throw new IllegalArgumentException("Subscriptions cannot be null");
        }
        
        if (subscriptions.isEmpty()) {
            throw new IllegalArgumentException("Subscriptions cannot be empty");
        }
        
        // 创建不可变副本
        subscriptions = Collections.unmodifiableList(new ArrayList<>(subscriptions));
        
        // 验证所有订阅
        for (MqttTopicSubscription subscription : subscriptions) {
            if (subscription == null) {
                throw new IllegalArgumentException("Subscription cannot be null");
            }
        }
    }
    
    /**
     * 获取订阅数量
     *
     * @return 订阅数量
     */
    public int getSubscriptionCount() {
        return subscriptions.size();
    }
    
    /**
     * 获取指定索引的订阅
     *
     * @param index 索引
     * @return 主题订阅
     */
    public MqttTopicSubscription getSubscription(int index) {
        return subscriptions.get(index);
    }
    
    /**
     * 检查是否包含指定主题过滤器
     *
     * @param topicFilter 主题过滤器
     * @return 如果包含返回true
     */
    public boolean containsTopicFilter(String topicFilter) {
        return subscriptions.stream()
                .anyMatch(subscription -> subscription.topicFilter().equals(topicFilter));
    }
    
    /**
     * 获取所有主题过滤器
     *
     * @return 主题过滤器列表
     */
    public List<String> getTopicFilters() {
        return subscriptions.stream()
                .map(MqttTopicSubscription::topicFilter)
                .toList();
    }
    
    /**
     * 创建单个订阅的负载
     *
     * @param topicFilter 主题过滤器
     * @param qos QoS等级
     * @return 订阅负载
     */
    public static MqttSubscribePayload single(String topicFilter, int qos) {
        return new MqttSubscribePayload(List.of(new MqttTopicSubscription(topicFilter, qos)));
    }
    
    /**
     * 创建多个订阅的负载
     *
     * @param subscriptions 订阅数组
     * @return 订阅负载
     */
    public static MqttSubscribePayload of(MqttTopicSubscription... subscriptions) {
        return new MqttSubscribePayload(List.of(subscriptions));
    }
    
    @Override
    public String toString() {
        return String.format("MqttSubscribePayload{subscriptionsCount=%d, topics=%s}",
                           getSubscriptionCount(), getTopicFilters());
    }
}