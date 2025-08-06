/**
 * MQTT订阅包
 *
 * @author zhenglin
 * @date 2025/08/06
 */
package com.vmqtt.common.protocol.packet.subscribe;

import com.vmqtt.common.protocol.packet.MqttFixedHeader;
import com.vmqtt.common.protocol.packet.MqttPacket;
import com.vmqtt.common.protocol.packet.MqttPacketType;
import com.vmqtt.common.protocol.packet.MqttPacketWithId;

import java.util.List;

/**
 * MQTT SUBSCRIBE包定义
 * 
 * 客户端订阅一个或多个主题过滤器
 */
public record MqttSubscribePacket(
        MqttFixedHeader fixedHeader,
        MqttSubscribeVariableHeader variableHeader,
        MqttSubscribePayload payload) implements MqttPacket, MqttPacketWithId {
    
    @Override
    public MqttFixedHeader getFixedHeader() {
        return fixedHeader;
    }
    
    /**
     * 构造函数验证
     */
    public MqttSubscribePacket {
        if (fixedHeader.packetType() != MqttPacketType.SUBSCRIBE) {
            throw new IllegalArgumentException("Invalid packet type for SUBSCRIBE packet");
        }
        
        if (variableHeader == null) {
            throw new IllegalArgumentException("Variable header cannot be null for SUBSCRIBE packet");
        }
        
        if (payload == null) {
            throw new IllegalArgumentException("Payload cannot be null for SUBSCRIBE packet");
        }
        
        // SUBSCRIBE包必须有包标识符
        MqttPacketWithId.validatePacketId(variableHeader.packetId());
        
        // SUBSCRIBE包负载不能为空
        if (payload.subscriptions().isEmpty()) {
            throw new IllegalArgumentException("SUBSCRIBE packet must contain at least one subscription");
        }
    }
    
    @Override
    public int getPacketId() {
        return variableHeader.packetId();
    }
    
    /**
     * 获取订阅列表
     *
     * @return 订阅列表
     */
    public List<MqttTopicSubscription> getSubscriptions() {
        return payload.subscriptions();
    }
    
    /**
     * 检查是否有属性（MQTT 5.0）
     *
     * @return 如果有属性返回true
     */
    public boolean hasProperties() {
        return variableHeader.hasProperties();
    }
    
    /**
     * 获取属性（MQTT 5.0）
     *
     * @return 属性对象
     */
    public Object getProperties() {
        return variableHeader.properties();
    }
    
    /**
     * 创建SUBSCRIBE包构建器
     *
     * @param packetId 包标识符
     * @return SUBSCRIBE包构建器
     */
    public static Builder builder(int packetId) {
        return new Builder(packetId);
    }
    
    /**
     * 创建简单的SUBSCRIBE包
     *
     * @param packetId 包标识符
     * @param subscriptions 订阅列表
     * @return SUBSCRIBE包
     */
    public static MqttSubscribePacket create(int packetId, List<MqttTopicSubscription> subscriptions) {
        return builder(packetId).subscriptions(subscriptions).build();
    }
    
    /**
     * SUBSCRIBE包构建器
     */
    public static class Builder {
        private final int packetId;
        private List<MqttTopicSubscription> subscriptions;
        private Object properties; // MQTT 5.0属性
        
        private Builder(int packetId) {
            this.packetId = packetId;
        }
        
        public Builder subscriptions(List<MqttTopicSubscription> subscriptions) {
            this.subscriptions = subscriptions;
            return this;
        }
        
        public Builder subscription(String topicFilter, int qos) {
            return subscription(new MqttTopicSubscription(topicFilter, qos));
        }
        
        public Builder subscription(MqttTopicSubscription subscription) {
            if (subscriptions == null) {
                subscriptions = new java.util.ArrayList<>();
            }
            subscriptions.add(subscription);
            return this;
        }
        
        public Builder properties(Object properties) {
            this.properties = properties;
            return this;
        }
        
        public MqttSubscribePacket build() {
            if (subscriptions == null || subscriptions.isEmpty()) {
                throw new IllegalArgumentException("SUBSCRIBE packet must contain at least one subscription");
            }
            
            // 计算剩余长度
            int remainingLength = calculateRemainingLength();
            
            // 创建固定头部（SUBSCRIBE包固定QoS=1）
            MqttFixedHeader fixedHeader = MqttFixedHeader.createSubscribeHeader(remainingLength);
            
            // 创建可变头部
            MqttSubscribeVariableHeader variableHeader = new MqttSubscribeVariableHeader(
                packetId, properties
            );
            
            // 创建负载
            MqttSubscribePayload payload = new MqttSubscribePayload(subscriptions);
            
            return new MqttSubscribePacket(fixedHeader, variableHeader, payload);
        }
        
        private int calculateRemainingLength() {
            int length = 2; // 包标识符
            
            // MQTT 5.0属性长度
            if (properties != null) {
                // 这里需要根据实际属性实现计算长度
                length += 1; // 暂时占位
            }
            
            // 订阅负载长度
            for (MqttTopicSubscription subscription : subscriptions) {
                length += 2 + subscription.topicFilter().getBytes().length; // 主题过滤器长度
                length += 1; // QoS字节
            }
            
            return length;
        }
    }
    
    @Override
    public String toString() {
        return String.format("MqttSubscribePacket{packetId=%d, subscriptionsCount=%d, hasProperties=%s}",
                           getPacketId(), getSubscriptions().size(), hasProperties());
    }
}