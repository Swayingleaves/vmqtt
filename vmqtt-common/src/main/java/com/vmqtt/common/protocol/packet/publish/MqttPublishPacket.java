/**
 * MQTT发布包
 *
 * @author zhenglin
 * @date 2025/08/06
 */
package com.vmqtt.common.protocol.packet.publish;

import com.vmqtt.common.protocol.MqttQos;
import com.vmqtt.common.protocol.packet.MqttFixedHeader;
import com.vmqtt.common.protocol.packet.MqttPacket;
import com.vmqtt.common.protocol.packet.MqttPacketType;
import com.vmqtt.common.protocol.packet.MqttPacketWithId;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;

/**
 * MQTT PUBLISH包定义
 * 
 * 用于传输应用消息的包
 */
public record MqttPublishPacket(
        MqttFixedHeader fixedHeader,
        MqttPublishVariableHeader variableHeader,
        byte[] payload) implements MqttPacket, MqttPacketWithId {
    
    @Override
    public MqttFixedHeader getFixedHeader() {
        return fixedHeader;
    }
    
    /**
     * 构造函数验证
     */
    public MqttPublishPacket {
        if (fixedHeader.packetType() != MqttPacketType.PUBLISH) {
            throw new IllegalArgumentException("Invalid packet type for PUBLISH packet");
        }
        
        if (variableHeader == null) {
            throw new IllegalArgumentException("Variable header cannot be null for PUBLISH packet");
        }
        
        // QoS > 0时必须有包标识符
        if (fixedHeader.qos().getValue() > 0) {
            if (variableHeader.packetId() == 0) {
                throw new IllegalArgumentException("Packet ID is required for QoS > 0 PUBLISH packets");
            }
            MqttPacketWithId.validatePacketId(variableHeader.packetId());
        } else {
            // QoS 0时不应该有包标识符
            if (variableHeader.packetId() != 0) {
                throw new IllegalArgumentException("Packet ID must be 0 for QoS 0 PUBLISH packets");
            }
        }
        
        // 验证主题名
        if (variableHeader.topicName() == null || variableHeader.topicName().isEmpty()) {
            throw new IllegalArgumentException("Topic name cannot be null or empty for PUBLISH packet");
        }
        
        // 复制负载以保证不可变性
        if (payload != null) {
            payload = payload.clone();
        }
    }
    
    @Override
    public int getPacketId() {
        return variableHeader.packetId();
    }
    
    /**
     * 获取主题名
     *
     * @return 主题名
     */
    public String getTopicName() {
        return variableHeader.topicName();
    }
    
    /**
     * 获取QoS等级
     *
     * @return QoS等级
     */
    public MqttQos getQos() {
        return fixedHeader.qos();
    }
    
    /**
     * 获取DUP标志
     *
     * @return DUP标志
     */
    public boolean isDup() {
        return fixedHeader.dupFlag();
    }
    
    /**
     * 获取RETAIN标志
     *
     * @return RETAIN标志
     */
    public boolean isRetain() {
        return fixedHeader.retainFlag();
    }
    
    /**
     * 获取负载的副本
     *
     * @return 负载副本
     */
    public byte[] getPayloadCopy() {
        return payload != null ? payload.clone() : null;
    }
    
    /**
     * 获取负载的字符串表示
     *
     * @return 负载字符串
     */
    public String getPayloadAsString() {
        return payload != null ? new String(payload, StandardCharsets.UTF_8) : null;
    }
    
    /**
     * 获取负载长度
     *
     * @return 负载长度
     */
    public int getPayloadLength() {
        return payload != null ? payload.length : 0;
    }
    
    /**
     * 检查是否有负载
     *
     * @return 如果有负载返回true
     */
    public boolean hasPayload() {
        return payload != null && payload.length > 0;
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
     * 创建PUBLISH包构建器
     *
     * @param topicName 主题名
     * @param qos QoS等级
     * @return PUBLISH包构建器
     */
    public static Builder builder(String topicName, MqttQos qos) {
        return new Builder(topicName, qos);
    }
    
    /**
     * 创建QoS 0的PUBLISH包
     *
     * @param topicName 主题名
     * @param payload 负载
     * @param retain 保留标志
     * @return PUBLISH包
     */
    public static MqttPublishPacket createQos0(String topicName, byte[] payload, boolean retain) {
        return builder(topicName, MqttQos.AT_MOST_ONCE)
            .payload(payload)
            .retain(retain)
            .build();
    }
    
    /**
     * 创建QoS 1的PUBLISH包
     *
     * @param topicName 主题名
     * @param payload 负载
     * @param packetId 包标识符
     * @param retain 保留标志
     * @return PUBLISH包
     */
    public static MqttPublishPacket createQos1(String topicName, byte[] payload, 
                                              int packetId, boolean retain) {
        return builder(topicName, MqttQos.AT_LEAST_ONCE)
            .payload(payload)
            .packetId(packetId)
            .retain(retain)
            .build();
    }
    
    /**
     * 创建QoS 2的PUBLISH包
     *
     * @param topicName 主题名
     * @param payload 负载
     * @param packetId 包标识符
     * @param retain 保留标志
     * @return PUBLISH包
     */
    public static MqttPublishPacket createQos2(String topicName, byte[] payload, 
                                              int packetId, boolean retain) {
        return builder(topicName, MqttQos.EXACTLY_ONCE)
            .payload(payload)
            .packetId(packetId)
            .retain(retain)
            .build();
    }
    
    /**
     * PUBLISH包构建器
     */
    public static class Builder {
        private final String topicName;
        private final MqttQos qos;
        private boolean dupFlag = false;
        private boolean retain = false;
        private int packetId = 0;
        private byte[] payload;
        private Object properties; // MQTT 5.0属性
        
        private Builder(String topicName, MqttQos qos) {
            this.topicName = topicName;
            this.qos = qos;
        }
        
        public Builder dup(boolean dup) {
            this.dupFlag = dup;
            return this;
        }
        
        public Builder retain(boolean retain) {
            this.retain = retain;
            return this;
        }
        
        public Builder packetId(int packetId) {
            this.packetId = packetId;
            return this;
        }
        
        public Builder payload(byte[] payload) {
            this.payload = payload;
            return this;
        }
        
        public Builder payload(String payload) {
            this.payload = payload != null ? payload.getBytes(StandardCharsets.UTF_8) : null;
            return this;
        }
        
        public Builder properties(Object properties) {
            this.properties = properties;
            return this;
        }
        
        public MqttPublishPacket build() {
            // 验证包标识符
            if (qos.getValue() > 0) {
                if (packetId == 0) {
                    throw new IllegalArgumentException("Packet ID is required for QoS > 0");
                }
                MqttPacketWithId.validatePacketId(packetId);
            }
            
            // 计算剩余长度
            int remainingLength = calculateRemainingLength();
            
            // 创建固定头部
            MqttFixedHeader fixedHeader = MqttFixedHeader.createPublishHeader(
                dupFlag, qos, retain, remainingLength
            );
            
            // 创建可变头部
            MqttPublishVariableHeader variableHeader = new MqttPublishVariableHeader(
                topicName, packetId, properties
            );
            
            return new MqttPublishPacket(fixedHeader, variableHeader, payload);
        }
        
        private int calculateRemainingLength() {
            int length = 0;
            
            // 主题名长度
            length += 2 + topicName.getBytes(StandardCharsets.UTF_8).length;
            
            // 包标识符（仅QoS > 0）
            if (qos.getValue() > 0) {
                length += 2;
            }
            
            // MQTT 5.0属性长度
            if (properties != null) {
                // 这里需要根据实际属性实现计算长度
                length += 1; // 暂时占位
            }
            
            // 负载长度
            if (payload != null) {
                length += payload.length;
            }
            
            return length;
        }
    }
    
    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null || getClass() != obj.getClass()) return false;
        
        MqttPublishPacket that = (MqttPublishPacket) obj;
        return fixedHeader.equals(that.fixedHeader) &&
               variableHeader.equals(that.variableHeader) &&
               Arrays.equals(payload, that.payload);
    }
    
    @Override
    public int hashCode() {
        int result = fixedHeader.hashCode();
        result = 31 * result + variableHeader.hashCode();
        result = 31 * result + Arrays.hashCode(payload);
        return result;
    }
    
    @Override
    public String toString() {
        return String.format("MqttPublishPacket{topic='%s', qos=%s, dup=%s, retain=%s, packetId=%d, payloadLength=%d}",
                           getTopicName(), getQos(), isDup(), isRetain(), getPacketId(), getPayloadLength());
    }
}