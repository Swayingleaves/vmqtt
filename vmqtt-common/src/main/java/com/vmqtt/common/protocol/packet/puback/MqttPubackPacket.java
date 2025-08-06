/**
 * MQTT发布确认包
 *
 * @author zhenglin
 * @date 2025/08/06
 */
package com.vmqtt.common.protocol.packet.puback;

import com.vmqtt.common.protocol.packet.MqttFixedHeader;
import com.vmqtt.common.protocol.packet.MqttPacket;
import com.vmqtt.common.protocol.packet.MqttPacketType;
import com.vmqtt.common.protocol.packet.MqttPacketWithId;

/**
 * MQTT PUBACK包定义
 * 
 * QoS 1发布确认包
 */
public record MqttPubackPacket(
        MqttFixedHeader fixedHeader,
        int packetId,
        Object properties) implements MqttPacket, MqttPacketWithId { // MQTT 5.0属性，使用Object暂时占位
    
    @Override
    public MqttFixedHeader getFixedHeader() {
        return fixedHeader;
    }
    
    /**
     * 构造函数验证
     */
    public MqttPubackPacket {
        if (fixedHeader.packetType() != MqttPacketType.PUBACK) {
            throw new IllegalArgumentException("Invalid packet type for PUBACK packet");
        }
        
        MqttPacketWithId.validatePacketId(packetId);
    }
    
    @Override
    public int getPacketId() {
        return packetId;
    }
    
    /**
     * 检查是否有属性
     *
     * @return 如果有属性返回true
     */
    public boolean hasProperties() {
        return properties != null;
    }
    
    /**
     * 创建PUBACK包
     *
     * @param packetId 包标识符
     * @return PUBACK包
     */
    public static MqttPubackPacket create(int packetId) {
        return create(packetId, null);
    }
    
    /**
     * 创建MQTT 5.0的PUBACK包
     *
     * @param packetId 包标识符
     * @param properties 属性
     * @return PUBACK包
     */
    public static MqttPubackPacket create(int packetId, Object properties) {
        // 计算剩余长度
        int remainingLength = 2; // 包标识符
        
        if (properties != null) {
            // MQTT 5.0属性长度计算
            remainingLength += 1; // 暂时占位
        }
        
        MqttFixedHeader fixedHeader = new MqttFixedHeader(MqttPacketType.PUBACK, remainingLength);
        return new MqttPubackPacket(fixedHeader, packetId, properties);
    }
    
    @Override
    public String toString() {
        return String.format("MqttPubackPacket{packetId=%d, hasProperties=%s}", 
                           packetId, hasProperties());
    }
}