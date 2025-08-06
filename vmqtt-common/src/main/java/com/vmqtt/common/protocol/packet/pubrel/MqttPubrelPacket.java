/**
 * MQTT发布释放包
 *
 * @author zhenglin
 * @date 2025/08/06
 */
package com.vmqtt.common.protocol.packet.pubrel;

import com.vmqtt.common.protocol.packet.MqttFixedHeader;
import com.vmqtt.common.protocol.packet.MqttPacket;
import com.vmqtt.common.protocol.packet.MqttPacketType;
import com.vmqtt.common.protocol.packet.MqttPacketWithId;

/**
 * MQTT PUBREL包定义
 * 
 * QoS 2发布释放包（第二步）
 */
public record MqttPubrelPacket(
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
    public MqttPubrelPacket {
        if (fixedHeader.packetType() != MqttPacketType.PUBREL) {
            throw new IllegalArgumentException("Invalid packet type for PUBREL packet");
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
     * 创建PUBREL包
     *
     * @param packetId 包标识符
     * @return PUBREL包
     */
    public static MqttPubrelPacket create(int packetId) {
        return create(packetId, null);
    }
    
    /**
     * 创建MQTT 5.0的PUBREL包
     *
     * @param packetId 包标识符
     * @param properties 属性
     * @return PUBREL包
     */
    public static MqttPubrelPacket create(int packetId, Object properties) {
        // 计算剩余长度
        int remainingLength = 2; // 包标识符
        
        if (properties != null) {
            // MQTT 5.0属性长度计算
            remainingLength += 1; // 暂时占位
        }
        
        // PUBREL包的固定头部QoS必须为1
        MqttFixedHeader fixedHeader = MqttFixedHeader.createPubrelHeader(remainingLength);
        return new MqttPubrelPacket(fixedHeader, packetId, properties);
    }
    
    @Override
    public String toString() {
        return String.format("MqttPubrelPacket{packetId=%d, hasProperties=%s}", 
                           packetId, hasProperties());
    }
}