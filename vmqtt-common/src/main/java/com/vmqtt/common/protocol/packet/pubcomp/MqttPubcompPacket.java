/**
 * MQTT发布完成包
 *
 * @author zhenglin
 * @date 2025/08/06
 */
package com.vmqtt.common.protocol.packet.pubcomp;

import com.vmqtt.common.protocol.packet.MqttFixedHeader;
import com.vmqtt.common.protocol.packet.MqttPacket;
import com.vmqtt.common.protocol.packet.MqttPacketType;
import com.vmqtt.common.protocol.packet.MqttPacketWithId;

/**
 * MQTT PUBCOMP包定义
 * 
 * QoS 2发布完成包（第三步）
 */
public record MqttPubcompPacket(
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
    public MqttPubcompPacket {
        if (fixedHeader.packetType() != MqttPacketType.PUBCOMP) {
            throw new IllegalArgumentException("Invalid packet type for PUBCOMP packet");
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
     * 创建PUBCOMP包
     *
     * @param packetId 包标识符
     * @return PUBCOMP包
     */
    public static MqttPubcompPacket create(int packetId) {
        return create(packetId, null);
    }
    
    /**
     * 创建MQTT 5.0的PUBCOMP包
     *
     * @param packetId 包标识符
     * @param properties 属性
     * @return PUBCOMP包
     */
    public static MqttPubcompPacket create(int packetId, Object properties) {
        // 计算剩余长度
        int remainingLength = 2; // 包标识符
        
        if (properties != null) {
            // MQTT 5.0属性长度计算
            remainingLength += 1; // 暂时占位
        }
        
        MqttFixedHeader fixedHeader = new MqttFixedHeader(MqttPacketType.PUBCOMP, remainingLength);
        return new MqttPubcompPacket(fixedHeader, packetId, properties);
    }
    
    @Override
    public String toString() {
        return String.format("MqttPubcompPacket{packetId=%d, hasProperties=%s}", 
                           packetId, hasProperties());
    }
}