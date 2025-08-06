/**
 * MQTT数据包基础接口
 *
 * @author zhenglin
 * @date 2025/08/06
 */
package com.vmqtt.common.protocol.packet;

/**
 * MQTT控制包基础接口
 * 
 * 所有MQTT控制包的基础接口，定义了基本的包结构
 */
public interface MqttPacket {
    
    /**
     * 获取固定头部
     *
     * @return MQTT固定头部
     */
    MqttFixedHeader getFixedHeader();
    
    /**
     * 获取包类型
     *
     * @return MQTT包类型
     */
    default MqttPacketType getPacketType() {
        return getFixedHeader().packetType();
    }
    
    /**
     * 获取剩余长度
     *
     * @return 剩余长度
     */
    default int getRemainingLength() {
        return getFixedHeader().remainingLength();
    }
    
    /**
     * 检查是否为控制包
     *
     * @return 如果是控制包返回true
     */
    default boolean isControlPacket() {
        return getPacketType() != MqttPacketType.PUBLISH;
    }
    
    /**
     * 检查是否为发布包
     *
     * @return 如果是发布包返回true
     */
    default boolean isPublishPacket() {
        return getPacketType() == MqttPacketType.PUBLISH;
    }
    
    /**
     * 检查是否包含包标识符
     *
     * @return 如果包含包标识符返回true
     */
    default boolean hasPacketId() {
        return getPacketType().hasPacketId();
    }
    
    /**
     * 获取包标识符（如果存在）
     *
     * @return 包标识符，如果不存在返回0
     */
    default int getPacketId() {
        if (this instanceof MqttPacketWithId packetWithId) {
            return packetWithId.getPacketId();
        }
        return 0;
    }
}