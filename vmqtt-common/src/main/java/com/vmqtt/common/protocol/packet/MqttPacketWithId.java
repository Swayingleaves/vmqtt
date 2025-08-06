/**
 * 包含包标识符的MQTT数据包接口
 *
 * @author zhenglin
 * @date 2025/08/06
 */
package com.vmqtt.common.protocol.packet;

/**
 * 包含包标识符的MQTT控制包接口
 * 
 * 用于需要包标识符的MQTT控制包，如PUBLISH(QoS>0)、PUBACK、PUBREC、PUBREL、PUBCOMP、
 * SUBSCRIBE、SUBACK、UNSUBSCRIBE、UNSUBACK
 */
public interface MqttPacketWithId extends MqttPacket {
    
    /**
     * 获取包标识符
     *
     * @return 包标识符（1-65535）
     */
    int getPacketId();
    
    /**
     * 验证包标识符
     *
     * @param packetId 包标识符
     * @throws IllegalArgumentException 如果包标识符无效
     */
    static void validatePacketId(int packetId) {
        if (packetId < 1 || packetId > 65535) {
            throw new IllegalArgumentException("Packet ID must be between 1 and 65535, got: " + packetId);
        }
    }
    
    /**
     * 检查包标识符是否有效
     *
     * @param packetId 包标识符
     * @return 如果有效返回true
     */
    static boolean isValidPacketId(int packetId) {
        return packetId >= 1 && packetId <= 65535;
    }
}