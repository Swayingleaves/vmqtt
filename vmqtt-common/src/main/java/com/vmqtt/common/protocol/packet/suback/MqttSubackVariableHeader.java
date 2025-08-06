/**
 * MQTT订阅确认包可变头部
 *
 * @author zhenglin
 * @date 2025/08/06
 */
package com.vmqtt.common.protocol.packet.suback;

/**
 * MQTT SUBACK包可变头部定义
 * 
 * 包含包标识符和属性（MQTT 5.0）
 */
public record MqttSubackVariableHeader(
        int packetId,
        Object properties) { // MQTT 5.0属性，使用Object暂时占位
    
    /**
     * 构造函数验证
     */
    public MqttSubackVariableHeader {
        if (packetId < 1 || packetId > 65535) {
            throw new IllegalArgumentException("Packet ID must be between 1 and 65535");
        }
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
     * 创建MQTT 3.x的可变头部
     *
     * @param packetId 包标识符
     * @return SUBACK可变头部
     */
    public static MqttSubackVariableHeader create3x(int packetId) {
        return new MqttSubackVariableHeader(packetId, null);
    }
    
    /**
     * 创建MQTT 5.0的可变头部
     *
     * @param packetId 包标识符
     * @param properties 属性
     * @return SUBACK可变头部
     */
    public static MqttSubackVariableHeader create5(int packetId, Object properties) {
        return new MqttSubackVariableHeader(packetId, properties);
    }
    
    @Override
    public String toString() {
        return String.format("MqttSubackVariableHeader{packetId=%d, hasProperties=%s}",
                           packetId, hasProperties());
    }
}