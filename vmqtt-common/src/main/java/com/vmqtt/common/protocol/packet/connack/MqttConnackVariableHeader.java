/**
 * MQTT连接确认包可变头部
 *
 * @author zhenglin
 * @date 2025/08/06
 */
package com.vmqtt.common.protocol.packet.connack;

/**
 * MQTT CONNACK包可变头部定义
 * 
 * 包含连接确认标志、连接返回码和属性（MQTT 5.0）
 */
public record MqttConnackVariableHeader(
        boolean sessionPresent,
        MqttConnectReturnCode returnCode,
        Object properties) { // MQTT 5.0属性，使用Object暂时占位
    
    /**
     * 构造函数验证
     */
    public MqttConnackVariableHeader {
        if (returnCode == null) {
            throw new IllegalArgumentException("Return code cannot be null");
        }
        
        // 如果连接被拒绝，会话存在标志必须为false
        if (returnCode != MqttConnectReturnCode.CONNECTION_ACCEPTED && sessionPresent) {
            throw new IllegalArgumentException("Session present flag must be false when connection is rejected");
        }
    }
    
    /**
     * 获取连接确认标志字节
     *
     * @return 连接确认标志字节
     */
    public byte getAcknowledgeFlags() {
        return sessionPresent ? (byte) 0x01 : (byte) 0x00;
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
     * 从字节创建连接确认标志
     *
     * @param flags 标志字节
     * @return 会话存在标志
     * @throws IllegalArgumentException 如果保留位不为0
     */
    public static boolean parseAcknowledgeFlags(byte flags) {
        // 检查保留位（bit 7-1）必须为0
        if ((flags & 0xFE) != 0) {
            throw new IllegalArgumentException("Reserved bits in acknowledge flags must be 0");
        }
        
        return (flags & 0x01) != 0;
    }
    
    /**
     * 创建MQTT 3.x的可变头部
     *
     * @param sessionPresent 会话存在标志
     * @param returnCode 返回码
     * @return CONNACK可变头部
     */
    public static MqttConnackVariableHeader create3x(boolean sessionPresent, 
                                                     MqttConnectReturnCode returnCode) {
        return new MqttConnackVariableHeader(sessionPresent, returnCode, null);
    }
    
    /**
     * 创建MQTT 5.0的可变头部
     *
     * @param sessionPresent 会话存在标志
     * @param returnCode 返回码
     * @param properties 属性
     * @return CONNACK可变头部
     */
    public static MqttConnackVariableHeader create5(boolean sessionPresent, 
                                                    MqttConnectReturnCode returnCode, 
                                                    Object properties) {
        return new MqttConnackVariableHeader(sessionPresent, returnCode, properties);
    }
    
    @Override
    public String toString() {
        return String.format("MqttConnackVariableHeader{sessionPresent=%s, returnCode=%s, hasProperties=%s}",
                           sessionPresent, returnCode, hasProperties());
    }
}