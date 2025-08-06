/**
 * MQTT连接确认包
 *
 * @author zhenglin
 * @date 2025/08/06
 */
package com.vmqtt.common.protocol.packet.connack;

import com.vmqtt.common.protocol.packet.MqttFixedHeader;
import com.vmqtt.common.protocol.packet.MqttPacket;
import com.vmqtt.common.protocol.packet.MqttPacketType;

/**
 * MQTT CONNACK包定义
 * 
 * 服务器响应客户端CONNECT请求的确认包
 */
public record MqttConnackPacket(
        MqttFixedHeader fixedHeader,
        MqttConnackVariableHeader variableHeader) implements MqttPacket {
    
    @Override
    public MqttFixedHeader getFixedHeader() {
        return fixedHeader;
    }
    
    /**
     * 构造函数验证
     */
    public MqttConnackPacket {
        if (fixedHeader.packetType() != MqttPacketType.CONNACK) {
            throw new IllegalArgumentException("Invalid packet type for CONNACK packet");
        }
        
        if (variableHeader == null) {
            throw new IllegalArgumentException("Variable header cannot be null for CONNACK packet");
        }
    }
    
    /**
     * 获取会话存在标志
     *
     * @return 如果会话存在返回true
     */
    public boolean isSessionPresent() {
        return variableHeader.sessionPresent();
    }
    
    /**
     * 获取连接返回码
     *
     * @return 连接返回码
     */
    public MqttConnectReturnCode getReturnCode() {
        return variableHeader.returnCode();
    }
    
    /**
     * 检查连接是否成功
     *
     * @return 如果连接成功返回true
     */
    public boolean isConnectionAccepted() {
        return variableHeader.returnCode() == MqttConnectReturnCode.CONNECTION_ACCEPTED;
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
     * @return 属性对象，如果没有属性返回null
     */
    public Object getProperties() {
        return variableHeader.properties();
    }
    
    /**
     * 创建成功的CONNACK包
     *
     * @param sessionPresent 会话存在标志
     * @return 成功的CONNACK包
     */
    public static MqttConnackPacket createSuccess(boolean sessionPresent) {
        return create(sessionPresent, MqttConnectReturnCode.CONNECTION_ACCEPTED, null);
    }
    
    /**
     * 创建失败的CONNACK包
     *
     * @param returnCode 错误返回码
     * @return 失败的CONNACK包
     */
    public static MqttConnackPacket createFailure(MqttConnectReturnCode returnCode) {
        if (returnCode == MqttConnectReturnCode.CONNECTION_ACCEPTED) {
            throw new IllegalArgumentException("Cannot create failure CONNACK with success return code");
        }
        return create(false, returnCode, null);
    }
    
    /**
     * 创建MQTT 5.0的CONNACK包
     *
     * @param sessionPresent 会话存在标志
     * @param returnCode 返回码
     * @param properties 属性
     * @return MQTT 5.0 CONNACK包
     */
    public static MqttConnackPacket createMqtt5(boolean sessionPresent, 
                                               MqttConnectReturnCode returnCode, 
                                               Object properties) {
        return create(sessionPresent, returnCode, properties);
    }
    
    /**
     * 创建CONNACK包
     *
     * @param sessionPresent 会话存在标志
     * @param returnCode 返回码
     * @param properties 属性（可以为null）
     * @return CONNACK包
     */
    private static MqttConnackPacket create(boolean sessionPresent, 
                                          MqttConnectReturnCode returnCode, 
                                          Object properties) {
        // 计算剩余长度
        int remainingLength = 2; // 连接确认标志(1) + 连接返回码(1)
        
        if (properties != null) {
            // MQTT 5.0属性长度计算，这里暂时使用占位符
            remainingLength += 1; // 属性长度占位
        }
        
        MqttFixedHeader fixedHeader = new MqttFixedHeader(MqttPacketType.CONNACK, remainingLength);
        MqttConnackVariableHeader variableHeader = new MqttConnackVariableHeader(
            sessionPresent, returnCode, properties
        );
        
        return new MqttConnackPacket(fixedHeader, variableHeader);
    }
    
    @Override
    public String toString() {
        return String.format("MqttConnackPacket{sessionPresent=%s, returnCode=%s, hasProperties=%s}",
                           isSessionPresent(), getReturnCode(), hasProperties());
    }
}