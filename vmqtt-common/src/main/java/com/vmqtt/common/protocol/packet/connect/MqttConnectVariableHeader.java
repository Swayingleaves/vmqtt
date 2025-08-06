/**
 * MQTT连接包可变头部
 *
 * @author zhenglin
 * @date 2025/08/06
 */
package com.vmqtt.common.protocol.packet.connect;

import com.vmqtt.common.protocol.MqttVersion;

/**
 * MQTT CONNECT包可变头部定义
 * 
 * 包含协议名、协议级别、连接标志和保活时间
 */
public record MqttConnectVariableHeader(
        MqttVersion mqttVersion,
        MqttConnectFlags connectFlags,
        int keepAlive,
        Object properties) { // MQTT 5.0属性，使用Object暂时占位
    
    /**
     * 构造函数验证
     */
    public MqttConnectVariableHeader {
        if (mqttVersion == null) {
            throw new IllegalArgumentException("MQTT version cannot be null");
        }
        
        if (connectFlags == null) {
            throw new IllegalArgumentException("Connect flags cannot be null");
        }
        
        if (keepAlive < 0 || keepAlive > 65535) {
            throw new IllegalArgumentException("Keep alive must be between 0 and 65535");
        }
        
        // MQTT 5.0特定验证
        if (mqttVersion.isMqtt5()) {
            // 在MQTT 5.0中，properties可以为null表示没有属性
        } else {
            // 在MQTT 3.x中，不应该有properties
            if (properties != null) {
                throw new IllegalArgumentException("Properties are not supported in MQTT " + mqttVersion.getVersion());
            }
        }
    }
    
    /**
     * 创建MQTT 3.x的可变头部
     *
     * @param mqttVersion MQTT版本
     * @param connectFlags 连接标志
     * @param keepAlive 保活时间
     * @return MQTT连接可变头部
     */
    public static MqttConnectVariableHeader create3x(MqttVersion mqttVersion, 
                                                     MqttConnectFlags connectFlags, 
                                                     int keepAlive) {
        return new MqttConnectVariableHeader(mqttVersion, connectFlags, keepAlive, null);
    }
    
    /**
     * 创建MQTT 5.0的可变头部
     *
     * @param connectFlags 连接标志
     * @param keepAlive 保活时间
     * @param properties 属性
     * @return MQTT连接可变头部
     */
    public static MqttConnectVariableHeader create5(MqttConnectFlags connectFlags, 
                                                    int keepAlive, 
                                                    Object properties) {
        return new MqttConnectVariableHeader(MqttVersion.MQTT_5_0, connectFlags, keepAlive, properties);
    }
    
    /**
     * 获取协议名
     *
     * @return 协议名
     */
    public String getProtocolName() {
        return mqttVersion.getProtocolName();
    }
    
    /**
     * 获取协议级别
     *
     * @return 协议级别
     */
    public byte getProtocolLevel() {
        return mqttVersion.getLevel();
    }
    
    /**
     * 检查是否支持属性
     *
     * @return 如果支持属性返回true
     */
    public boolean supportsProperties() {
        return mqttVersion.supportsProperties();
    }
    
    /**
     * 检查是否有属性
     *
     * @return 如果有属性返回true
     */
    public boolean hasProperties() {
        return properties != null;
    }
    
    @Override
    public String toString() {
        return String.format("MqttConnectVariableHeader{version=%s, flags=%s, keepAlive=%d, hasProperties=%s}",
                           mqttVersion, connectFlags, keepAlive, hasProperties());
    }
}