/**
 * MQTT协议版本定义
 *
 * @author zhenglin
 * @date 2025/08/06
 */
package com.vmqtt.common.protocol;

/**
 * MQTT协议版本枚举
 */
public enum MqttVersion {
    /**
     * MQTT 3.1协议
     */
    MQTT_3_1((byte) 0x03, "MQIsdp", "3.1"),
    
    /**
     * MQTT 3.1.1协议
     */
    MQTT_3_1_1((byte) 0x04, "MQTT", "3.1.1"),
    
    /**
     * MQTT 5.0协议
     */
    MQTT_5_0((byte) 0x05, "MQTT", "5.0");
    
    /**
     * 协议级别
     */
    private final byte level;
    
    /**
     * 协议名称
     */
    private final String protocolName;
    
    /**
     * 协议版本字符串
     */
    private final String version;
    
    /**
     * 构造函数
     *
     * @param level 协议级别
     * @param protocolName 协议名称
     * @param version 版本字符串
     */
    MqttVersion(byte level, String protocolName, String version) {
        this.level = level;
        this.protocolName = protocolName;
        this.version = version;
    }
    
    /**
     * 获取协议级别
     *
     * @return 协议级别
     */
    public byte getLevel() {
        return level;
    }
    
    /**
     * 获取协议名称
     *
     * @return 协议名称
     */
    public String getProtocolName() {
        return protocolName;
    }
    
    /**
     * 获取版本字符串
     *
     * @return 版本字符串
     */
    public String getVersion() {
        return version;
    }
    
    /**
     * 根据协议级别获取MQTT版本
     *
     * @param level 协议级别
     * @return MQTT版本
     * @throws IllegalArgumentException 如果协议级别不支持
     */
    public static MqttVersion fromLevel(byte level) {
        for (MqttVersion version : values()) {
            if (version.level == level) {
                return version;
            }
        }
        throw new IllegalArgumentException("Unsupported MQTT protocol level: " + level);
    }
    
    /**
     * 根据协议名称和级别获取MQTT版本
     *
     * @param protocolName 协议名称
     * @param level 协议级别
     * @return MQTT版本
     * @throws IllegalArgumentException 如果协议不支持
     */
    public static MqttVersion fromProtocolNameAndLevel(String protocolName, byte level) {
        for (MqttVersion version : values()) {
            if (version.protocolName.equals(protocolName) && version.level == level) {
                return version;
            }
        }
        throw new IllegalArgumentException(
                String.format("Unsupported MQTT protocol: name=%s, level=%d", protocolName, level));
    }
    
    /**
     * 检查是否为MQTT 5.0协议
     *
     * @return 如果是MQTT 5.0返回true
     */
    public boolean isMqtt5() {
        return this == MQTT_5_0;
    }
    
    /**
     * 检查是否支持属性（MQTT 5.0特性）
     *
     * @return 如果支持属性返回true
     */
    public boolean supportsProperties() {
        return isMqtt5();
    }
    
    /**
     * 检查是否支持用户属性
     *
     * @return 如果支持用户属性返回true
     */
    public boolean supportsUserProperties() {
        return isMqtt5();
    }
    
    /**
     * 检查是否支持原因码
     *
     * @return 如果支持原因码返回true
     */
    public boolean supportsReasonCodes() {
        return isMqtt5();
    }
    
    /**
     * 检查是否支持共享订阅
     *
     * @return 如果支持共享订阅返回true
     */
    public boolean supportsSharedSubscriptions() {
        return isMqtt5();
    }
    
    /**
     * 检查是否支持订阅标识符
     *
     * @return 如果支持订阅标识符返回true
     */
    public boolean supportsSubscriptionIdentifiers() {
        return isMqtt5();
    }
    
    /**
     * 检查是否支持主题别名
     *
     * @return 如果支持主题别名返回true
     */
    public boolean supportsTopicAliases() {
        return isMqtt5();
    }
    
    @Override
    public String toString() {
        return String.format("MQTT %s (level=%d, protocol=%s)", version, level, protocolName);
    }
}