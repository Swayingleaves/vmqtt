/**
 * MQTT QoS级别定义
 *
 * @author zhenglin
 * @date 2025/08/06
 */
package com.vmqtt.common.protocol;

/**
 * MQTT服务质量（Quality of Service）级别
 */
public enum MqttQos {
    /**
     * 至多一次传递
     * 消息可能丢失，但不会重复
     * 性能最高，适用于可以容忍消息丢失的场景
     */
    AT_MOST_ONCE(0, "At most once delivery"),
    
    /**
     * 至少一次传递
     * 消息保证到达，但可能重复
     * 需要确认机制，适用于不能容忍消息丢失的场景
     */
    AT_LEAST_ONCE(1, "At least once delivery"),
    
    /**
     * 恰好一次传递
     * 消息保证到达且只传递一次
     * 性能最低，适用于对消息准确性要求极高的场景
     */
    EXACTLY_ONCE(2, "Exactly once delivery");
    
    /**
     * QoS级别值
     */
    private final int value;
    
    /**
     * QoS描述
     */
    private final String description;
    
    /**
     * 构造函数
     *
     * @param value QoS级别值
     * @param description QoS描述
     */
    MqttQos(int value, String description) {
        this.value = value;
        this.description = description;
    }
    
    /**
     * 获取QoS级别值
     *
     * @return QoS级别值
     */
    public int getValue() {
        return value;
    }
    
    /**
     * 获取QoS描述
     *
     * @return QoS描述
     */
    public String getDescription() {
        return description;
    }
    
    /**
     * 根据值获取QoS级别
     *
     * @param value QoS级别值
     * @return QoS级别
     * @throws IllegalArgumentException 如果QoS级别不支持
     */
    public static MqttQos fromValue(int value) {
        for (MqttQos qos : values()) {
            if (qos.value == value) {
                return qos;
            }
        }
        throw new IllegalArgumentException("Invalid QoS level: " + value);
    }
    
    /**
     * 检查是否需要确认
     *
     * @return 如果需要确认返回true
     */
    public boolean requiresAcknowledgment() {
        return this != AT_MOST_ONCE;
    }
    
    /**
     * 检查是否需要存储
     *
     * @return 如果需要存储返回true
     */
    public boolean requiresStorage() {
        return this != AT_MOST_ONCE;
    }
    
    /**
     * 检查是否需要重传
     *
     * @return 如果需要重传返回true
     */
    public boolean requiresRetransmission() {
        return this != AT_MOST_ONCE;
    }
    
    /**
     * 检查是否需要去重
     *
     * @return 如果需要去重返回true
     */
    public boolean requiresDeduplication() {
        return this == EXACTLY_ONCE;
    }
    
    /**
     * 获取较低的QoS级别
     *
     * @param other 另一个QoS级别
     * @return 较低的QoS级别
     */
    public MqttQos min(MqttQos other) {
        return this.value <= other.value ? this : other;
    }
    
    /**
     * 获取较高的QoS级别
     *
     * @param other 另一个QoS级别
     * @return 较高的QoS级别
     */
    public MqttQos max(MqttQos other) {
        return this.value >= other.value ? this : other;
    }
    
    /**
     * 检查是否是有效的QoS级别
     *
     * @param value QoS级别值
     * @return 如果有效返回true
     */
    public static boolean isValid(int value) {
        return value >= 0 && value <= 2;
    }
    
    @Override
    public String toString() {
        return String.format("QoS %d (%s)", value, description);
    }
}