/**
 * MQTT订阅确认返回码
 *
 * @author zhenglin
 * @date 2025/08/06
 */
package com.vmqtt.common.protocol.packet.suback;

/**
 * MQTT SUBACK包返回码定义
 * 
 * 支持MQTT 3.1.1和5.0的返回码
 */
public enum MqttSubackReturnCode {
    /**
     * 最大QoS 0
     */
    MAXIMUM_QOS_0((byte) 0x00, "Maximum QoS 0", true),
    
    /**
     * 最大QoS 1
     */
    MAXIMUM_QOS_1((byte) 0x01, "Maximum QoS 1", true),
    
    /**
     * 最大QoS 2
     */
    MAXIMUM_QOS_2((byte) 0x02, "Maximum QoS 2", true),
    
    /**
     * 失败（MQTT 3.1.1）
     */
    FAILURE((byte) 0x80, "Failure", false);
    
    /**
     * 返回码值
     */
    private final byte value;
    
    /**
     * 返回码描述
     */
    private final String description;
    
    /**
     * 是否为成功返回码
     */
    private final boolean success;
    
    /**
     * 构造函数
     *
     * @param value 返回码值
     * @param description 描述
     * @param success 是否成功
     */
    MqttSubackReturnCode(byte value, String description, boolean success) {
        this.value = value;
        this.description = description;
        this.success = success;
    }
    
    /**
     * 获取返回码值
     *
     * @return 返回码值
     */
    public byte getValue() {
        return value;
    }
    
    /**
     * 获取返回码描述
     *
     * @return 返回码描述
     */
    public String getDescription() {
        return description;
    }
    
    /**
     * 检查是否为成功返回码
     *
     * @return 如果是成功返回码返回true
     */
    public boolean isSuccess() {
        return success;
    }
    
    /**
     * 检查是否为失败返回码
     *
     * @return 如果是失败返回码返回true
     */
    public boolean isFailure() {
        return !success;
    }
    
    /**
     * 获取对应的QoS等级
     *
     * @return QoS等级，如果是失败返回码返回-1
     */
    public int getQosLevel() {
        return switch (this) {
            case MAXIMUM_QOS_0 -> 0;
            case MAXIMUM_QOS_1 -> 1;
            case MAXIMUM_QOS_2 -> 2;
            case FAILURE -> -1;
        };
    }
    
    /**
     * 根据值获取返回码
     *
     * @param value 返回码值
     * @return MQTT SUBACK返回码
     * @throws IllegalArgumentException 如果值无效
     */
    public static MqttSubackReturnCode fromValue(byte value) {
        for (MqttSubackReturnCode returnCode : values()) {
            if (returnCode.value == value) {
                return returnCode;
            }
        }
        throw new IllegalArgumentException("Invalid MQTT SUBACK return code: " + value);
    }
    
    /**
     * 根据值获取返回码（安全版本）
     *
     * @param value 返回码值
     * @return MQTT SUBACK返回码，如果值无效返回null
     */
    public static MqttSubackReturnCode fromValueSafe(byte value) {
        for (MqttSubackReturnCode returnCode : values()) {
            if (returnCode.value == value) {
                return returnCode;
            }
        }
        return null;
    }
    
    /**
     * 根据QoS等级获取返回码
     *
     * @param qos QoS等级
     * @return 对应的成功返回码
     * @throws IllegalArgumentException 如果QoS等级无效
     */
    public static MqttSubackReturnCode fromQos(int qos) {
        return switch (qos) {
            case 0 -> MAXIMUM_QOS_0;
            case 1 -> MAXIMUM_QOS_1;
            case 2 -> MAXIMUM_QOS_2;
            default -> throw new IllegalArgumentException("Invalid QoS level: " + qos);
        };
    }
    
    /**
     * 检查值是否有效
     *
     * @param value 返回码值
     * @return 如果值有效返回true
     */
    public static boolean isValidValue(byte value) {
        return fromValueSafe(value) != null;
    }
    
    /**
     * 获取所有成功的返回码
     *
     * @return 成功返回码数组
     */
    public static MqttSubackReturnCode[] getSuccessCodes() {
        return new MqttSubackReturnCode[] {
            MAXIMUM_QOS_0,
            MAXIMUM_QOS_1,
            MAXIMUM_QOS_2
        };
    }
    
    @Override
    public String toString() {
        return String.format("%s(%d): %s", name(), value, description);
    }
}