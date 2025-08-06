/**
 * MQTT数据包类型枚举
 *
 * @author zhenglin
 * @date 2025/08/06
 */
package com.vmqtt.common.protocol.packet;

/**
 * MQTT控制包类型定义
 * 
 * 根据MQTT 3.1.1和5.0规范定义的16种控制包类型
 */
public enum MqttPacketType {
    /**
     * 保留 - 禁止使用
     */
    RESERVED_0(0, "RESERVED", false, false),
    
    /**
     * 客户端连接到服务器
     */
    CONNECT(1, "CONNECT", false, false),
    
    /**
     * 服务器响应连接请求
     */
    CONNACK(2, "CONNACK", false, false),
    
    /**
     * 发布消息
     */
    PUBLISH(3, "PUBLISH", true, true),
    
    /**
     * 发布确认 (QoS 1)
     */
    PUBACK(4, "PUBACK", false, true),
    
    /**
     * 发布收到 (QoS 2)
     */
    PUBREC(5, "PUBREC", false, true),
    
    /**
     * 发布释放 (QoS 2)
     */
    PUBREL(6, "PUBREL", false, true),
    
    /**
     * 发布完成 (QoS 2)
     */
    PUBCOMP(7, "PUBCOMP", false, true),
    
    /**
     * 订阅主题
     */
    SUBSCRIBE(8, "SUBSCRIBE", false, true),
    
    /**
     * 订阅确认
     */
    SUBACK(9, "SUBACK", false, true),
    
    /**
     * 取消订阅
     */
    UNSUBSCRIBE(10, "UNSUBSCRIBE", false, true),
    
    /**
     * 取消订阅确认
     */
    UNSUBACK(11, "UNSUBACK", false, true),
    
    /**
     * PING请求
     */
    PINGREQ(12, "PINGREQ", false, false),
    
    /**
     * PING响应
     */
    PINGRESP(13, "PINGRESP", false, false),
    
    /**
     * 断开连接
     */
    DISCONNECT(14, "DISCONNECT", false, false),
    
    /**
     * 认证交换 (仅MQTT 5.0)
     */
    AUTH(15, "AUTH", false, false);
    
    /**
     * 包类型值（4位）
     */
    private final int value;
    
    /**
     * 包类型名称
     */
    private final String name;
    
    /**
     * 是否可以设置DUP标志
     */
    private final boolean canHaveDupFlag;
    
    /**
     * 是否包含包标识符
     */
    private final boolean hasPacketId;
    
    /**
     * 构造函数
     *
     * @param value 包类型值
     * @param name 包类型名称
     * @param canHaveDupFlag 是否可以设置DUP标志
     * @param hasPacketId 是否包含包标识符
     */
    MqttPacketType(int value, String name, boolean canHaveDupFlag, boolean hasPacketId) {
        this.value = value;
        this.name = name;
        this.canHaveDupFlag = canHaveDupFlag;
        this.hasPacketId = hasPacketId;
    }
    
    /**
     * 获取包类型值
     *
     * @return 包类型值
     */
    public int getValue() {
        return value;
    }
    
    /**
     * 获取包类型名称
     *
     * @return 包类型名称
     */
    public String getName() {
        return name;
    }
    
    /**
     * 检查是否可以设置DUP标志
     *
     * @return 如果可以设置DUP标志返回true
     */
    public boolean canHaveDupFlag() {
        return canHaveDupFlag;
    }
    
    /**
     * 检查是否包含包标识符
     *
     * @return 如果包含包标识符返回true
     */
    public boolean hasPacketId() {
        return hasPacketId;
    }
    
    /**
     * 根据值获取包类型
     *
     * @param value 包类型值
     * @return MQTT包类型
     * @throws IllegalArgumentException 如果值无效
     */
    public static MqttPacketType fromValue(int value) {
        for (MqttPacketType type : values()) {
            if (type.value == value) {
                return type;
            }
        }
        throw new IllegalArgumentException("Invalid MQTT packet type value: " + value);
    }
    
    /**
     * 检查是否为客户端到服务器的包类型
     *
     * @return 如果是客户端到服务器的包返回true
     */
    public boolean isClientToServer() {
        return switch (this) {
            case CONNECT, PUBLISH, PUBACK, PUBREC, PUBREL, PUBCOMP, 
                 SUBSCRIBE, UNSUBSCRIBE, PINGREQ, DISCONNECT, AUTH -> true;
            default -> false;
        };
    }
    
    /**
     * 检查是否为服务器到客户端的包类型
     *
     * @return 如果是服务器到客户端的包返回true
     */
    public boolean isServerToClient() {
        return switch (this) {
            case CONNACK, PUBLISH, PUBACK, PUBREC, PUBREL, PUBCOMP, 
                 SUBACK, UNSUBACK, PINGRESP, DISCONNECT, AUTH -> true;
            default -> false;
        };
    }
    
    /**
     * 检查是否为双向包类型
     *
     * @return 如果是双向包返回true
     */
    public boolean isBidirectional() {
        return switch (this) {
            case PUBLISH, PUBACK, PUBREC, PUBREL, PUBCOMP, DISCONNECT, AUTH -> true;
            default -> false;
        };
    }
    
    /**
     * 检查是否仅在MQTT 5.0中支持
     *
     * @return 如果仅在MQTT 5.0中支持返回true
     */
    public boolean isMqtt5Only() {
        return this == AUTH;
    }
    
    @Override
    public String toString() {
        return String.format("%s(%d)", name, value);
    }
}