/**
 * MQTT 5.0属性定义
 *
 * @author zhenglin
 * @date 2025/08/06
 */
package com.vmqtt.common.protocol.property;

/**
 * MQTT 5.0属性枚举
 * 
 * 定义所有MQTT 5.0规范中的属性标识符
 */
public enum MqttProperty {
    /**
     * 载荷格式指示符
     */
    PAYLOAD_FORMAT_INDICATOR(0x01, "Payload Format Indicator", MqttPropertyType.BYTE),
    
    /**
     * 消息过期间隔
     */
    MESSAGE_EXPIRY_INTERVAL(0x02, "Message Expiry Interval", MqttPropertyType.FOUR_BYTE_INTEGER),
    
    /**
     * 内容类型
     */
    CONTENT_TYPE(0x03, "Content Type", MqttPropertyType.UTF8_STRING),
    
    /**
     * 响应主题
     */
    RESPONSE_TOPIC(0x08, "Response Topic", MqttPropertyType.UTF8_STRING),
    
    /**
     * 关联数据
     */
    CORRELATION_DATA(0x09, "Correlation Data", MqttPropertyType.BINARY_DATA),
    
    /**
     * 订阅标识符
     */
    SUBSCRIPTION_IDENTIFIER(0x0B, "Subscription Identifier", MqttPropertyType.VARIABLE_BYTE_INTEGER),
    
    /**
     * 会话过期间隔
     */
    SESSION_EXPIRY_INTERVAL(0x11, "Session Expiry Interval", MqttPropertyType.FOUR_BYTE_INTEGER),
    
    /**
     * 分配的客户端标识符
     */
    ASSIGNED_CLIENT_IDENTIFIER(0x12, "Assigned Client Identifier", MqttPropertyType.UTF8_STRING),
    
    /**
     * 服务器保活
     */
    SERVER_KEEP_ALIVE(0x13, "Server Keep Alive", MqttPropertyType.TWO_BYTE_INTEGER),
    
    /**
     * 认证方法
     */
    AUTHENTICATION_METHOD(0x15, "Authentication Method", MqttPropertyType.UTF8_STRING),
    
    /**
     * 认证数据
     */
    AUTHENTICATION_DATA(0x16, "Authentication Data", MqttPropertyType.BINARY_DATA),
    
    /**
     * 请求问题信息
     */
    REQUEST_PROBLEM_INFORMATION(0x17, "Request Problem Information", MqttPropertyType.BYTE),
    
    /**
     * 遗嘱延迟间隔
     */
    WILL_DELAY_INTERVAL(0x18, "Will Delay Interval", MqttPropertyType.FOUR_BYTE_INTEGER),
    
    /**
     * 请求响应信息
     */
    REQUEST_RESPONSE_INFORMATION(0x19, "Request Response Information", MqttPropertyType.BYTE),
    
    /**
     * 响应信息
     */
    RESPONSE_INFORMATION(0x1A, "Response Information", MqttPropertyType.UTF8_STRING),
    
    /**
     * 服务器引用
     */
    SERVER_REFERENCE(0x1C, "Server Reference", MqttPropertyType.UTF8_STRING),
    
    /**
     * 原因字符串
     */
    REASON_STRING(0x1F, "Reason String", MqttPropertyType.UTF8_STRING),
    
    /**
     * 接收最大值
     */
    RECEIVE_MAXIMUM(0x21, "Receive Maximum", MqttPropertyType.TWO_BYTE_INTEGER),
    
    /**
     * 主题别名最大值
     */
    TOPIC_ALIAS_MAXIMUM(0x22, "Topic Alias Maximum", MqttPropertyType.TWO_BYTE_INTEGER),
    
    /**
     * 主题别名
     */
    TOPIC_ALIAS(0x23, "Topic Alias", MqttPropertyType.TWO_BYTE_INTEGER),
    
    /**
     * 最大QoS
     */
    MAXIMUM_QOS(0x24, "Maximum QoS", MqttPropertyType.BYTE),
    
    /**
     * 保留可用
     */
    RETAIN_AVAILABLE(0x25, "Retain Available", MqttPropertyType.BYTE),
    
    /**
     * 用户属性
     */
    USER_PROPERTY(0x26, "User Property", MqttPropertyType.UTF8_STRING_PAIR),
    
    /**
     * 最大包大小
     */
    MAXIMUM_PACKET_SIZE(0x27, "Maximum Packet Size", MqttPropertyType.FOUR_BYTE_INTEGER),
    
    /**
     * 通配符订阅可用
     */
    WILDCARD_SUBSCRIPTION_AVAILABLE(0x28, "Wildcard Subscription Available", MqttPropertyType.BYTE),
    
    /**
     * 订阅标识符可用
     */
    SUBSCRIPTION_IDENTIFIER_AVAILABLE(0x29, "Subscription Identifier Available", MqttPropertyType.BYTE),
    
    /**
     * 共享订阅可用
     */
    SHARED_SUBSCRIPTION_AVAILABLE(0x2A, "Shared Subscription Available", MqttPropertyType.BYTE);
    
    /**
     * 属性标识符
     */
    private final int identifier;
    
    /**
     * 属性名称
     */
    private final String name;
    
    /**
     * 属性类型
     */
    private final MqttPropertyType type;
    
    /**
     * 构造函数
     *
     * @param identifier 属性标识符
     * @param name 属性名称
     * @param type 属性类型
     */
    MqttProperty(int identifier, String name, MqttPropertyType type) {
        this.identifier = identifier;
        this.name = name;
        this.type = type;
    }
    
    /**
     * 获取属性标识符
     *
     * @return 属性标识符
     */
    public int getIdentifier() {
        return identifier;
    }
    
    /**
     * 获取属性名称
     *
     * @return 属性名称
     */
    public String getName() {
        return name;
    }
    
    /**
     * 获取属性类型
     *
     * @return 属性类型
     */
    public MqttPropertyType getType() {
        return type;
    }
    
    /**
     * 根据标识符获取属性
     *
     * @param identifier 属性标识符
     * @return MQTT属性
     * @throws IllegalArgumentException 如果标识符无效
     */
    public static MqttProperty fromIdentifier(int identifier) {
        for (MqttProperty property : values()) {
            if (property.identifier == identifier) {
                return property;
            }
        }
        throw new IllegalArgumentException("Unknown property identifier: " + identifier);
    }
    
    /**
     * 根据标识符获取属性（安全版本）
     *
     * @param identifier 属性标识符
     * @return MQTT属性，如果标识符无效返回null
     */
    public static MqttProperty fromIdentifierSafe(int identifier) {
        for (MqttProperty property : values()) {
            if (property.identifier == identifier) {
                return property;
            }
        }
        return null;
    }
    
    /**
     * 检查标识符是否有效
     *
     * @param identifier 属性标识符
     * @return 如果有效返回true
     */
    public static boolean isValidIdentifier(int identifier) {
        return fromIdentifierSafe(identifier) != null;
    }
    
    @Override
    public String toString() {
        return String.format("%s(0x%02X): %s [%s]", name(), identifier, name, type);
    }
}