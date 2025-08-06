/**
 * MQTT连接返回码
 *
 * @author zhenglin
 * @date 2025/08/06
 */
package com.vmqtt.common.protocol.packet.connack;

/**
 * MQTT CONNACK包连接返回码定义
 * 
 * 支持MQTT 3.1、3.1.1和5.0的返回码
 */
public enum MqttConnectReturnCode {
    /**
     * 连接已接受
     */
    CONNECTION_ACCEPTED((byte) 0x00, "Connection accepted"),
    
    /**
     * 连接已拒绝，不支持的协议版本
     */
    CONNECTION_REFUSED_UNACCEPTABLE_PROTOCOL_VERSION((byte) 0x01, "Connection refused: unacceptable protocol version"),
    
    /**
     * 连接已拒绝，不合格的客户端标识符
     */
    CONNECTION_REFUSED_IDENTIFIER_REJECTED((byte) 0x02, "Connection refused: identifier rejected"),
    
    /**
     * 连接已拒绝，服务端不可用
     */
    CONNECTION_REFUSED_SERVER_UNAVAILABLE((byte) 0x03, "Connection refused: server unavailable"),
    
    /**
     * 连接已拒绝，无效的用户名或密码
     */
    CONNECTION_REFUSED_BAD_USER_NAME_OR_PASSWORD((byte) 0x04, "Connection refused: bad username or password"),
    
    /**
     * 连接已拒绝，未授权
     */
    CONNECTION_REFUSED_NOT_AUTHORIZED((byte) 0x05, "Connection refused: not authorized");
    
    /**
     * 返回码值
     */
    private final byte value;
    
    /**
     * 返回码描述
     */
    private final String description;
    
    /**
     * 构造函数
     *
     * @param value 返回码值
     * @param description 描述
     */
    MqttConnectReturnCode(byte value, String description) {
        this.value = value;
        this.description = description;
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
        return this == CONNECTION_ACCEPTED;
    }
    
    /**
     * 检查是否为失败返回码
     *
     * @return 如果是失败返回码返回true
     */
    public boolean isFailure() {
        return this != CONNECTION_ACCEPTED;
    }
    
    /**
     * 根据值获取返回码
     *
     * @param value 返回码值
     * @return MQTT连接返回码
     * @throws IllegalArgumentException 如果值无效
     */
    public static MqttConnectReturnCode fromValue(byte value) {
        for (MqttConnectReturnCode returnCode : values()) {
            if (returnCode.value == value) {
                return returnCode;
            }
        }
        throw new IllegalArgumentException("Invalid MQTT connect return code: " + value);
    }
    
    /**
     * 根据值获取返回码（安全版本）
     *
     * @param value 返回码值
     * @return MQTT连接返回码，如果值无效返回null
     */
    public static MqttConnectReturnCode fromValueSafe(byte value) {
        for (MqttConnectReturnCode returnCode : values()) {
            if (returnCode.value == value) {
                return returnCode;
            }
        }
        return null;
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
     * 获取所有失败的返回码
     *
     * @return 失败返回码数组
     */
    public static MqttConnectReturnCode[] getFailureCodes() {
        return new MqttConnectReturnCode[] {
            CONNECTION_REFUSED_UNACCEPTABLE_PROTOCOL_VERSION,
            CONNECTION_REFUSED_IDENTIFIER_REJECTED,
            CONNECTION_REFUSED_SERVER_UNAVAILABLE,
            CONNECTION_REFUSED_BAD_USER_NAME_OR_PASSWORD,
            CONNECTION_REFUSED_NOT_AUTHORIZED
        };
    }
    
    /**
     * 根据异常类型获取对应的返回码
     *
     * @param exception 异常
     * @return 对应的返回码
     */
    public static MqttConnectReturnCode fromException(Exception exception) {
        String message = exception.getMessage().toLowerCase();
        
        if (message.contains("protocol") || message.contains("version")) {
            return CONNECTION_REFUSED_UNACCEPTABLE_PROTOCOL_VERSION;
        } else if (message.contains("identifier") || message.contains("client")) {
            return CONNECTION_REFUSED_IDENTIFIER_REJECTED;
        } else if (message.contains("username") || message.contains("password") || message.contains("credential")) {
            return CONNECTION_REFUSED_BAD_USER_NAME_OR_PASSWORD;
        } else if (message.contains("authorized") || message.contains("permission")) {
            return CONNECTION_REFUSED_NOT_AUTHORIZED;
        } else {
            return CONNECTION_REFUSED_SERVER_UNAVAILABLE;
        }
    }
    
    @Override
    public String toString() {
        return String.format("%s(%d): %s", name(), value, description);
    }
}