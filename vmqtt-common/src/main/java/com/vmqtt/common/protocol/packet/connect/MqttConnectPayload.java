/**
 * MQTT连接包负载
 *
 * @author zhenglin
 * @date 2025/08/06
 */
package com.vmqtt.common.protocol.packet.connect;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Objects;

/**
 * MQTT CONNECT包负载定义
 * 
 * 包含客户端标识符、遗嘱主题、遗嘱消息、用户名和密码
 */
public record MqttConnectPayload(
        String clientId,
        String willTopic,
        byte[] willMessage,
        String username,
        byte[] password) {
    
    /**
     * 构造函数验证
     */
    public MqttConnectPayload {
        // 验证客户端ID
        if (clientId != null) {
            validateClientId(clientId);
        }
        
        // 验证遗嘱主题
        if (willTopic != null) {
            validateTopic(willTopic, "Will topic");
        }
        
        // 验证用户名
        if (username != null) {
            validateString(username, "Username", 65535);
        }
        
        // 复制可变数组以保证不可变性
        if (willMessage != null) {
            willMessage = willMessage.clone();
        }
        if (password != null) {
            password = password.clone();
        }
    }
    
    /**
     * 获取遗嘱消息的副本
     *
     * @return 遗嘱消息副本，如果为null则返回null
     */
    public byte[] getWillMessageCopy() {
        return willMessage != null ? willMessage.clone() : null;
    }
    
    /**
     * 获取密码的副本
     *
     * @return 密码副本，如果为null则返回null
     */
    public byte[] getPasswordCopy() {
        return password != null ? password.clone() : null;
    }
    
    /**
     * 获取遗嘱消息的字符串表示
     *
     * @return 遗嘱消息字符串
     */
    public String getWillMessageAsString() {
        return willMessage != null ? new String(willMessage, StandardCharsets.UTF_8) : null;
    }
    
    /**
     * 获取密码的字符串表示
     *
     * @return 密码字符串
     */
    public String getPasswordAsString() {
        return password != null ? new String(password, StandardCharsets.UTF_8) : null;
    }
    
    /**
     * 检查是否有遗嘱消息
     *
     * @return 如果有遗嘱消息返回true
     */
    public boolean hasWillMessage() {
        return willMessage != null && willMessage.length > 0;
    }
    
    /**
     * 检查是否有密码
     *
     * @return 如果有密码返回true
     */
    public boolean hasPassword() {
        return password != null && password.length > 0;
    }
    
    /**
     * 创建简单的负载（仅客户端ID）
     *
     * @param clientId 客户端ID
     * @return MQTT连接负载
     */
    public static MqttConnectPayload simple(String clientId) {
        return new MqttConnectPayload(clientId, null, null, null, null);
    }
    
    /**
     * 创建带认证的负载
     *
     * @param clientId 客户端ID
     * @param username 用户名
     * @param password 密码
     * @return MQTT连接负载
     */
    public static MqttConnectPayload withCredentials(String clientId, String username, String password) {
        return new MqttConnectPayload(clientId, null, null, username, 
                                    password != null ? password.getBytes(StandardCharsets.UTF_8) : null);
    }
    
    /**
     * 创建带遗嘱的负载
     *
     * @param clientId 客户端ID
     * @param willTopic 遗嘱主题
     * @param willMessage 遗嘱消息
     * @return MQTT连接负载
     */
    public static MqttConnectPayload withWill(String clientId, String willTopic, String willMessage) {
        return new MqttConnectPayload(clientId, willTopic, 
                                    willMessage != null ? willMessage.getBytes(StandardCharsets.UTF_8) : null, 
                                    null, null);
    }
    
    /**
     * 创建完整的负载
     *
     * @param clientId 客户端ID
     * @param willTopic 遗嘱主题
     * @param willMessage 遗嘱消息
     * @param username 用户名
     * @param password 密码
     * @return MQTT连接负载
     */
    public static MqttConnectPayload complete(String clientId, String willTopic, String willMessage,
                                            String username, String password) {
        return new MqttConnectPayload(
            clientId, 
            willTopic, 
            willMessage != null ? willMessage.getBytes(StandardCharsets.UTF_8) : null,
            username, 
            password != null ? password.getBytes(StandardCharsets.UTF_8) : null
        );
    }
    
    /**
     * 验证客户端ID
     *
     * @param clientId 客户端ID
     */
    private static void validateClientId(String clientId) {
        if (clientId.length() > 23) {
            // MQTT 3.1规范建议客户端ID不超过23个字符，但3.1.1和5.0没有限制
            // 这里作为警告处理，实际实现中可以根据需要调整
        }
        
        // 检查是否包含无效字符（根据MQTT规范，客户端ID应该只包含0-9, a-z, A-Z）
        // 但实际上许多实现允许更多字符，这里保持宽松
        if (clientId.isEmpty()) {
            // 空客户端ID在某些情况下是允许的（Clean Session = true）
        }
    }
    
    /**
     * 验证主题名
     *
     * @param topic 主题名
     * @param fieldName 字段名（用于错误消息）
     */
    private static void validateTopic(String topic, String fieldName) {
        if (topic.isEmpty()) {
            throw new IllegalArgumentException(fieldName + " cannot be empty");
        }
        
        if (topic.length() > 65535) {
            throw new IllegalArgumentException(fieldName + " length cannot exceed 65535 bytes");
        }
        
        // MQTT主题名不能包含通配符
        if (topic.contains("+") || topic.contains("#")) {
            throw new IllegalArgumentException(fieldName + " cannot contain wildcards");
        }
        
        // 不能包含空字符
        if (topic.contains("\0")) {
            throw new IllegalArgumentException(fieldName + " cannot contain null characters");
        }
    }
    
    /**
     * 验证字符串字段
     *
     * @param value 字符串值
     * @param fieldName 字段名
     * @param maxLength 最大长度
     */
    private static void validateString(String value, String fieldName, int maxLength) {
        if (value.length() > maxLength) {
            throw new IllegalArgumentException(fieldName + " length cannot exceed " + maxLength + " bytes");
        }
        
        // 不能包含空字符
        if (value.contains("\0")) {
            throw new IllegalArgumentException(fieldName + " cannot contain null characters");
        }
    }
    
    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null || getClass() != obj.getClass()) return false;
        
        MqttConnectPayload that = (MqttConnectPayload) obj;
        return Objects.equals(clientId, that.clientId) &&
               Objects.equals(willTopic, that.willTopic) &&
               Arrays.equals(willMessage, that.willMessage) &&
               Objects.equals(username, that.username) &&
               Arrays.equals(password, that.password);
    }
    
    @Override
    public int hashCode() {
        int result = Objects.hash(clientId, willTopic, username);
        result = 31 * result + Arrays.hashCode(willMessage);
        result = 31 * result + Arrays.hashCode(password);
        return result;
    }
    
    @Override
    public String toString() {
        return String.format("MqttConnectPayload{clientId='%s', willTopic='%s', hasWillMessage=%s, username='%s', hasPassword=%s}",
                           clientId, willTopic, hasWillMessage(), username, hasPassword());
    }
}