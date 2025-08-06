/**
 * MQTT发布包可变头部
 *
 * @author zhenglin
 * @date 2025/08/06
 */
package com.vmqtt.common.protocol.packet.publish;

import java.nio.charset.StandardCharsets;

/**
 * MQTT PUBLISH包可变头部定义
 * 
 * 包含主题名、包标识符（QoS>0时）和属性（MQTT 5.0）
 */
public record MqttPublishVariableHeader(
        String topicName,
        int packetId,
        Object properties) { // MQTT 5.0属性，使用Object暂时占位
    
    /**
     * 构造函数验证
     */
    public MqttPublishVariableHeader {
        // 验证主题名
        if (topicName == null) {
            throw new IllegalArgumentException("Topic name cannot be null");
        }
        
        if (topicName.isEmpty()) {
            throw new IllegalArgumentException("Topic name cannot be empty");
        }
        
        // 验证主题名长度
        byte[] topicBytes = topicName.getBytes(StandardCharsets.UTF_8);
        if (topicBytes.length > 65535) {
            throw new IllegalArgumentException("Topic name length cannot exceed 65535 bytes");
        }
        
        // 验证主题名不能包含通配符（发布时）
        if (topicName.contains("+") || topicName.contains("#")) {
            throw new IllegalArgumentException("Topic name cannot contain wildcards for PUBLISH packet");
        }
        
        // 验证主题名不能包含空字符
        if (topicName.contains("\0")) {
            throw new IllegalArgumentException("Topic name cannot contain null characters");
        }
        
        // 验证包标识符
        if (packetId < 0 || packetId > 65535) {
            throw new IllegalArgumentException("Packet ID must be between 0 and 65535");
        }
    }
    
    /**
     * 创建QoS 0的可变头部（无包标识符）
     *
     * @param topicName 主题名
     * @return PUBLISH可变头部
     */
    public static MqttPublishVariableHeader createQos0(String topicName) {
        return new MqttPublishVariableHeader(topicName, 0, null);
    }
    
    /**
     * 创建QoS > 0的可变头部（包含包标识符）
     *
     * @param topicName 主题名
     * @param packetId 包标识符
     * @return PUBLISH可变头部
     */
    public static MqttPublishVariableHeader createWithPacketId(String topicName, int packetId) {
        return new MqttPublishVariableHeader(topicName, packetId, null);
    }
    
    /**
     * 创建MQTT 5.0的可变头部（包含属性）
     *
     * @param topicName 主题名
     * @param packetId 包标识符
     * @param properties 属性
     * @return PUBLISH可变头部
     */
    public static MqttPublishVariableHeader createMqtt5(String topicName, int packetId, Object properties) {
        return new MqttPublishVariableHeader(topicName, packetId, properties);
    }
    
    /**
     * 获取主题名字节数组
     *
     * @return 主题名字节数组
     */
    public byte[] getTopicNameBytes() {
        return topicName.getBytes(StandardCharsets.UTF_8);
    }
    
    /**
     * 获取主题名长度
     *
     * @return 主题名字节长度
     */
    public int getTopicNameLength() {
        return getTopicNameBytes().length;
    }
    
    /**
     * 检查是否有包标识符
     *
     * @return 如果有包标识符返回true
     */
    public boolean hasPacketId() {
        return packetId > 0;
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
     * 验证主题名是否有效
     *
     * @param topicName 主题名
     * @return 如果有效返回true
     */
    public static boolean isValidTopicName(String topicName) {
        if (topicName == null || topicName.isEmpty()) {
            return false;
        }
        
        // 不能包含通配符
        if (topicName.contains("+") || topicName.contains("#")) {
            return false;
        }
        
        // 不能包含空字符
        if (topicName.contains("\0")) {
            return false;
        }
        
        // 检查长度
        try {
            byte[] bytes = topicName.getBytes(StandardCharsets.UTF_8);
            return bytes.length <= 65535;
        } catch (Exception e) {
            return false;
        }
    }
    
    /**
     * 检查主题名是否为系统主题
     *
     * @return 如果是系统主题返回true
     */
    public boolean isSystemTopic() {
        return topicName.startsWith("$");
    }
    
    @Override
    public String toString() {
        return String.format("MqttPublishVariableHeader{topic='%s', packetId=%d, hasProperties=%s}",
                           topicName, packetId, hasProperties());
    }
}