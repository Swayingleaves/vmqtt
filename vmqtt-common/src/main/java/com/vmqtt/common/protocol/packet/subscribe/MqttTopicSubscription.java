/**
 * MQTT主题订阅
 *
 * @author zhenglin
 * @date 2025/08/06
 */
package com.vmqtt.common.protocol.packet.subscribe;

import com.vmqtt.common.protocol.MqttQos;

import java.nio.charset.StandardCharsets;

/**
 * MQTT主题订阅定义
 * 
 * 包含主题过滤器和QoS等级
 */
public record MqttTopicSubscription(String topicFilter, int qos) {
    
    /**
     * 构造函数验证
     */
    public MqttTopicSubscription {
        // 验证主题过滤器
        if (topicFilter == null) {
            throw new IllegalArgumentException("Topic filter cannot be null");
        }
        
        if (topicFilter.isEmpty()) {
            throw new IllegalArgumentException("Topic filter cannot be empty");
        }
        
        // 验证主题过滤器长度
        byte[] topicBytes = topicFilter.getBytes(StandardCharsets.UTF_8);
        if (topicBytes.length > 65535) {
            throw new IllegalArgumentException("Topic filter length cannot exceed 65535 bytes");
        }
        
        // 验证主题过滤器格式
        validateTopicFilter(topicFilter);
        
        // 验证QoS等级
        if (qos < 0 || qos > 2) {
            throw new IllegalArgumentException("QoS must be 0, 1, or 2");
        }
    }
    
    /**
     * 获取QoS枚举值
     *
     * @return QoS等级
     */
    public MqttQos getQos() {
        return MqttQos.fromValue(qos);
    }
    
    /**
     * 检查是否为通配符订阅
     *
     * @return 如果包含通配符返回true
     */
    public boolean isWildcardSubscription() {
        return topicFilter.contains("+") || topicFilter.contains("#");
    }
    
    /**
     * 检查是否为单级通配符订阅
     *
     * @return 如果包含单级通配符返回true
     */
    public boolean hasSingleLevelWildcard() {
        return topicFilter.contains("+");
    }
    
    /**
     * 检查是否为多级通配符订阅
     *
     * @return 如果包含多级通配符返回true
     */
    public boolean hasMultiLevelWildcard() {
        return topicFilter.contains("#");
    }
    
    /**
     * 检查是否为系统主题订阅
     *
     * @return 如果是系统主题返回true
     */
    public boolean isSystemTopic() {
        return topicFilter.startsWith("$");
    }
    
    /**
     * 检查是否为共享订阅（MQTT 5.0）
     *
     * @return 如果是共享订阅返回true
     */
    public boolean isSharedSubscription() {
        return topicFilter.startsWith("$share/");
    }
    
    /**
     * 获取共享订阅的组名（MQTT 5.0）
     *
     * @return 共享组名，如果不是共享订阅返回null
     */
    public String getSharedGroupName() {
        if (!isSharedSubscription()) {
            return null;
        }
        
        String[] parts = topicFilter.split("/", 3);
        return parts.length >= 2 ? parts[1] : null;
    }
    
    /**
     * 获取共享订阅的实际主题过滤器（MQTT 5.0）
     *
     * @return 实际主题过滤器，如果不是共享订阅返回原过滤器
     */
    public String getActualTopicFilter() {
        if (!isSharedSubscription()) {
            return topicFilter;
        }
        
        String[] parts = topicFilter.split("/", 3);
        return parts.length >= 3 ? parts[2] : topicFilter;
    }
    
    /**
     * 验证主题过滤器格式
     *
     * @param topicFilter 主题过滤器
     */
    private static void validateTopicFilter(String topicFilter) {
        // 不能包含空字符
        if (topicFilter.contains("\0")) {
            throw new IllegalArgumentException("Topic filter cannot contain null characters");
        }
        
        // 验证通配符使用规则
        validateWildcards(topicFilter);
    }
    
    /**
     * 验证通配符使用规则
     *
     * @param topicFilter 主题过滤器
     */
    private static void validateWildcards(String topicFilter) {
        // 多级通配符规则
        int hashIndex = topicFilter.indexOf('#');
        if (hashIndex != -1) {
            // '#'必须是最后一个字符
            if (hashIndex != topicFilter.length() - 1) {
                throw new IllegalArgumentException("Multi-level wildcard '#' must be the last character");
            }
            
            // '#'前面必须是'/'或者是第一个字符
            if (hashIndex > 0 && topicFilter.charAt(hashIndex - 1) != '/') {
                throw new IllegalArgumentException("Multi-level wildcard '#' must be preceded by '/' or be the first character");
            }
            
            // 只能有一个'#'
            if (topicFilter.indexOf('#', hashIndex + 1) != -1) {
                throw new IllegalArgumentException("Topic filter can contain only one multi-level wildcard '#'");
            }
        }
        
        // 单级通配符规则
        int plusIndex = topicFilter.indexOf('+');
        while (plusIndex != -1) {
            // '+'前后必须是'/'或者是字符串边界
            if (plusIndex > 0 && topicFilter.charAt(plusIndex - 1) != '/') {
                throw new IllegalArgumentException("Single-level wildcard '+' must be preceded by '/' or be the first character");
            }
            
            if (plusIndex < topicFilter.length() - 1 && topicFilter.charAt(plusIndex + 1) != '/') {
                throw new IllegalArgumentException("Single-level wildcard '+' must be followed by '/' or be the last character");
            }
            
            plusIndex = topicFilter.indexOf('+', plusIndex + 1);
        }
    }
    
    /**
     * 检查主题过滤器是否有效
     *
     * @param topicFilter 主题过滤器
     * @return 如果有效返回true
     */
    public static boolean isValidTopicFilter(String topicFilter) {
        if (topicFilter == null || topicFilter.isEmpty()) {
            return false;
        }
        
        try {
            byte[] bytes = topicFilter.getBytes(StandardCharsets.UTF_8);
            if (bytes.length > 65535) {
                return false;
            }
            
            validateTopicFilter(topicFilter);
            return true;
        } catch (Exception e) {
            return false;
        }
    }
    
    @Override
    public String toString() {
        return String.format("MqttTopicSubscription{topicFilter='%s', qos=%d}", topicFilter, qos);
    }
}