/**
 * 客户端连接信息
 *
 * @author zhenglin
 * @date 2025/08/06
 */
package com.vmqtt.common.model;

import com.vmqtt.common.protocol.MqttVersion;
import lombok.Data;
import lombok.Builder;
import lombok.AllArgsConstructor;
import lombok.NoArgsConstructor;
import io.netty.channel.Channel;

import java.time.LocalDateTime;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 客户端连接模型
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class ClientConnection {
    
    /**
     * 连接唯一标识
     */
    private String connectionId;
    
    /**
     * 客户端ID
     */
    private String clientId;
    
    /**
     * 网络通道
     */
    private Channel channel;
    
    /**
     * MQTT协议版本
     */
    private MqttVersion mqttVersion;
    
    /**
     * 会话ID
     */
    private String sessionId;
    
    /**
     * 用户名
     */
    private String username;
    
    /**
     * 客户端IP地址
     */
    private String clientIp;
    
    /**
     * 客户端端口
     */
    private int clientPort;
    
    /**
     * 连接建立时间
     */
    @Builder.Default
    private LocalDateTime connectedAt = LocalDateTime.now();
    
    /**
     * 最后活动时间
     */
    @Builder.Default
    private LocalDateTime lastActivity = LocalDateTime.now();
    
    /**
     * 保持连接时间（秒）
     */
    @Builder.Default
    private int keepAliveSeconds = 60;
    
    /**
     * 清理会话标志
     */
    @Builder.Default
    private boolean cleanSession = true;
    
    /**
     * 连接状态
     */
    @Builder.Default
    private ConnectionState state = ConnectionState.CONNECTING;
    
    /**
     * 连接属性（MQTT 5.0）
     */
    @Builder.Default
    private Map<String, Object> properties = new ConcurrentHashMap<>();
    
    /**
     * 统计信息
     */
    @Builder.Default
    private ConnectionStats stats = new ConnectionStats();
    
    /**
     * 连接状态枚举
     */
    public enum ConnectionState {
        /**
         * 连接中
         */
        CONNECTING,
        
        /**
         * 已连接
         */
        CONNECTED,
        
        /**
         * 断开连接中
         */
        DISCONNECTING,
        
        /**
         * 已断开
         */
        DISCONNECTED,
        
        /**
         * 连接失败
         */
        FAILED
    }
    
    /**
     * 连接统计信息
     */
    @Data
    @Builder
    @NoArgsConstructor
    @AllArgsConstructor
    public static class ConnectionStats {
        /**
         * 接收消息数
         */
        @Builder.Default
        private long messagesReceived = 0L;
        
        /**
         * 发送消息数
         */
        @Builder.Default
        private long messagesSent = 0L;
        
        /**
         * 接收字节数
         */
        @Builder.Default
        private long bytesReceived = 0L;
        
        /**
         * 发送字节数
         */
        @Builder.Default
        private long bytesSent = 0L;
        
        /**
         * 最后心跳时间
         */
        private LocalDateTime lastHeartbeat;
        
        /**
         * 丢包数
         */
        @Builder.Default
        private long packetsDropped = 0L;
    }
    
    /**
     * 检查连接是否活跃
     *
     * @return 如果连接活跃返回true
     */
    public boolean isActive() {
        return channel != null && channel.isActive() && 
               state == ConnectionState.CONNECTED;
    }
    
    /**
     * 检查连接是否已过期
     *
     * @return 如果连接已过期返回true
     */
    public boolean isExpired() {
        if (keepAliveSeconds <= 0) {
            return false;
        }
        
        LocalDateTime expiryTime = lastActivity.plusSeconds(keepAliveSeconds * 2);
        return LocalDateTime.now().isAfter(expiryTime);
    }
    
    /**
     * 更新最后活动时间
     */
    public void updateLastActivity() {
        this.lastActivity = LocalDateTime.now();
    }
    
    /**
     * 增加接收消息计数
     *
     * @param messageSize 消息大小
     */
    public void incrementMessagesReceived(int messageSize) {
        stats.messagesReceived++;
        stats.bytesReceived += messageSize;
        updateLastActivity();
    }
    
    /**
     * 增加发送消息计数
     *
     * @param messageSize 消息大小
     */
    public void incrementMessagesSent(int messageSize) {
        stats.messagesSent++;
        stats.bytesSent += messageSize;
    }
    
    /**
     * 增加丢包计数
     */
    public void incrementPacketsDropped() {
        stats.packetsDropped++;
    }
    
    /**
     * 获取连接持续时间（毫秒）
     *
     * @return 连接持续时间
     */
    public long getConnectionDurationMs() {
        return java.time.Duration.between(connectedAt, LocalDateTime.now()).toMillis();
    }
    
    /**
     * 获取空闲时间（毫秒）
     *
     * @return 空闲时间
     */
    public long getIdleTimeMs() {
        return java.time.Duration.between(lastActivity, LocalDateTime.now()).toMillis();
    }
}