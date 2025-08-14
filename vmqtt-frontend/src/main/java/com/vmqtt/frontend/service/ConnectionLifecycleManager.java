/**
 * 连接生命周期管理器
 *
 * @author zhenglin
 * @date 2025/08/08
 */
package com.vmqtt.frontend.service;

import com.vmqtt.common.model.ClientConnection;
import com.vmqtt.common.service.ConnectionManager;
import com.vmqtt.frontend.config.NettyServerConfig;
import io.netty.channel.Channel;
import io.netty.handler.timeout.IdleState;
import io.netty.util.AttributeKey;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

/**
 * 连接生命周期管理服务
 * 负责管理MQTT客户端连接的完整生命周期
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class ConnectionLifecycleManager {
    
    private final NettyServerConfig config;
    private final ConnectionManager connectionManager;
    
    // Channel属性键定义
    private static final AttributeKey<String> CONNECTION_ID_KEY = AttributeKey.valueOf("connectionId");
    private static final AttributeKey<String> CLIENT_ID_KEY = AttributeKey.valueOf("clientId");
    private static final AttributeKey<LocalDateTime> LAST_ACTIVITY_KEY = AttributeKey.valueOf("lastActivity");
    private static final AttributeKey<Boolean> AUTHENTICATED_KEY = AttributeKey.valueOf("authenticated");
    
    /**
     * 处理通道激活事件
     *
     * @param channel 网络通道
     */
    public void handleChannelActive(Channel channel) {
        try {
            // 生成唯一连接ID
            String connectionId = generateConnectionId();
            
            // 设置通道属性
            channel.attr(CONNECTION_ID_KEY).set(connectionId);
            channel.attr(LAST_ACTIVITY_KEY).set(LocalDateTime.now());
            channel.attr(AUTHENTICATED_KEY).set(false);
            
            // 创建连接对象
            ClientConnection connection = createConnection(channel, connectionId);
            
            // 注册连接
            CompletableFuture<Void> registerFuture = connectionManager.registerConnection(connection);
            registerFuture.whenComplete((result, ex) -> {
                if (ex != null) {
                    log.error("注册连接失败: connectionId={}", connectionId, ex);
                    channel.close();
                } else {
                    log.debug("连接已激活并注册: connectionId={}, remoteAddress={}", 
                        connectionId, channel.remoteAddress());
                }
            });
            
        } catch (Exception e) {
            log.error("处理通道激活事件失败: {}", channel.remoteAddress(), e);
            channel.close();
        }
    }
    
    /**
     * 处理通道关闭事件
     *
     * @param channel 网络通道
     */
    public void handleChannelInactive(Channel channel) {
        try {
            String connectionId = getConnectionId(channel);
            String clientId = getClientId(channel);
            
            if (connectionId != null) {
                // 移除连接
                connectionManager.removeConnection(connectionId);
                log.debug("连接已关闭并清理: connectionId={}, clientId={}", connectionId, clientId);
            }
            
        } catch (Exception e) {
            log.error("处理通道关闭事件失败: {}", channel.remoteAddress(), e);
        }
    }
    
    /**
     * 处理连接异常
     *
     * @param channel 网络通道
     * @param cause 异常原因
     */
    public void handleConnectionException(Channel channel, Throwable cause) {
        try {
            String connectionId = getConnectionId(channel);
            String clientId = getClientId(channel);
            
            log.warn("连接发生异常: connectionId={}, clientId={}, cause={}", 
                connectionId, clientId, cause.getMessage());
            
            // 记录异常并关闭连接
            if (connectionId != null) {
                connectionManager.disconnectConnection(connectionId, "连接异常: " + cause.getMessage());
            } else {
                channel.close();
            }
            
        } catch (Exception e) {
            log.error("处理连接异常时发生错误", e);
            channel.close();
        }
    }
    
    /**
     * 处理协议错误
     *
     * @param channel 网络通道
     * @param error 协议错误
     */
    public void handleProtocolError(Channel channel, Exception error) {
        try {
            String connectionId = getConnectionId(channel);
            String clientId = getClientId(channel);
            
            log.warn("协议错误: connectionId={}, clientId={}, error={}", 
                connectionId, clientId, error.getMessage());
            
            // 根据错误类型决定处理方式
            if (isRecoverableError(error)) {
                // 可恢复错误，记录但保持连接
                log.debug("可恢复的协议错误，保持连接");
            } else {
                // 严重错误，关闭连接
                if (connectionId != null) {
                    connectionManager.disconnectConnection(connectionId, "协议错误: " + error.getMessage());
                } else {
                    channel.close();
                }
            }
            
        } catch (Exception e) {
            log.error("处理协议错误时发生异常", e);
            channel.close();
        }
    }
    
    /**
     * 处理空闲超时
     *
     * @param channel 网络通道
     * @param idleState 空闲状态
     */
    public void handleIdleTimeout(Channel channel, IdleState idleState) {
        try {
            String connectionId = getConnectionId(channel);
            String clientId = getClientId(channel);
            
            log.debug("连接空闲超时: connectionId={}, clientId={}, idleState={}", 
                connectionId, clientId, idleState);
            
            switch (idleState) {
                case READER_IDLE:
                    // 读空闲：客户端可能断线
                    handleReadIdle(channel, connectionId, clientId);
                    break;
                case WRITER_IDLE:
                    // 写空闲：服务端长时间未发送数据
                    handleWriteIdle(channel, connectionId, clientId);
                    break;
                case ALL_IDLE:
                    // 读写空闲：连接完全空闲
                    handleAllIdle(channel, connectionId, clientId);
                    break;
            }
            
        } catch (Exception e) {
            log.error("处理空闲超时时发生异常", e);
            channel.close();
        }
    }
    
    /**
     * 更新连接最后活动时间
     *
     * @param channel 网络通道
     */
    public void updateLastActivity(Channel channel) {
        try {
            channel.attr(LAST_ACTIVITY_KEY).set(LocalDateTime.now());
            
            String connectionId = getConnectionId(channel);
            if (connectionId != null) {
                connectionManager.updateLastActivity(connectionId);
            }
            
        } catch (Exception e) {
            log.error("更新连接活动时间失败", e);
        }
    }
    
    /**
     * 设置客户端ID
     *
     * @param channel 网络通道
     * @param clientId 客户端ID
     */
    public void setClientId(Channel channel, String clientId) {
        channel.attr(CLIENT_ID_KEY).set(clientId);
        log.debug("设置客户端ID: connectionId={}, clientId={}", getConnectionId(channel), clientId);
    }
    
    /**
     * 设置认证状态
     *
     * @param channel 网络通道
     * @param authenticated 是否已认证
     */
    public void setAuthenticated(Channel channel, boolean authenticated) {
        channel.attr(AUTHENTICATED_KEY).set(authenticated);
        log.debug("设置认证状态: connectionId={}, authenticated={}", getConnectionId(channel), authenticated);
    }
    
    /**
     * 检查是否已认证
     *
     * @param channel 网络通道
     * @return 是否已认证
     */
    public boolean isAuthenticated(Channel channel) {
        Boolean authenticated = channel.attr(AUTHENTICATED_KEY).get();
        return authenticated != null && authenticated;
    }
    
    /**
     * 处理读空闲
     */
    private void handleReadIdle(Channel channel, String connectionId, String clientId) {
        log.debug("处理读空闲: connectionId={}, clientId={}", connectionId, clientId);
        
        // 发送PINGREQ检查连接状态（如果是服务端主动检查的话）
        // 或者等待客户端发送PINGREQ，超时后关闭连接
        
        // 这里简单地关闭长时间无读取的连接
        if (connectionId != null) {
            connectionManager.disconnectConnection(connectionId, "读空闲超时");
        } else {
            channel.close();
        }
    }
    
    /**
     * 处理写空闲
     */
    private void handleWriteIdle(Channel channel, String connectionId, String clientId) {
        log.debug("处理写空闲: connectionId={}, clientId={}", connectionId, clientId);
        
        // 写空闲通常不需要特殊处理，除非需要发送保活消息
    }
    
    /**
     * 处理全空闲
     */
    private void handleAllIdle(Channel channel, String connectionId, String clientId) {
        log.debug("处理全空闲: connectionId={}, clientId={}", connectionId, clientId);
        
        // 全空闲说明连接完全不活跃，应该关闭
        if (connectionId != null) {
            connectionManager.disconnectConnection(connectionId, "连接完全空闲");
        } else {
            channel.close();
        }
    }
    
    /**
     * 创建连接对象
     */
    private ClientConnection createConnection(Channel channel, String connectionId) {
        return ClientConnection.builder()
            .connectionId(connectionId)
            .channel(channel)
            .remoteAddress(channel.remoteAddress().toString())
            .connectedAt(LocalDateTime.now())
            .lastActivity(LocalDateTime.now())
            .connectionState(ClientConnection.ConnectionState.CONNECTED)
            .build();
    }
    
    /**
     * 生成连接ID
     */
    private String generateConnectionId() {
        return "conn_" + UUID.randomUUID().toString().replace("-", "");
    }
    
    /**
     * 从Channel获取连接ID
     */
    private String getConnectionId(Channel channel) {
        return channel.attr(CONNECTION_ID_KEY).get();
    }
    
    /**
     * 从Channel获取客户端ID
     */
    private String getClientId(Channel channel) {
        return channel.attr(CLIENT_ID_KEY).get();
    }
    
    /**
     * 判断是否为可恢复错误
     */
    private boolean isRecoverableError(Exception error) {
        // 简单实现：检查错误类型
        String errorMessage = error.getMessage();
        if (errorMessage == null) {
            return false;
        }
        
        // 可恢复的错误类型
        return errorMessage.contains("timeout") || 
               errorMessage.contains("interrupted") ||
               errorMessage.contains("temporary");
    }
}