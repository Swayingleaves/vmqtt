/**
 * 连接管理器接口
 *
 * @author zhenglin
 * @date 2025/08/06
 */
package com.vmqtt.common.service;

import com.vmqtt.common.model.ClientConnection;
import io.netty.channel.Channel;

import java.util.Collection;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

/**
 * 连接管理器接口
 * 负责管理所有MQTT客户端连接的生命周期
 */
public interface ConnectionManager {
    
    /**
     * 处理新的客户端连接
     *
     * @param channel 网络通道
     * @return 异步操作结果
     */
    CompletableFuture<Void> handleNewConnection(Channel channel);
    
    /**
     * 注册客户端连接
     *
     * @param connection 客户端连接信息
     * @return 异步操作结果
     */
    CompletableFuture<Void> registerConnection(ClientConnection connection);
    
    /**
     * 移除客户端连接
     *
     * @param connectionId 连接ID
     * @return 被移除的连接信息
     */
    CompletableFuture<Optional<ClientConnection>> removeConnection(String connectionId);
    
    /**
     * 根据连接ID获取连接信息
     *
     * @param connectionId 连接ID
     * @return 连接信息
     */
    Optional<ClientConnection> getConnection(String connectionId);
    
    /**
     * 根据客户端ID获取连接信息
     *
     * @param clientId 客户端ID
     * @return 连接信息
     */
    Optional<ClientConnection> getConnectionByClientId(String clientId);
    
    /**
     * 获取所有活跃连接
     *
     * @return 活跃连接集合
     */
    Collection<ClientConnection> getActiveConnections();
    
    /**
     * 获取连接数量
     *
     * @return 连接数量
     */
    long getConnectionCount();
    
    /**
     * 检查客户端是否已连接
     *
     * @param clientId 客户端ID
     * @return 如果已连接返回true
     */
    boolean isClientConnected(String clientId);
    
    /**
     * 断开指定客户端连接
     *
     * @param clientId 客户端ID
     * @param reason 断开原因
     * @return 异步操作结果
     */
    CompletableFuture<Boolean> disconnectClient(String clientId, String reason);
    
    /**
     * 断开指定连接
     *
     * @param connectionId 连接ID
     * @param reason 断开原因
     * @return 异步操作结果
     */
    CompletableFuture<Boolean> disconnectConnection(String connectionId, String reason);
    
    /**
     * 更新连接的最后活动时间
     *
     * @param connectionId 连接ID
     */
    void updateLastActivity(String connectionId);
    
    /**
     * 清理过期连接
     *
     * @return 清理的连接数量
     */
    CompletableFuture<Integer> cleanupExpiredConnections();
    
    /**
     * 获取连接统计信息
     *
     * @return 连接统计信息
     */
    ConnectionStats getConnectionStats();
    
    /**
     * 注册连接事件监听器
     *
     * @param listener 连接事件监听器
     */
    void addConnectionListener(ConnectionEventListener listener);
    
    /**
     * 移除连接事件监听器
     *
     * @param listener 连接事件监听器
     */
    void removeConnectionListener(ConnectionEventListener listener);
    
    /**
     * 连接统计信息
     */
    interface ConnectionStats {
        /**
         * 获取总连接数
         *
         * @return 总连接数
         */
        long getTotalConnections();
        
        /**
         * 获取活跃连接数
         *
         * @return 活跃连接数
         */
        long getActiveConnections();
        
        /**
         * 获取失败连接数
         *
         * @return 失败连接数
         */
        long getFailedConnections();
        
        /**
         * 获取平均连接持续时间
         *
         * @return 平均连接持续时间（毫秒）
         */
        long getAverageConnectionDuration();
        
        /**
         * 获取连接建立速率（每秒）
         *
         * @return 连接建立速率
         */
        double getConnectionRate();
        
        /**
         * 获取连接断开速率（每秒）
         *
         * @return 连接断开速率
         */
        double getDisconnectionRate();
    }
    
    /**
     * 连接事件监听器
     */
    interface ConnectionEventListener {
        /**
         * 连接建立事件
         *
         * @param connection 连接信息
         */
        void onConnectionEstablished(ClientConnection connection);
        
        /**
         * 连接断开事件
         *
         * @param connection 连接信息
         * @param reason 断开原因
         */
        void onConnectionClosed(ClientConnection connection, String reason);
        
        /**
         * 连接失败事件
         *
         * @param connectionId 连接ID
         * @param reason 失败原因
         */
        void onConnectionFailed(String connectionId, String reason);
        
        /**
         * 连接超时事件
         *
         * @param connection 连接信息
         */
        void onConnectionTimeout(ClientConnection connection);
    }
}