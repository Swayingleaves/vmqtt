/**
 * 连接管理器实现类
 *
 * @author zhenglin
 * @date 2025/08/06
 */
package com.vmqtt.common.service.impl;

import com.vmqtt.common.model.ClientConnection;
import com.vmqtt.common.service.ConnectionManager;
import com.vmqtt.common.util.MetricsUtils;
import io.netty.channel.Channel;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;
import java.util.Collection;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;

/**
 * 连接管理器默认实现
 * 基于内存存储的高性能连接管理
 */
@Slf4j
@Service
public class ConnectionManagerImpl implements ConnectionManager {

    /**
     * 连接存储：connectionId -> ClientConnection
     */
    private final ConcurrentHashMap<String, ClientConnection> connections = new ConcurrentHashMap<>();
    
    /**
     * 客户端ID映射：clientId -> connectionId
     */
    private final ConcurrentHashMap<String, String> clientIdToConnectionId = new ConcurrentHashMap<>();
    
    /**
     * 网络通道映射：channel -> connectionId
     */
    private final ConcurrentHashMap<Channel, String> channelToConnectionId = new ConcurrentHashMap<>();
    
    /**
     * 连接事件监听器集合
     */
    private final ConcurrentLinkedQueue<ConnectionEventListener> listeners = new ConcurrentLinkedQueue<>();
    
    /**
     * 连接统计信息
     */
    private final ConnectionStatsImpl stats = new ConnectionStatsImpl();

    @Override
    public CompletableFuture<Void> handleNewConnection(Channel channel) {
        return CompletableFuture.runAsync(() -> {
            try {
                String connectionId = generateConnectionId();
                String remoteAddress = channel.remoteAddress() != null ? 
                    channel.remoteAddress().toString() : "unknown";
                
                log.info("处理新连接: connectionId={}, remoteAddress={}", connectionId, remoteAddress);
                
                // 存储channel映射
                channelToConnectionId.put(channel, connectionId);
                
                // 更新统计信息
                stats.totalConnections.increment();
                
                // 触发连接建立事件（此时还没有完整的ClientConnection对象）
                log.debug("新连接已建立: connectionId={}", connectionId);
                
            } catch (Exception e) {
                log.error("处理新连接失败", e);
                stats.failedConnections.increment();
                throw new RuntimeException("处理新连接失败", e);
            }
        });
    }

    @Override
    public CompletableFuture<Void> registerConnection(ClientConnection connection) {
        return CompletableFuture.runAsync(() -> {
            try {
                String connectionId = connection.getConnectionId();
                String clientId = connection.getClientId();
                
                log.info("注册客户端连接: connectionId={}, clientId={}", connectionId, clientId);
                
                // 检查客户端ID是否已存在
                if (clientId != null && clientIdToConnectionId.containsKey(clientId)) {
                    String existingConnectionId = clientIdToConnectionId.get(clientId);
                    Optional<ClientConnection> existingConnection = Optional.ofNullable(connections.get(existingConnectionId));
                    
                    if (existingConnection.isPresent()) {
                        log.warn("客户端ID已存在，断开旧连接: clientId={}, oldConnectionId={}, newConnectionId={}", 
                            clientId, existingConnectionId, connectionId);
                        
                        // 异步断开旧连接
                        disconnectConnection(existingConnectionId, "Client ID taken over").join();
                    }
                }
                
                // 存储连接信息
                connections.put(connectionId, connection);
                if (clientId != null) {
                    clientIdToConnectionId.put(clientId, connectionId);
                }
                
                // 更新连接状态和时间
                connection.setConnectedAt(LocalDateTime.now());
                connection.setLastActivity(LocalDateTime.now());
                connection.setConnectionState(ClientConnection.ConnectionState.CONNECTED);
                
                // 更新统计信息
                stats.activeConnections.increment();
                
                // 触发连接建立事件
                listeners.forEach(listener -> {
                    try {
                        listener.onConnectionEstablished(connection);
                    } catch (Exception e) {
                        log.warn("连接事件监听器异常", e);
                    }
                });
                
                log.info("客户端连接注册成功: connectionId={}, clientId={}", connectionId, clientId);
                
            } catch (Exception e) {
                log.error("注册连接失败: connectionId={}", connection.getConnectionId(), e);
                stats.failedConnections.increment();
                throw new RuntimeException("注册连接失败", e);
            }
        });
    }

    @Override
    public CompletableFuture<Optional<ClientConnection>> removeConnection(String connectionId) {
        return CompletableFuture.supplyAsync(() -> {
            try {
                log.info("移除连接: connectionId={}", connectionId);
                
                ClientConnection connection = connections.remove(connectionId);
                if (connection == null) {
                    log.warn("连接不存在: connectionId={}", connectionId);
                    return Optional.empty();
                }
                
                // 清理映射关系
                String clientId = connection.getClientId();
                if (clientId != null) {
                    clientIdToConnectionId.remove(clientId);
                }
                
                Channel channel = connection.getChannel();
                if (channel != null) {
                    channelToConnectionId.remove(channel);
                }
                
                // 更新连接状态
                connection.setConnectionState(ClientConnection.ConnectionState.DISCONNECTED);
                connection.setDisconnectedAt(LocalDateTime.now());
                
                // 更新统计信息
                stats.activeConnections.decrement();
                
                // 触发连接断开事件
                listeners.forEach(listener -> {
                    try {
                        listener.onConnectionClosed(connection, "Connection removed");
                    } catch (Exception e) {
                        log.warn("连接事件监听器异常", e);
                    }
                });
                
                log.info("连接移除成功: connectionId={}, clientId={}", connectionId, clientId);
                return Optional.of(connection);
                
            } catch (Exception e) {
                log.error("移除连接失败: connectionId={}", connectionId, e);
                throw new RuntimeException("移除连接失败", e);
            }
        });
    }

    @Override
    public Optional<ClientConnection> getConnection(String connectionId) {
        return Optional.ofNullable(connections.get(connectionId));
    }

    @Override
    public Optional<ClientConnection> getConnectionByClientId(String clientId) {
        String connectionId = clientIdToConnectionId.get(clientId);
        if (connectionId != null) {
            return Optional.ofNullable(connections.get(connectionId));
        }
        return Optional.empty();
    }

    @Override
    public Collection<ClientConnection> getActiveConnections() {
        return connections.values();
    }

    @Override
    public long getConnectionCount() {
        return connections.size();
    }

    @Override
    public boolean isClientConnected(String clientId) {
        String connectionId = clientIdToConnectionId.get(clientId);
        if (connectionId != null) {
            ClientConnection connection = connections.get(connectionId);
            return connection != null && connection.getConnectionState() == ClientConnection.ConnectionState.CONNECTED;
        }
        return false;
    }

    @Override
    public CompletableFuture<Boolean> disconnectClient(String clientId, String reason) {
        return CompletableFuture.supplyAsync(() -> {
            try {
                log.info("断开客户端连接: clientId={}, reason={}", clientId, reason);
                
                String connectionId = clientIdToConnectionId.get(clientId);
                if (connectionId == null) {
                    log.warn("客户端连接不存在: clientId={}", clientId);
                    return false;
                }
                
                return disconnectConnection(connectionId, reason).join();
                
            } catch (Exception e) {
                log.error("断开客户端连接失败: clientId={}", clientId, e);
                return false;
            }
        });
    }

    @Override
    public CompletableFuture<Boolean> disconnectConnection(String connectionId, String reason) {
        return CompletableFuture.supplyAsync(() -> {
            try {
                log.info("断开连接: connectionId={}, reason={}", connectionId, reason);
                
                ClientConnection connection = connections.get(connectionId);
                if (connection == null) {
                    log.warn("连接不存在: connectionId={}", connectionId);
                    return false;
                }
                
                // 关闭网络通道
                Channel channel = connection.getChannel();
                if (channel != null && channel.isActive()) {
                    channel.close().addListener(future -> {
                        if (future.isSuccess()) {
                            log.debug("网络通道已关闭: connectionId={}", connectionId);
                        } else {
                            log.warn("关闭网络通道失败: connectionId={}", connectionId, future.cause());
                        }
                    });
                }
                
                // 异步移除连接
                removeConnection(connectionId).thenAccept(removedConnection -> {
                    if (removedConnection.isPresent()) {
                        log.info("连接断开完成: connectionId={}, clientId={}", 
                            connectionId, removedConnection.get().getClientId());
                    }
                });
                
                return true;
                
            } catch (Exception e) {
                log.error("断开连接失败: connectionId={}", connectionId, e);
                return false;
            }
        });
    }

    @Override
    public void updateLastActivity(String connectionId) {
        ClientConnection connection = connections.get(connectionId);
        if (connection != null) {
            connection.setLastActivity(LocalDateTime.now());
        }
    }

    @Override
    public CompletableFuture<Integer> cleanupExpiredConnections() {
        return CompletableFuture.supplyAsync(() -> {
            log.info("开始清理过期连接");
            
            int cleanupCount = 0;
            LocalDateTime now = LocalDateTime.now();
            
            for (ClientConnection connection : connections.values()) {
                try {
                    // 检查连接是否过期（简单实现：空闲超过30分钟）
                    if (connection.getIdleTimeMs() > 30 * 60 * 1000) {
                        log.info("清理过期连接: connectionId={}, clientId={}, idleTime={}ms", 
                            connection.getConnectionId(), connection.getClientId(), connection.getIdleTimeMs());
                        
                        disconnectConnection(connection.getConnectionId(), "Connection expired").join();
                        cleanupCount++;
                    }
                } catch (Exception e) {
                    log.warn("清理连接异常: connectionId={}", connection.getConnectionId(), e);
                }
            }
            
            log.info("过期连接清理完成，清理数量: {}", cleanupCount);
            return cleanupCount;
        });
    }

    @Override
    public ConnectionStats getConnectionStats() {
        return stats;
    }

    @Override
    public void addConnectionListener(ConnectionEventListener listener) {
        listeners.add(listener);
        log.debug("添加连接事件监听器: {}", listener.getClass().getSimpleName());
    }

    @Override
    public void removeConnectionListener(ConnectionEventListener listener) {
        listeners.remove(listener);
        log.debug("移除连接事件监听器: {}", listener.getClass().getSimpleName());
    }

    /**
     * 生成唯一连接ID
     *
     * @return 连接ID
     */
    private String generateConnectionId() {
        return "conn_" + UUID.randomUUID().toString().replace("-", "");
    }

    /**
     * 连接统计信息实现
     */
    private static class ConnectionStatsImpl implements ConnectionStats {
        private final LongAdder totalConnections = new LongAdder();
        private final LongAdder activeConnections = new LongAdder();
        private final LongAdder failedConnections = new LongAdder();
        private final AtomicLong startTime = new AtomicLong(System.currentTimeMillis());
        
        @Override
        public long getTotalConnections() {
            return totalConnections.sum();
        }

        @Override
        public long getActiveConnections() {
            return activeConnections.sum();
        }

        @Override
        public long getFailedConnections() {
            return failedConnections.sum();
        }

        @Override
        public long getAverageConnectionDuration() {
            // 简单实现：返回平均连接时长
            long totalTime = System.currentTimeMillis() - startTime.get();
            long totalConns = getTotalConnections();
            return totalConns > 0 ? totalTime / totalConns : 0;
        }

        @Override
        public double getConnectionRate() {
            long uptime = System.currentTimeMillis() - startTime.get();
            return uptime > 0 ? (getTotalConnections() * 1000.0 / uptime) : 0.0;
        }

        @Override
        public double getDisconnectionRate() {
            long uptime = System.currentTimeMillis() - startTime.get();
            long disconnections = getTotalConnections() - getActiveConnections();
            return uptime > 0 ? (disconnections * 1000.0 / uptime) : 0.0;
        }
    }
}