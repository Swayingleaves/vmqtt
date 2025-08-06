/**
 * 连接管理器实现类测试
 *
 * @author zhenglin
 * @date 2025/08/06
 */
package com.vmqtt.common.service.impl;

import com.vmqtt.common.model.ClientConnection;
import com.vmqtt.common.service.ConnectionManager;
import io.netty.channel.Channel;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.net.SocketAddress;
import java.time.LocalDateTime;
import java.util.Collection;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

/**
 * 连接管理器实现类单元测试
 */
@ExtendWith(MockitoExtension.class)
class ConnectionManagerImplTest {

    private ConnectionManagerImpl connectionManager;

    @Mock
    private Channel mockChannel;

    @Mock
    private SocketAddress mockSocketAddress;

    @BeforeEach
    void setUp() {
        connectionManager = new ConnectionManagerImpl();
        
        // 模拟Channel行为
        when(mockChannel.remoteAddress()).thenReturn(mockSocketAddress);
        when(mockSocketAddress.toString()).thenReturn("127.0.0.1:12345");
        when(mockChannel.isActive()).thenReturn(true);
    }

    @Test
    void testHandleNewConnection() {
        // 测试处理新连接
        assertDoesNotThrow(() -> {
            connectionManager.handleNewConnection(mockChannel).join();
        });
    }

    @Test
    void testRegisterConnection() {
        // 创建测试连接
        ClientConnection connection = createTestConnection("conn_1", "client_1", mockChannel);

        // 注册连接
        assertDoesNotThrow(() -> {
            connectionManager.registerConnection(connection).join();
        });

        // 验证连接已注册
        Optional<ClientConnection> retrieved = connectionManager.getConnection("conn_1");
        assertTrue(retrieved.isPresent());
        assertEquals("client_1", retrieved.get().getClientId());
    }

    @Test
    void testRegisterConnectionWithDuplicateClientId() {
        // 创建两个使用相同客户端ID的连接
        ClientConnection connection1 = createTestConnection("conn_1", "client_1", mockChannel);
        ClientConnection connection2 = createTestConnection("conn_2", "client_1", mock(Channel.class));

        // 注册第一个连接
        connectionManager.registerConnection(connection1).join();

        // 注册第二个连接（相同客户端ID）
        connectionManager.registerConnection(connection2).join();

        // 验证只有第二个连接存在
        Optional<ClientConnection> byClientId = connectionManager.getConnectionByClientId("client_1");
        assertTrue(byClientId.isPresent());
        assertEquals("conn_2", byClientId.get().getConnectionId());
    }

    @Test
    void testRemoveConnection() {
        // 注册连接
        ClientConnection connection = createTestConnection("conn_1", "client_1", mockChannel);
        connectionManager.registerConnection(connection).join();

        // 移除连接
        Optional<ClientConnection> removed = connectionManager.removeConnection("conn_1").join();

        // 验证连接已移除
        assertTrue(removed.isPresent());
        assertEquals("conn_1", removed.get().getConnectionId());

        // 验证连接不再存在
        Optional<ClientConnection> retrieved = connectionManager.getConnection("conn_1");
        assertFalse(retrieved.isPresent());
    }

    @Test
    void testGetConnectionByClientId() {
        // 注册连接
        ClientConnection connection = createTestConnection("conn_1", "client_1", mockChannel);
        connectionManager.registerConnection(connection).join();

        // 根据客户端ID获取连接
        Optional<ClientConnection> retrieved = connectionManager.getConnectionByClientId("client_1");
        assertTrue(retrieved.isPresent());
        assertEquals("conn_1", retrieved.get().getConnectionId());

        // 查询不存在的客户端ID
        Optional<ClientConnection> notFound = connectionManager.getConnectionByClientId("not_exist");
        assertFalse(notFound.isPresent());
    }

    @Test
    void testGetActiveConnections() {
        // 注册多个连接
        ClientConnection connection1 = createTestConnection("conn_1", "client_1", mockChannel);
        ClientConnection connection2 = createTestConnection("conn_2", "client_2", mock(Channel.class));
        
        connectionManager.registerConnection(connection1).join();
        connectionManager.registerConnection(connection2).join();

        // 获取所有活跃连接
        Collection<ClientConnection> activeConnections = connectionManager.getActiveConnections();
        assertEquals(2, activeConnections.size());
    }

    @Test
    void testGetConnectionCount() {
        assertEquals(0, connectionManager.getConnectionCount());

        // 注册连接
        ClientConnection connection = createTestConnection("conn_1", "client_1", mockChannel);
        connectionManager.registerConnection(connection).join();

        assertEquals(1, connectionManager.getConnectionCount());
    }

    @Test
    void testIsClientConnected() {
        // 客户端未连接时
        assertFalse(connectionManager.isClientConnected("client_1"));

        // 注册连接
        ClientConnection connection = createTestConnection("conn_1", "client_1", mockChannel);
        connectionManager.registerConnection(connection).join();

        // 客户端已连接时
        assertTrue(connectionManager.isClientConnected("client_1"));
    }

    @Test
    void testDisconnectClient() {
        // 注册连接
        ClientConnection connection = createTestConnection("conn_1", "client_1", mockChannel);
        connectionManager.registerConnection(connection).join();

        // 断开客户端连接
        boolean result = connectionManager.disconnectClient("client_1", "Test disconnect").join();
        assertTrue(result);

        // 断开不存在的客户端
        boolean notFoundResult = connectionManager.disconnectClient("not_exist", "Test disconnect").join();
        assertFalse(notFoundResult);
    }

    @Test
    void testDisconnectConnection() {
        // 注册连接
        ClientConnection connection = createTestConnection("conn_1", "client_1", mockChannel);
        connectionManager.registerConnection(connection).join();

        // 断开连接
        boolean result = connectionManager.disconnectConnection("conn_1", "Test disconnect").join();
        assertTrue(result);

        // 断开不存在的连接
        boolean notFoundResult = connectionManager.disconnectConnection("not_exist", "Test disconnect").join();
        assertFalse(notFoundResult);
    }

    @Test
    void testUpdateLastActivity() {
        // 注册连接
        ClientConnection connection = createTestConnection("conn_1", "client_1", mockChannel);
        connectionManager.registerConnection(connection).join();

        LocalDateTime beforeUpdate = connection.getLastActivity();
        
        // 等待一小段时间确保时间变化
        try {
            Thread.sleep(10);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }

        // 更新最后活动时间
        connectionManager.updateLastActivity("conn_1");

        // 验证时间已更新
        Optional<ClientConnection> updated = connectionManager.getConnection("conn_1");
        assertTrue(updated.isPresent());
        assertTrue(updated.get().getLastActivity().isAfter(beforeUpdate));
    }

    @Test
    void testConnectionEventListener() {
        AtomicBoolean connectionEstablished = new AtomicBoolean(false);
        AtomicBoolean connectionClosed = new AtomicBoolean(false);

        // 添加事件监听器
        ConnectionManager.ConnectionEventListener listener = new ConnectionManager.ConnectionEventListener() {
            @Override
            public void onConnectionEstablished(ClientConnection connection) {
                connectionEstablished.set(true);
            }

            @Override
            public void onConnectionClosed(ClientConnection connection, String reason) {
                connectionClosed.set(true);
            }

            @Override
            public void onConnectionFailed(String connectionId, String reason) {
                // 测试中不使用
            }

            @Override
            public void onConnectionTimeout(ClientConnection connection) {
                // 测试中不使用
            }
        };

        connectionManager.addConnectionListener(listener);

        // 注册连接，触发建立事件
        ClientConnection connection = createTestConnection("conn_1", "client_1", mockChannel);
        connectionManager.registerConnection(connection).join();

        assertTrue(connectionEstablished.get());

        // 移除连接，触发关闭事件
        connectionManager.removeConnection("conn_1").join();

        assertTrue(connectionClosed.get());

        // 移除监听器
        connectionManager.removeConnectionListener(listener);
    }

    @Test
    void testGetConnectionStats() {
        ConnectionManager.ConnectionStats stats = connectionManager.getConnectionStats();
        assertNotNull(stats);
        
        // 初始统计信息应该为0
        assertEquals(0, stats.getTotalConnections());
        assertEquals(0, stats.getActiveConnections());
        assertEquals(0, stats.getFailedConnections());
    }

    /**
     * 创建测试用的客户端连接
     */
    private ClientConnection createTestConnection(String connectionId, String clientId, Channel channel) {
        return ClientConnection.builder()
                .connectionId(connectionId)
                .clientId(clientId)
                .channel(channel)
                .remoteAddress("127.0.0.1:12345")
                .connectedAt(LocalDateTime.now())
                .lastActivity(LocalDateTime.now())
                .connectionState(ClientConnection.ConnectionState.CONNECTED)
                .build();
    }
}