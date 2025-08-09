/**
 * 连接生命周期管理器测试
 *
 * @author zhenglin
 * @date 2025/08/09
 */
package com.vmqtt.frontend.service;

import com.vmqtt.common.service.ConnectionManager;
import com.vmqtt.frontend.config.NettyServerConfig;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.timeout.IdleState;
import io.netty.util.Attribute;
import io.netty.util.AttributeKey;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.net.InetSocketAddress;
import java.util.concurrent.CompletableFuture;

import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

/**
 * ConnectionLifecycleManager测试
 */
@ExtendWith(MockitoExtension.class)
class ConnectionLifecycleManagerTest {
    
    @Mock
    private NettyServerConfig config;
    
    @Mock
    private ConnectionManager connectionManager;
    
    @Mock
    private Channel channel;
    
    @Mock
    private ChannelHandlerContext ctx;
    
    @Mock
    private Attribute<String> stringAttribute;
    
    @Mock
    private Attribute<Boolean> booleanAttribute;
    
    private ConnectionLifecycleManager lifecycleManager;
    
    @BeforeEach
    void setUp() {
        lifecycleManager = new ConnectionLifecycleManager(config, connectionManager);
        
        // 设置默认行为
        when(channel.attr(any(AttributeKey.class))).thenReturn(stringAttribute);
        when(channel.remoteAddress()).thenReturn(new InetSocketAddress("127.0.0.1", 12345));
        when(connectionManager.registerConnection(any())).thenReturn(CompletableFuture.completedFuture(null));
    }
    
    @Test
    void testHandleChannelActive_Success() {
        // Given
        when(connectionManager.registerConnection(any())).thenReturn(CompletableFuture.completedFuture(null));
        
        // When
        lifecycleManager.handleChannelActive(channel);
        
        // Then
        verify(channel, atLeastOnce()).attr(any(AttributeKey.class));
        verify(connectionManager, timeout(1000)).registerConnection(any());
    }
    
    @Test
    void testHandleChannelActive_RegistrationFailure() {
        // Given
        CompletableFuture<Void> failedFuture = new CompletableFuture<>();
        failedFuture.completeExceptionally(new RuntimeException("Registration failed"));
        when(connectionManager.registerConnection(any())).thenReturn(failedFuture);
        
        // When
        lifecycleManager.handleChannelActive(channel);
        
        // Then
        verify(connectionManager, timeout(1000)).registerConnection(any());
        verify(channel, timeout(1000)).close();
    }
    
    @Test
    void testHandleChannelInactive_WithConnectionId() {
        // Given
        when(stringAttribute.get()).thenReturn("conn_123", "client_456");
        
        // When
        lifecycleManager.handleChannelInactive(channel);
        
        // Then
        verify(connectionManager).removeConnection("conn_123");
    }
    
    @Test
    void testHandleChannelInactive_WithoutConnectionId() {
        // Given
        when(stringAttribute.get()).thenReturn(null);
        
        // When
        lifecycleManager.handleChannelInactive(channel);
        
        // Then
        verify(connectionManager, never()).removeConnection(anyString());
    }
    
    @Test
    void testHandleConnectionException() {
        // Given
        Exception cause = new RuntimeException("Test exception");
        when(stringAttribute.get()).thenReturn("conn_123", "client_456");
        
        // When
        lifecycleManager.handleConnectionException(channel, cause);
        
        // Then
        verify(connectionManager).disconnectConnection("conn_123", "连接异常: Test exception");
    }
    
    @Test
    void testHandleProtocolError_RecoverableError() {
        // Given
        Exception error = new RuntimeException("timeout error");
        when(stringAttribute.get()).thenReturn("conn_123", "client_456");
        
        // When
        lifecycleManager.handleProtocolError(channel, error);
        
        // Then - should not disconnect for recoverable errors
        verify(connectionManager, never()).disconnectConnection(anyString(), anyString());
    }
    
    @Test
    void testHandleProtocolError_NonRecoverableError() {
        // Given
        Exception error = new RuntimeException("fatal error");
        when(stringAttribute.get()).thenReturn("conn_123", "client_456");
        
        // When
        lifecycleManager.handleProtocolError(channel, error);
        
        // Then
        verify(connectionManager).disconnectConnection("conn_123", "协议错误: fatal error");
    }
    
    @Test
    void testHandleIdleTimeout_ReaderIdle() {
        // Given
        when(stringAttribute.get()).thenReturn("conn_123", "client_456");
        
        // When
        lifecycleManager.handleIdleTimeout(channel, IdleState.READER_IDLE);
        
        // Then
        verify(connectionManager).disconnectConnection("conn_123", "读空闲超时");
    }
    
    @Test
    void testHandleIdleTimeout_WriterIdle() {
        // Given
        when(stringAttribute.get()).thenReturn("conn_123", "client_456");
        
        // When
        lifecycleManager.handleIdleTimeout(channel, IdleState.WRITER_IDLE);
        
        // Then - writer idle should not cause disconnection by default
        verify(connectionManager, never()).disconnectConnection(anyString(), anyString());
    }
    
    @Test
    void testHandleIdleTimeout_AllIdle() {
        // Given
        when(stringAttribute.get()).thenReturn("conn_123", "client_456");
        
        // When
        lifecycleManager.handleIdleTimeout(channel, IdleState.ALL_IDLE);
        
        // Then
        verify(connectionManager).disconnectConnection("conn_123", "连接完全空闲");
    }
    
    @Test
    void testUpdateLastActivity() {
        // Given
        when(stringAttribute.get()).thenReturn("conn_123");
        
        // When
        lifecycleManager.updateLastActivity(channel);
        
        // Then
        verify(connectionManager).updateLastActivity("conn_123");
    }
    
    @Test
    void testSetClientId() {
        // When
        lifecycleManager.setClientId(channel, "test_client");
        
        // Then
        verify(stringAttribute).set("test_client");
    }
    
    @Test
    void testSetAuthenticated() {
        // Given
        @SuppressWarnings("unchecked")
        Attribute<Boolean> boolAttr = mock(Attribute.class);
        when(channel.attr(any(AttributeKey.class))).thenReturn(boolAttr);
        
        // When
        lifecycleManager.setAuthenticated(channel, true);
        
        // Then
        verify(boolAttr).set(true);
    }
    
    @Test
    void testIsAuthenticated_True() {
        // Given
        @SuppressWarnings("unchecked")
        Attribute<Boolean> boolAttr = mock(Attribute.class);
        when(channel.attr(any(AttributeKey.class))).thenReturn(boolAttr);
        when(boolAttr.get()).thenReturn(true);
        
        // When
        boolean result = lifecycleManager.isAuthenticated(channel);
        
        // Then
        assert result;
    }
    
    @Test
    void testIsAuthenticated_False() {
        // Given
        @SuppressWarnings("unchecked")
        Attribute<Boolean> boolAttr = mock(Attribute.class);
        when(channel.attr(any(AttributeKey.class))).thenReturn(boolAttr);
        when(boolAttr.get()).thenReturn(false);
        
        // When
        boolean result = lifecycleManager.isAuthenticated(channel);
        
        // Then
        assert !result;
    }
    
    @Test
    void testIsAuthenticated_Null() {
        // Given
        @SuppressWarnings("unchecked")
        Attribute<Boolean> boolAttr = mock(Attribute.class);
        when(channel.attr(any(AttributeKey.class))).thenReturn(boolAttr);
        when(boolAttr.get()).thenReturn(null);
        
        // When
        boolean result = lifecycleManager.isAuthenticated(channel);
        
        // Then
        assert !result;
    }
}