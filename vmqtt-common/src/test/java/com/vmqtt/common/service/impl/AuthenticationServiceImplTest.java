/**
 * 认证服务实现类测试
 *
 * @author zhenglin
 * @date 2025/08/06
 */
package com.vmqtt.common.service.impl;

import com.vmqtt.common.model.ClientConnection;
import com.vmqtt.common.protocol.packet.connect.MqttConnectFlags;
import com.vmqtt.common.protocol.packet.connect.MqttConnectPacket;
import com.vmqtt.common.protocol.packet.connect.MqttConnectPayload;
import com.vmqtt.common.protocol.packet.connect.MqttConnectVariableHeader;
import com.vmqtt.common.service.AuthenticationService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.jupiter.api.Assertions.*;

/**
 * 认证服务实现类单元测试
 */
class AuthenticationServiceImplTest {

    private AuthenticationServiceImpl authenticationService;

    @BeforeEach
    void setUp() {
        authenticationService = new AuthenticationServiceImpl();
        authenticationService.init(); // 初始化默认管理员账户
    }

    @Test
    void testDefaultAdminAuthentication() {
        // 创建管理员连接包
        MqttConnectPacket connectPacket = createConnectPacket("admin", "admin", "admin123");
        ClientConnection connection = createTestConnection("conn_1", "127.0.0.1:12345");

        // 认证管理员
        AuthenticationService.AuthenticationResult result = 
            authenticationService.authenticate(connectPacket, connection).join();

        // 验证认证成功
        assertTrue(result.isSuccess());
        assertEquals(AuthenticationService.AuthenticationCode.CONNECTION_ACCEPTED, result.getReturnCode());
        assertNotNull(result.getPermissions());
        assertNotNull(result.getCredentials());
    }

    @Test
    void testAuthenticationWithInvalidClientId() {
        // 创建无效客户端ID的连接包
        MqttConnectPacket connectPacket = createConnectPacket("", "user", "pass");
        ClientConnection connection = createTestConnection("conn_1", "127.0.0.1:12345");

        // 认证
        AuthenticationService.AuthenticationResult result = 
            authenticationService.authenticate(connectPacket, connection).join();

        // 验证认证失败
        assertFalse(result.isSuccess());
        assertEquals(AuthenticationService.AuthenticationCode.IDENTIFIER_REJECTED, result.getReturnCode());
    }

    @Test
    void testAuthenticationWithBadCredentials() {
        // 创建错误凭据的连接包
        MqttConnectPacket connectPacket = createConnectPacket("admin", "admin", "wrong_password");
        ClientConnection connection = createTestConnection("conn_1", "127.0.0.1:12345");

        // 认证
        AuthenticationService.AuthenticationResult result = 
            authenticationService.authenticate(connectPacket, connection).join();

        // 验证认证失败
        assertFalse(result.isSuccess());
        assertEquals(AuthenticationService.AuthenticationCode.BAD_USERNAME_PASSWORD, result.getReturnCode());
    }

    @Test
    void testAuthenticationWithBlockedClient() {
        // 先禁用客户端
        authenticationService.blockClient("admin", "Test block").join();

        // 创建连接包
        MqttConnectPacket connectPacket = createConnectPacket("admin", "admin", "admin123");
        ClientConnection connection = createTestConnection("conn_1", "127.0.0.1:12345");

        // 认证
        AuthenticationService.AuthenticationResult result = 
            authenticationService.authenticate(connectPacket, connection).join();

        // 验证认证失败
        assertFalse(result.isSuccess());
        assertEquals(AuthenticationService.AuthenticationCode.CLIENT_BLOCKED, result.getReturnCode());
    }

    @Test
    void testCreateCredentials() {
        Map<String, Object> attributes = new HashMap<>();
        attributes.put("role", "user");

        // 创建凭据
        AuthenticationService.AuthenticationCredentials credentials = 
            authenticationService.createCredentials("test_client", "testuser", "testpass", attributes).join();

        // 验证凭据
        assertNotNull(credentials);
        assertEquals("test_client", credentials.getClientId());
        assertEquals("testuser", credentials.getUsername());
        assertNotNull(credentials.getPasswordHash());
        assertFalse(credentials.isExpired());
        assertEquals("user", credentials.getAttributes().get("role"));
    }

    @Test
    void testRevokeCredentials() {
        // 创建凭据
        authenticationService.createCredentials("test_client", "testuser", "testpass", null).join();

        // 撤销凭据
        boolean result = authenticationService.revokeCredentials("test_client").join();
        assertTrue(result);

        // 再次撤销应该返回false
        boolean secondResult = authenticationService.revokeCredentials("test_client").join();
        assertFalse(secondResult);
    }

    @Test
    void testCheckPublishPermission() {
        // 检查管理员权限（可以发布到系统主题）
        boolean adminPermission = authenticationService.checkPublishPermission("admin", "$SYS/test").join();
        assertTrue(adminPermission);

        // 创建普通用户
        authenticationService.createCredentials("normal_user", "user", "pass", null).join();

        // 检查普通用户权限（不能发布到系统主题）
        boolean userPermission = authenticationService.checkPublishPermission("normal_user", "$SYS/test").join();
        assertFalse(userPermission);

        // 检查普通用户权限（可以发布到普通主题）
        boolean normalTopicPermission = authenticationService.checkPublishPermission("normal_user", "test/topic").join();
        assertTrue(normalTopicPermission);
    }

    @Test
    void testCheckSubscribePermission() {
        // 检查管理员权限（可以订阅系统主题）
        boolean adminPermission = authenticationService.checkSubscribePermission("admin", "$SYS/+").join();
        assertTrue(adminPermission);

        // 创建普通用户
        authenticationService.createCredentials("normal_user", "user", "pass", null).join();

        // 检查普通用户权限（不能订阅系统主题）
        boolean userPermission = authenticationService.checkSubscribePermission("normal_user", "$SYS/+").join();
        assertFalse(userPermission);

        // 检查普通用户权限（可以订阅普通主题）
        boolean normalTopicPermission = authenticationService.checkSubscribePermission("normal_user", "test/+").join();
        assertTrue(normalTopicPermission);
    }

    @Test
    void testIsValidClientId() {
        // 有效的客户端ID
        assertTrue(authenticationService.isValidClientId("valid_client_123"));
        assertTrue(authenticationService.isValidClientId("client-123"));

        // 无效的客户端ID
        assertFalse(authenticationService.isValidClientId(null));
        assertFalse(authenticationService.isValidClientId(""));
        assertFalse(authenticationService.isValidClientId("   "));
        assertFalse(authenticationService.isValidClientId("client\nwith\ncontrol"));

        // 太长的客户端ID
        String longClientId = "a".repeat(70000);
        assertFalse(authenticationService.isValidClientId(longClientId));
    }

    @Test
    void testGetClientPermissions() {
        // 获取默认权限
        AuthenticationService.ClientPermissions permissions = 
            authenticationService.getClientPermissions("new_client");

        assertNotNull(permissions);
        assertTrue(permissions.canConnect());
        assertEquals(1, permissions.getMaxQos());
        assertEquals(1024 * 1024, permissions.getMaxMessageSize());
    }

    @Test
    void testUpdatePermissions() {
        // 创建新的权限实现（由于接口限制，我们使用内部实现）
        // 这里简化测试，主要验证方法不抛异常
        AuthenticationService.ClientPermissions currentPermissions = 
            authenticationService.getClientPermissions("test_client");

        boolean result = authenticationService.updatePermissions("test_client", currentPermissions).join();
        assertTrue(result);
    }

    @Test
    void testBlockAndUnblockClient() {
        String clientId = "test_client";
        String reason = "Test blocking";

        // 初始状态：客户端未被禁用
        assertFalse(authenticationService.isClientBlocked(clientId));

        // 禁用客户端
        boolean blockResult = authenticationService.blockClient(clientId, reason).join();
        assertTrue(blockResult);
        assertTrue(authenticationService.isClientBlocked(clientId));

        // 解禁客户端
        boolean unblockResult = authenticationService.unblockClient(clientId).join();
        assertTrue(unblockResult);
        assertFalse(authenticationService.isClientBlocked(clientId));

        // 重复解禁应该返回false
        boolean secondUnblock = authenticationService.unblockClient(clientId).join();
        assertFalse(secondUnblock);
    }

    @Test
    void testAuthenticationEventListener() {
        AtomicBoolean authenticationSuccess = new AtomicBoolean(false);
        AtomicBoolean authenticationFailure = new AtomicBoolean(false);
        AtomicBoolean clientBlocked = new AtomicBoolean(false);

        // 添加事件监听器
        AuthenticationService.AuthenticationEventListener listener = 
            new AuthenticationService.AuthenticationEventListener() {
                @Override
                public void onAuthenticationSuccess(String clientId, 
                                                  AuthenticationService.AuthenticationResult result) {
                    authenticationSuccess.set(true);
                }

                @Override
                public void onAuthenticationFailure(String clientId, 
                                                  AuthenticationService.AuthenticationResult result) {
                    authenticationFailure.set(true);
                }

                @Override
                public void onPermissionDenied(String clientId, String action, String resource) {
                    // 测试中不使用
                }

                @Override
                public void onClientBlocked(String clientId, String reason) {
                    clientBlocked.set(true);
                }

                @Override
                public void onClientUnblocked(String clientId) {
                    // 测试中不使用
                }

                @Override
                public void onCredentialsExpired(String clientId, 
                                                AuthenticationService.AuthenticationCredentials credentials) {
                    // 测试中不使用
                }
            };

        authenticationService.addAuthenticationListener(listener);

        // 成功认证，触发成功事件
        MqttConnectPacket connectPacket = createConnectPacket("admin", "admin", "admin123");
        ClientConnection connection = createTestConnection("conn_1", "127.0.0.1:12345");
        authenticationService.authenticate(connectPacket, connection).join();
        assertTrue(authenticationSuccess.get());

        // 失败认证，触发失败事件
        MqttConnectPacket badConnectPacket = createConnectPacket("admin", "admin", "wrong");
        authenticationService.authenticate(badConnectPacket, connection).join();
        assertTrue(authenticationFailure.get());

        // 禁用客户端，触发禁用事件
        authenticationService.blockClient("test_client", "Test").join();
        assertTrue(clientBlocked.get());

        // 移除监听器
        authenticationService.removeAuthenticationListener(listener);
    }

    @Test
    void testGetAuthenticationStats() {
        AuthenticationService.AuthenticationStats stats = authenticationService.getAuthenticationStats();
        assertNotNull(stats);

        // 执行一次成功认证
        MqttConnectPacket connectPacket = createConnectPacket("admin", "admin", "admin123");
        ClientConnection connection = createTestConnection("conn_1", "127.0.0.1:12345");
        authenticationService.authenticate(connectPacket, connection).join();

        // 验证统计信息更新
        assertTrue(stats.getTotalAuthentications() > 0);
        assertTrue(stats.getSuccessfulAuthentications() > 0);
        assertTrue(stats.getAuthenticationSuccessRate() > 0);
    }

    /**
     * 创建MQTT连接包
     */
    private MqttConnectPacket createConnectPacket(String clientId, String username, String password) {
        MqttConnectVariableHeader variableHeader = new MqttConnectVariableHeader(
            "MQTT", 4, new MqttConnectFlags(true, true, false, 0, false, true, 2), 60, null
        );

        MqttConnectPayload payload = new MqttConnectPayload(clientId, username, password.getBytes(), null, null);

        return new MqttConnectPacket(null, variableHeader, payload);
    }

    /**
     * 创建测试用的客户端连接
     */
    private ClientConnection createTestConnection(String connectionId, String remoteAddress) {
        return ClientConnection.builder()
                .connectionId(connectionId)
                .remoteAddress(remoteAddress)
                .connectedAt(LocalDateTime.now())
                .lastActivity(LocalDateTime.now())
                .connectionState(ClientConnection.ConnectionState.CONNECTING)
                .build();
    }
}