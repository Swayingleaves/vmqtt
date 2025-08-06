/**
 * 认证服务实现类
 *
 * @author zhenglin
 * @date 2025/08/06
 */
package com.vmqtt.common.service.impl;

import com.vmqtt.common.model.ClientConnection;
import com.vmqtt.common.protocol.packet.connect.MqttConnectPacket;
import com.vmqtt.common.service.AuthenticationService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.LocalDateTime;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;

/**
 * 认证服务默认实现
 * 基于内存存储的认证和授权管理
 */
@Slf4j
@Service
public class AuthenticationServiceImpl implements AuthenticationService {

    /**
     * 认证凭据存储：clientId -> AuthenticationCredentials
     */
    private final ConcurrentHashMap<String, AuthenticationCredentialsImpl> credentials = new ConcurrentHashMap<>();
    
    /**
     * 客户端权限存储：clientId -> ClientPermissions
     */
    private final ConcurrentHashMap<String, ClientPermissionsImpl> permissions = new ConcurrentHashMap<>();
    
    /**
     * 被禁用的客户端：clientId -> 禁用原因
     */
    private final ConcurrentHashMap<String, String> blockedClients = new ConcurrentHashMap<>();
    
    /**
     * 认证事件监听器集合
     */
    private final ConcurrentLinkedQueue<AuthenticationEventListener> listeners = new ConcurrentLinkedQueue<>();
    
    /**
     * 认证统计信息
     */
    private final AuthenticationStatsImpl stats = new AuthenticationStatsImpl();

    /**
     * 默认管理员用户凭据（用于初始化）
     */
    private static final String DEFAULT_ADMIN_CLIENT_ID = "admin";
    private static final String DEFAULT_ADMIN_USERNAME = "admin";
    private static final String DEFAULT_ADMIN_PASSWORD = "admin123";

    /**
     * 初始化方法
     */
    public void init() {
        // 创建默认管理员账户
        createDefaultAdminCredentials();
        log.info("认证服务初始化完成");
    }

    @Override
    public CompletableFuture<AuthenticationResult> authenticate(MqttConnectPacket connectPacket, ClientConnection connection) {
        return CompletableFuture.supplyAsync(() -> {
            long startTime = System.currentTimeMillis();
            String clientId = connectPacket.payload() != null ? 
                connectPacket.payload().clientId() : null;
            String username = connectPacket.payload() != null ? 
                connectPacket.payload().username() : null;
            byte[] passwordBytes = connectPacket.payload() != null ? 
                connectPacket.payload().password() : null;
            String password = passwordBytes != null ? 
                new String(passwordBytes, java.nio.charset.StandardCharsets.UTF_8) : null;

            try {
                log.debug("开始认证客户端: clientId={}, username={}", clientId, username);

                // 更新统计信息
                stats.totalAuthentications.increment();

                // 1. 验证客户端ID
                if (!isValidClientId(clientId)) {
                    log.warn("无效的客户端ID: clientId={}", clientId);
                    AuthenticationResult result = createFailureResult(AuthenticationCode.IDENTIFIER_REJECTED, 
                        "Invalid client identifier");
                    stats.failedAuthentications.increment();
                    triggerAuthenticationFailure(clientId, result);
                    return result;
                }

                // 2. 检查客户端是否被禁用
                if (isClientBlocked(clientId)) {
                    String reason = blockedClients.get(clientId);
                    log.warn("客户端已被禁用: clientId={}, reason={}", clientId, reason);
                    AuthenticationResult result = createFailureResult(AuthenticationCode.CLIENT_BLOCKED, 
                        "Client is blocked: " + reason);
                    stats.failedAuthentications.increment();
                    triggerAuthenticationFailure(clientId, result);
                    return result;
                }

                // 3. 验证用户名和密码
                if (!validateCredentials(clientId, username, password)) {
                    log.warn("用户名或密码验证失败: clientId={}, username={}", clientId, username);
                    AuthenticationResult result = createFailureResult(AuthenticationCode.BAD_USERNAME_PASSWORD, 
                        "Bad username or password");
                    stats.failedAuthentications.increment();
                    triggerAuthenticationFailure(clientId, result);
                    return result;
                }

                // 4. 获取客户端权限
                ClientPermissions clientPermissions = getClientPermissions(clientId);
                if (!clientPermissions.canConnect()) {
                    log.warn("客户端无连接权限: clientId={}", clientId);
                    AuthenticationResult result = createFailureResult(AuthenticationCode.NOT_AUTHORIZED, 
                        "Not authorized to connect");
                    stats.failedAuthentications.increment();
                    triggerAuthenticationFailure(clientId, result);
                    return result;
                }

                // 5. 创建认证成功结果
                AuthenticationCredentials creds = credentials.get(clientId);
                AuthenticationResult result = createSuccessResult(clientPermissions, creds);
                
                // 更新统计信息
                stats.successfulAuthentications.increment();
                
                // 记录认证时间
                long authTime = System.currentTimeMillis() - startTime;
                stats.addAuthenticationTime(authTime);

                // 触发认证成功事件
                triggerAuthenticationSuccess(clientId, result);

                log.info("客户端认证成功: clientId={}, username={}, authTime={}ms", clientId, username, authTime);
                return result;

            } catch (Exception e) {
                log.error("认证过程异常: clientId={}, username={}", clientId, username, e);
                stats.failedAuthentications.increment();
                AuthenticationResult result = createFailureResult(AuthenticationCode.AUTHENTICATION_FAILED, 
                    "Authentication failed: " + e.getMessage());
                triggerAuthenticationFailure(clientId, result);
                return result;
            }
        });
    }

    @Override
    public CompletableFuture<Boolean> checkPublishPermission(String clientId, String topic) {
        return CompletableFuture.supplyAsync(() -> {
            try {
                ClientPermissions perms = getClientPermissions(clientId);
                boolean hasPermission = perms.canPublishTo(topic);
                
                if (!hasPermission) {
                    log.debug("发布权限检查失败: clientId={}, topic={}", clientId, topic);
                    triggerPermissionDenied(clientId, "publish", topic);
                }
                
                return hasPermission;
                
            } catch (Exception e) {
                log.error("检查发布权限异常: clientId={}, topic={}", clientId, topic, e);
                return false;
            }
        });
    }

    @Override
    public CompletableFuture<Boolean> checkSubscribePermission(String clientId, String topicFilter) {
        return CompletableFuture.supplyAsync(() -> {
            try {
                ClientPermissions perms = getClientPermissions(clientId);
                boolean hasPermission = perms.canSubscribeTo(topicFilter);
                
                if (!hasPermission) {
                    log.debug("订阅权限检查失败: clientId={}, topicFilter={}", clientId, topicFilter);
                    triggerPermissionDenied(clientId, "subscribe", topicFilter);
                }
                
                return hasPermission;
                
            } catch (Exception e) {
                log.error("检查订阅权限异常: clientId={}, topicFilter={}", clientId, topicFilter, e);
                return false;
            }
        });
    }

    @Override
    public boolean isValidClientId(String clientId) {
        // 简单验证规则
        if (clientId == null || clientId.trim().isEmpty()) {
            return false;
        }
        if (clientId.length() > 65535) {
            return false;
        }
        // 不允许包含控制字符
        return !clientId.matches(".*[\\p{Cntrl}].*");
    }

    @Override
    public ClientPermissions getClientPermissions(String clientId) {
        return permissions.computeIfAbsent(clientId, k -> createDefaultPermissions());
    }

    @Override
    public CompletableFuture<AuthenticationCredentials> createCredentials(String clientId, String username, 
                                                                         String password, Map<String, Object> attributes) {
        return CompletableFuture.supplyAsync(() -> {
            try {
                log.info("创建认证凭据: clientId={}, username={}", clientId, username);
                
                String passwordHash = hashPassword(password);
                long now = System.currentTimeMillis();
                long expiresAt = now + (365L * 24 * 60 * 60 * 1000); // 1年过期
                
                AuthenticationCredentialsImpl creds = new AuthenticationCredentialsImpl(
                    clientId, username, passwordHash, now, expiresAt, attributes != null ? attributes : new HashMap<>()
                );
                
                credentials.put(clientId, creds);
                stats.activeCredentials.increment();
                
                log.info("认证凭据创建成功: clientId={}, username={}", clientId, username);
                return creds;
                
            } catch (Exception e) {
                log.error("创建认证凭据失败: clientId={}, username={}", clientId, username, e);
                throw new RuntimeException("创建认证凭据失败", e);
            }
        });
    }

    @Override
    public CompletableFuture<Boolean> revokeCredentials(String clientId) {
        return CompletableFuture.supplyAsync(() -> {
            try {
                log.info("撤销认证凭据: clientId={}", clientId);
                
                AuthenticationCredentialsImpl removed = credentials.remove(clientId);
                if (removed != null) {
                    stats.activeCredentials.decrement();
                    log.info("认证凭据撤销成功: clientId={}", clientId);
                    return true;
                } else {
                    log.warn("认证凭据不存在: clientId={}", clientId);
                    return false;
                }
                
            } catch (Exception e) {
                log.error("撤销认证凭据失败: clientId={}", clientId, e);
                return false;
            }
        });
    }

    @Override
    public CompletableFuture<Boolean> updatePermissions(String clientId, ClientPermissions permissions) {
        return CompletableFuture.supplyAsync(() -> {
            try {
                log.info("更新客户端权限: clientId={}", clientId);
                
                if (permissions instanceof ClientPermissionsImpl) {
                    this.permissions.put(clientId, (ClientPermissionsImpl) permissions);
                } else {
                    // 转换为内部实现
                    ClientPermissionsImpl impl = new ClientPermissionsImpl();
                    impl.maxQos = permissions.getMaxQos();
                    impl.maxMessageSize = permissions.getMaxMessageSize();
                    impl.maxQueueSize = permissions.getMaxQueueSize();
                    impl.rateLimit = permissions.getRateLimit();
                    impl.sessionExpiryInterval = permissions.getSessionExpiryInterval();
                    impl.attributes.putAll(permissions.getAttributes());
                    this.permissions.put(clientId, impl);
                }
                
                log.info("客户端权限更新成功: clientId={}", clientId);
                return true;
                
            } catch (Exception e) {
                log.error("更新客户端权限失败: clientId={}", clientId, e);
                return false;
            }
        });
    }

    @Override
    public boolean isClientBlocked(String clientId) {
        return blockedClients.containsKey(clientId);
    }

    @Override
    public CompletableFuture<Boolean> blockClient(String clientId, String reason) {
        return CompletableFuture.supplyAsync(() -> {
            try {
                log.info("禁用客户端: clientId={}, reason={}", clientId, reason);
                
                blockedClients.put(clientId, reason);
                stats.blockedClients.increment();
                
                // 触发客户端禁用事件
                triggerClientBlocked(clientId, reason);
                
                log.info("客户端禁用成功: clientId={}, reason={}", clientId, reason);
                return true;
                
            } catch (Exception e) {
                log.error("禁用客户端失败: clientId={}, reason={}", clientId, reason, e);
                return false;
            }
        });
    }

    @Override
    public CompletableFuture<Boolean> unblockClient(String clientId) {
        return CompletableFuture.supplyAsync(() -> {
            try {
                log.info("解禁客户端: clientId={}", clientId);
                
                String removed = blockedClients.remove(clientId);
                if (removed != null) {
                    stats.blockedClients.decrement();
                    
                    // 触发客户端解禁事件
                    triggerClientUnblocked(clientId);
                    
                    log.info("客户端解禁成功: clientId={}", clientId);
                    return true;
                } else {
                    log.warn("客户端未被禁用: clientId={}", clientId);
                    return false;
                }
                
            } catch (Exception e) {
                log.error("解禁客户端失败: clientId={}", clientId, e);
                return false;
            }
        });
    }

    @Override
    public AuthenticationStats getAuthenticationStats() {
        return stats;
    }

    @Override
    public void addAuthenticationListener(AuthenticationEventListener listener) {
        listeners.add(listener);
        log.debug("添加认证事件监听器: {}", listener.getClass().getSimpleName());
    }

    @Override
    public void removeAuthenticationListener(AuthenticationEventListener listener) {
        listeners.remove(listener);
        log.debug("移除认证事件监听器: {}", listener.getClass().getSimpleName());
    }

    /**
     * 创建默认管理员凭据
     */
    private void createDefaultAdminCredentials() {
        Map<String, Object> adminAttributes = new HashMap<>();
        adminAttributes.put("role", "admin");
        adminAttributes.put("description", "Default administrator account");
        
        createCredentials(DEFAULT_ADMIN_CLIENT_ID, DEFAULT_ADMIN_USERNAME, 
            DEFAULT_ADMIN_PASSWORD, adminAttributes).join();
        
        // 创建管理员权限
        ClientPermissionsImpl adminPerms = new ClientPermissionsImpl();
        adminPerms.canConnect = true;
        adminPerms.maxQos = 2;
        adminPerms.maxMessageSize = 10 * 1024 * 1024; // 10MB
        adminPerms.maxQueueSize = 10000;
        adminPerms.rateLimit = 1000;
        adminPerms.sessionExpiryInterval = 24 * 60 * 60; // 24小时
        
        permissions.put(DEFAULT_ADMIN_CLIENT_ID, adminPerms);
        
        log.info("默认管理员账户创建完成: clientId={}, username={}", 
            DEFAULT_ADMIN_CLIENT_ID, DEFAULT_ADMIN_USERNAME);
    }

    /**
     * 验证用户凭据
     *
     * @param clientId 客户端ID
     * @param username 用户名
     * @param password 密码
     * @return 验证结果
     */
    private boolean validateCredentials(String clientId, String username, String password) {
        AuthenticationCredentialsImpl creds = credentials.get(clientId);
        if (creds == null) {
            return false;
        }
        
        // 检查用户名
        if (username == null || !username.equals(creds.getUsername())) {
            return false;
        }
        
        // 检查密码
        if (password == null) {
            return false;
        }
        
        String passwordHash = hashPassword(password);
        if (!passwordHash.equals(creds.getPasswordHash())) {
            return false;
        }
        
        // 检查凭据是否过期
        if (creds.isExpired()) {
            log.warn("认证凭据已过期: clientId={}", clientId);
            triggerCredentialsExpired(clientId, creds);
            return false;
        }
        
        return true;
    }

    /**
     * 哈希密码
     *
     * @param password 原始密码
     * @return 哈希后的密码
     */
    private String hashPassword(String password) {
        try {
            MessageDigest md = MessageDigest.getInstance("SHA-256");
            byte[] hash = md.digest(password.getBytes(StandardCharsets.UTF_8));
            return Base64.getEncoder().encodeToString(hash);
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException("SHA-256 algorithm not available", e);
        }
    }

    /**
     * 创建默认权限
     *
     * @return 默认权限
     */
    private ClientPermissionsImpl createDefaultPermissions() {
        ClientPermissionsImpl perms = new ClientPermissionsImpl();
        perms.canConnect = true;
        perms.maxQos = 1;
        perms.maxMessageSize = 1024 * 1024; // 1MB
        perms.maxQueueSize = 1000;
        perms.rateLimit = 100;
        perms.sessionExpiryInterval = 60 * 60; // 1小时
        return perms;
    }

    /**
     * 创建认证成功结果
     */
    private AuthenticationResult createSuccessResult(ClientPermissions permissions, AuthenticationCredentials credentials) {
        return new AuthenticationResultImpl(true, null, AuthenticationCode.CONNECTION_ACCEPTED, 
            permissions, credentials, permissions.getSessionExpiryInterval());
    }

    /**
     * 创建认证失败结果
     */
    private AuthenticationResult createFailureResult(AuthenticationCode code, String reason) {
        return new AuthenticationResultImpl(false, reason, code, null, null, 0);
    }

    // 事件触发方法
    private void triggerAuthenticationSuccess(String clientId, AuthenticationResult result) {
        listeners.forEach(listener -> {
            try {
                listener.onAuthenticationSuccess(clientId, result);
            } catch (Exception e) {
                log.warn("认证事件监听器异常", e);
            }
        });
    }

    private void triggerAuthenticationFailure(String clientId, AuthenticationResult result) {
        listeners.forEach(listener -> {
            try {
                listener.onAuthenticationFailure(clientId, result);
            } catch (Exception e) {
                log.warn("认证事件监听器异常", e);
            }
        });
    }

    private void triggerPermissionDenied(String clientId, String action, String resource) {
        listeners.forEach(listener -> {
            try {
                listener.onPermissionDenied(clientId, action, resource);
            } catch (Exception e) {
                log.warn("认证事件监听器异常", e);
            }
        });
    }

    private void triggerClientBlocked(String clientId, String reason) {
        listeners.forEach(listener -> {
            try {
                listener.onClientBlocked(clientId, reason);
            } catch (Exception e) {
                log.warn("认证事件监听器异常", e);
            }
        });
    }

    private void triggerClientUnblocked(String clientId) {
        listeners.forEach(listener -> {
            try {
                listener.onClientUnblocked(clientId);
            } catch (Exception e) {
                log.warn("认证事件监听器异常", e);
            }
        });
    }

    private void triggerCredentialsExpired(String clientId, AuthenticationCredentials credentials) {
        listeners.forEach(listener -> {
            try {
                listener.onCredentialsExpired(clientId, credentials);
            } catch (Exception e) {
                log.warn("认证事件监听器异常", e);
            }
        });
    }

    /**
     * 认证结果实现
     */
    private static class AuthenticationResultImpl implements AuthenticationResult {
        private final boolean success;
        private final String failureReason;
        private final AuthenticationCode returnCode;
        private final ClientPermissions permissions;
        private final AuthenticationCredentials credentials;
        private final int sessionExpiryInterval;

        public AuthenticationResultImpl(boolean success, String failureReason, AuthenticationCode returnCode,
                                      ClientPermissions permissions, AuthenticationCredentials credentials,
                                      int sessionExpiryInterval) {
            this.success = success;
            this.failureReason = failureReason;
            this.returnCode = returnCode;
            this.permissions = permissions;
            this.credentials = credentials;
            this.sessionExpiryInterval = sessionExpiryInterval;
        }

        @Override
        public boolean isSuccess() {
            return success;
        }

        @Override
        public String getFailureReason() {
            return failureReason;
        }

        @Override
        public AuthenticationCode getReturnCode() {
            return returnCode;
        }

        @Override
        public ClientPermissions getPermissions() {
            return permissions;
        }

        @Override
        public AuthenticationCredentials getCredentials() {
            return credentials;
        }

        @Override
        public int getSessionExpiryInterval() {
            return sessionExpiryInterval;
        }
    }

    /**
     * 客户端权限实现
     */
    private static class ClientPermissionsImpl implements ClientPermissions {
        private boolean canConnect = true;
        private int maxQos = 1;
        private int maxMessageSize = 1024 * 1024; // 1MB
        private int maxQueueSize = 1000;
        private int rateLimit = 100;
        private int sessionExpiryInterval = 60 * 60; // 1小时
        private Map<String, Object> attributes = new ConcurrentHashMap<>();

        @Override
        public boolean canConnect() {
            return canConnect;
        }

        @Override
        public boolean canPublishTo(String topic) {
            // 简单实现：检查是否是系统主题
            if (topic.startsWith("$SYS/")) {
                return attributes.containsKey("role") && "admin".equals(attributes.get("role"));
            }
            return true;
        }

        @Override
        public boolean canSubscribeTo(String topicFilter) {
            // 简单实现：检查是否是系统主题
            if (topicFilter.startsWith("$SYS/")) {
                return attributes.containsKey("role") && "admin".equals(attributes.get("role"));
            }
            return true;
        }

        @Override
        public int getMaxQos() {
            return maxQos;
        }

        @Override
        public int getMaxMessageSize() {
            return maxMessageSize;
        }

        @Override
        public int getMaxQueueSize() {
            return maxQueueSize;
        }

        @Override
        public int getRateLimit() {
            return rateLimit;
        }

        @Override
        public int getSessionExpiryInterval() {
            return sessionExpiryInterval;
        }

        @Override
        public Map<String, Object> getAttributes() {
            return attributes;
        }
    }

    /**
     * 认证凭据实现
     */
    private static class AuthenticationCredentialsImpl implements AuthenticationCredentials {
        private final String clientId;
        private final String username;
        private final String passwordHash;
        private final long createdAt;
        private final long expiresAt;
        private final Map<String, Object> attributes;

        public AuthenticationCredentialsImpl(String clientId, String username, String passwordHash,
                                           long createdAt, long expiresAt, Map<String, Object> attributes) {
            this.clientId = clientId;
            this.username = username;
            this.passwordHash = passwordHash;
            this.createdAt = createdAt;
            this.expiresAt = expiresAt;
            this.attributes = new ConcurrentHashMap<>(attributes);
        }

        @Override
        public String getClientId() {
            return clientId;
        }

        @Override
        public String getUsername() {
            return username;
        }

        @Override
        public String getPasswordHash() {
            return passwordHash;
        }

        @Override
        public long getCreatedAt() {
            return createdAt;
        }

        @Override
        public long getExpiresAt() {
            return expiresAt;
        }

        @Override
        public boolean isExpired() {
            return System.currentTimeMillis() > expiresAt;
        }

        @Override
        public Map<String, Object> getAttributes() {
            return attributes;
        }
    }

    /**
     * 认证统计信息实现
     */
    private static class AuthenticationStatsImpl implements AuthenticationStats {
        private final LongAdder totalAuthentications = new LongAdder();
        private final LongAdder successfulAuthentications = new LongAdder();
        private final LongAdder failedAuthentications = new LongAdder();
        private final LongAdder blockedClients = new LongAdder();
        private final LongAdder activeCredentials = new LongAdder();
        private final LongAdder expiredCredentials = new LongAdder();
        private final AtomicLong totalAuthTime = new AtomicLong(0);

        public void addAuthenticationTime(long time) {
            totalAuthTime.addAndGet(time);
        }

        @Override
        public long getTotalAuthentications() {
            return totalAuthentications.sum();
        }

        @Override
        public long getSuccessfulAuthentications() {
            return successfulAuthentications.sum();
        }

        @Override
        public long getFailedAuthentications() {
            return failedAuthentications.sum();
        }

        @Override
        public long getBlockedClients() {
            return blockedClients.sum();
        }

        @Override
        public long getActiveCredentials() {
            return activeCredentials.sum();
        }

        @Override
        public long getExpiredCredentials() {
            return expiredCredentials.sum();
        }

        @Override
        public double getAuthenticationSuccessRate() {
            long total = getTotalAuthentications();
            return total > 0 ? (double) getSuccessfulAuthentications() / total : 0.0;
        }

        @Override
        public double getAverageAuthenticationTime() {
            long total = getTotalAuthentications();
            return total > 0 ? (double) totalAuthTime.get() / total : 0.0;
        }
    }
}