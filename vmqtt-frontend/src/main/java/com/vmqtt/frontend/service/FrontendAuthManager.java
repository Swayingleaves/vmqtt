/**
 * 前端认证管理器
 *
 * @author zhenglin
 * @date 2025/08/08
 */
package com.vmqtt.frontend.service;

import com.vmqtt.common.service.AuthenticationService;
import com.vmqtt.frontend.config.NettyServerConfig;
import io.netty.channel.Channel;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CompletableFuture;

/**
 * 前端认证管理服务
 * 处理客户端认证和会话管理
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class FrontendAuthManager {
    
    private final AuthenticationService authService;
    private final NettyServerConfig config;
    
    // 缓存已认证的会话
    private final Map<String, AuthenticatedSession> authenticatedSessions = new ConcurrentHashMap<>();
    
    /**
     * 异步认证客户端
     *
     * @param channel 网络通道
     * @param clientId 客户端ID
     * @param username 用户名
     * @param password 密码
     * @return 认证结果
     */
    public CompletableFuture<AuthenticationResult> authenticateAsync(
            Channel channel, String clientId, String username, String password) {
        
        return CompletableFuture.supplyAsync(() -> {
            try {
                log.debug("开始认证客户端: clientId={}, username={}", clientId, username);
                
                // 1. 基础参数验证
                if (clientId == null || clientId.trim().isEmpty()) {
                    return AuthenticationResult.failure("客户端ID不能为空");
                }
                
                if (clientId.length() > config.getConnection().getMaxClientIdLength()) {
                    return AuthenticationResult.failure("客户端ID长度超过限制");
                }
                
                // 2. 调用认证服务
                boolean authenticated = authService.authenticate(clientId, username, password);
                if (!authenticated) {
                    log.warn("客户端认证失败: clientId={}, username={}", clientId, username);
                    return AuthenticationResult.failure("用户名或密码错误");
                }
                
                // 3. 检查是否已有活跃会话
                AuthenticatedSession existingSession = authenticatedSessions.get(clientId);
                if (existingSession != null && existingSession.isActive()) {
                    log.info("客户端已有活跃会话，将断开旧会话: clientId={}", clientId);
                    // 标记旧会话为无效
                    existingSession.invalidate();
                }
                
                // 4. 创建新的认证会话
                AuthenticatedSession newSession = AuthenticatedSession.builder()
                    .clientId(clientId)
                    .username(username)
                    .channel(channel)
                    .authenticatedAt(System.currentTimeMillis())
                    .lastActivityAt(System.currentTimeMillis())
                    .active(true)
                    .build();
                
                authenticatedSessions.put(clientId, newSession);
                
                log.info("客户端认证成功: clientId={}, username={}", clientId, username);
                return AuthenticationResult.success(newSession);
                
            } catch (Exception e) {
                log.error("认证过程中发生异常: clientId={}", clientId, e);
                return AuthenticationResult.failure("认证服务不可用");
            }
        });
    }
    
    /**
     * 检查客户端授权
     *
     * @param clientId 客户端ID
     * @param operation 操作类型
     * @param resource 资源（如主题）
     * @return 是否有权限
     */
    public CompletableFuture<Boolean> checkAuthorizationAsync(String clientId, String operation, String resource) {
        return CompletableFuture.supplyAsync(() -> {
            try {
                // 1. 检查会话是否存在且有效
                AuthenticatedSession session = authenticatedSessions.get(clientId);
                if (session == null || !session.isActive()) {
                    log.warn("客户端未认证或会话已失效: clientId={}", clientId);
                    return false;
                }
                
                // 2. 更新会话活动时间
                session.updateActivity();
                
                // 3. 调用授权服务
                return authService.authorize(clientId, operation, resource);
                
            } catch (Exception e) {
                log.error("授权检查过程中发生异常: clientId={}, operation={}, resource={}", 
                    clientId, operation, resource, e);
                return false;
            }
        });
    }
    
    /**
     * 获取认证会话
     *
     * @param clientId 客户端ID
     * @return 认证会话，如果不存在则返回null
     */
    public AuthenticatedSession getAuthenticatedSession(String clientId) {
        AuthenticatedSession session = authenticatedSessions.get(clientId);
        if (session != null && session.isActive()) {
            session.updateActivity();
            return session;
        }
        return null;
    }
    
    /**
     * 移除认证会话
     *
     * @param clientId 客户端ID
     */
    public void removeAuthenticatedSession(String clientId) {
        AuthenticatedSession session = authenticatedSessions.remove(clientId);
        if (session != null) {
            session.invalidate();
            log.debug("移除认证会话: clientId={}", clientId);
        }
    }
    
    /**
     * 清理过期会话
     *
     * @return 清理的会话数量
     */
    public int cleanupExpiredSessions() {
        long currentTime = System.currentTimeMillis();
        long sessionTimeout = config.getConnection().getTimeout() * 1000L; // 转换为毫秒
        
        int cleanupCount = 0;
        
        for (Map.Entry<String, AuthenticatedSession> entry : authenticatedSessions.entrySet()) {
            AuthenticatedSession session = entry.getValue();
            
            if (!session.isActive() || (currentTime - session.getLastActivityAt()) > sessionTimeout) {
                authenticatedSessions.remove(entry.getKey());
                session.invalidate();
                cleanupCount++;
                
                log.debug("清理过期会话: clientId={}", entry.getKey());
            }
        }
        
        if (cleanupCount > 0) {
            log.info("清理过期认证会话: count={}", cleanupCount);
        }
        
        return cleanupCount;
    }
    
    /**
     * 获取认证统计信息
     *
     * @return 统计信息
     */
    public AuthStats getAuthStats() {
        long activeSessionCount = authenticatedSessions.values().stream()
            .mapToLong(session -> session.isActive() ? 1 : 0)
            .sum();
        
        return AuthStats.builder()
            .totalSessions(authenticatedSessions.size())
            .activeSessions((int) activeSessionCount)
            .build();
    }
    
    /**
     * 认证结果
     */
    public static class AuthenticationResult {
        private final boolean success;
        private final String message;
        private final AuthenticatedSession session;
        
        private AuthenticationResult(boolean success, String message, AuthenticatedSession session) {
            this.success = success;
            this.message = message;
            this.session = session;
        }
        
        public static AuthenticationResult success(AuthenticatedSession session) {
            return new AuthenticationResult(true, "认证成功", session);
        }
        
        public static AuthenticationResult failure(String message) {
            return new AuthenticationResult(false, message, null);
        }
        
        public boolean isSuccess() { return success; }
        public String getMessage() { return message; }
        public AuthenticatedSession getSession() { return session; }
    }
    
    /**
     * 认证会话
     */
    public static class AuthenticatedSession {
        private String clientId;
        private String username;
        private Channel channel;
        private long authenticatedAt;
        private long lastActivityAt;
        private boolean active;
        
        public static AuthenticatedSessionBuilder builder() {
            return new AuthenticatedSessionBuilder();
        }
        
        public void updateActivity() {
            this.lastActivityAt = System.currentTimeMillis();
        }
        
        public void invalidate() {
            this.active = false;
        }
        
        public static class AuthenticatedSessionBuilder {
            private String clientId;
            private String username;
            private Channel channel;
            private long authenticatedAt;
            private long lastActivityAt;
            private boolean active;
            
            public AuthenticatedSessionBuilder clientId(String clientId) {
                this.clientId = clientId;
                return this;
            }
            
            public AuthenticatedSessionBuilder username(String username) {
                this.username = username;
                return this;
            }
            
            public AuthenticatedSessionBuilder channel(Channel channel) {
                this.channel = channel;
                return this;
            }
            
            public AuthenticatedSessionBuilder authenticatedAt(long authenticatedAt) {
                this.authenticatedAt = authenticatedAt;
                return this;
            }
            
            public AuthenticatedSessionBuilder lastActivityAt(long lastActivityAt) {
                this.lastActivityAt = lastActivityAt;
                return this;
            }
            
            public AuthenticatedSessionBuilder active(boolean active) {
                this.active = active;
                return this;
            }
            
            public AuthenticatedSession build() {
                AuthenticatedSession session = new AuthenticatedSession();
                session.clientId = this.clientId;
                session.username = this.username;
                session.channel = this.channel;
                session.authenticatedAt = this.authenticatedAt;
                session.lastActivityAt = this.lastActivityAt;
                session.active = this.active;
                return session;
            }
        }
        
        // Getters
        public String getClientId() { return clientId; }
        public String getUsername() { return username; }
        public Channel getChannel() { return channel; }
        public long getAuthenticatedAt() { return authenticatedAt; }
        public long getLastActivityAt() { return lastActivityAt; }
        public boolean isActive() { return active; }
    }
    
    /**
     * 认证统计信息
     */
    public static class AuthStats {
        private int totalSessions;
        private int activeSessions;
        
        public static AuthStatsBuilder builder() {
            return new AuthStatsBuilder();
        }
        
        public static class AuthStatsBuilder {
            private int totalSessions;
            private int activeSessions;
            
            public AuthStatsBuilder totalSessions(int totalSessions) {
                this.totalSessions = totalSessions;
                return this;
            }
            
            public AuthStatsBuilder activeSessions(int activeSessions) {
                this.activeSessions = activeSessions;
                return this;
            }
            
            public AuthStats build() {
                AuthStats stats = new AuthStats();
                stats.totalSessions = this.totalSessions;
                stats.activeSessions = this.activeSessions;
                return stats;
            }
        }
        
        public int getTotalSessions() { return totalSessions; }
        public int getActiveSessions() { return activeSessions; }
    }
}