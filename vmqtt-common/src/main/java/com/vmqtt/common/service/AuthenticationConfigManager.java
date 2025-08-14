/**
 * 认证配置管理器
 *
 * @author zhenglin
 * @date 2025/08/14
 */
package com.vmqtt.common.service;

import com.vmqtt.common.config.AuthenticationConfig;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

/**
 * 认证配置管理器
 * 负责将配置文件中的认证信息同步到认证服务
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class AuthenticationConfigManager {
    
    private final AuthenticationConfig authConfig;
    private final AuthenticationService authService;
    
    /**
     * 初始化认证配置
     */
    @PostConstruct
    public void initializeAuthentication() {
        try {
            log.info("开始初始化认证配置...");
            
            // 初始化配置默认值
            authConfig.init();
            
            // 同步用户配置到认证服务
            syncUsersToAuthService();
            
            log.info("认证配置初始化完成 - 认证启用: {}, 允许匿名: {}, 用户数量: {}", 
                authConfig.isEnabled(), authConfig.isAllowAnonymous(), authConfig.getUsers().size());
            
        } catch (Exception e) {
            log.error("认证配置初始化失败", e);
            throw new RuntimeException("认证配置初始化失败", e);
        }
    }
    
    /**
     * 同步配置文件中的用户到认证服务
     */
    private void syncUsersToAuthService() {
        log.info("开始同步用户配置到认证服务...");
        
        int syncedCount = 0;
        for (AuthenticationConfig.User userConfig : authConfig.getUsers()) {
            try {
                // 创建认证凭据
                Map<String, Object> attributes = new HashMap<>(userConfig.getAttributes());
                attributes.put("role", userConfig.getRole());
                attributes.put("configSource", "application.yml");
                
                authService.createCredentials(
                    userConfig.getClientId(),
                    userConfig.getUsername(), 
                    userConfig.getPassword(),
                    attributes
                ).get();
                
                // 创建客户端权限
                AuthenticationService.ClientPermissions permissions = createClientPermissions(userConfig);
                authService.updatePermissions(userConfig.getClientId(), permissions).get();
                
                syncedCount++;
                log.debug("用户同步成功: clientId={}, username={}, role={}", 
                    userConfig.getClientId(), userConfig.getUsername(), userConfig.getRole());
                
            } catch (Exception e) {
                log.error("同步用户配置失败: clientId={}, username={}", 
                    userConfig.getClientId(), userConfig.getUsername(), e);
            }
        }
        
        log.info("用户配置同步完成: {}/{} 成功", syncedCount, authConfig.getUsers().size());
    }
    
    /**
     * 根据配置创建客户端权限
     *
     * @param userConfig 用户配置
     * @return 客户端权限
     */
    private AuthenticationService.ClientPermissions createClientPermissions(AuthenticationConfig.User userConfig) {
        return new ConfigBasedClientPermissions(userConfig, authConfig);
    }
    
    /**
     * 重新加载认证配置
     */
    public CompletableFuture<Void> reloadConfiguration() {
        return CompletableFuture.runAsync(() -> {
            try {
                log.info("开始重新加载认证配置...");
                
                // 重新同步用户配置
                syncUsersToAuthService();
                
                log.info("认证配置重新加载完成");
                
            } catch (Exception e) {
                log.error("重新加载认证配置失败", e);
                throw new RuntimeException("重新加载认证配置失败", e);
            }
        });
    }
    
    /**
     * 添加新用户
     *
     * @param userConfig 用户配置
     * @return 操作结果
     */
    public CompletableFuture<Boolean> addUser(AuthenticationConfig.User userConfig) {
        return CompletableFuture.supplyAsync(() -> {
            try {
                log.info("添加新用户: clientId={}, username={}", userConfig.getClientId(), userConfig.getUsername());
                
                // 检查用户是否已存在
                if (authConfig.getUserByClientId(userConfig.getClientId()) != null) {
                    log.warn("用户已存在: clientId={}", userConfig.getClientId());
                    return false;
                }
                
                // 添加到配置
                authConfig.getUsers().add(userConfig);
                
                // 同步到认证服务
                Map<String, Object> attributes = new HashMap<>(userConfig.getAttributes());
                attributes.put("role", userConfig.getRole());
                attributes.put("configSource", "runtime");
                
                authService.createCredentials(
                    userConfig.getClientId(),
                    userConfig.getUsername(),
                    userConfig.getPassword(),
                    attributes
                ).get();
                
                AuthenticationService.ClientPermissions permissions = createClientPermissions(userConfig);
                authService.updatePermissions(userConfig.getClientId(), permissions).get();
                
                log.info("用户添加成功: clientId={}, username={}", userConfig.getClientId(), userConfig.getUsername());
                return true;
                
            } catch (Exception e) {
                log.error("添加用户失败: clientId={}, username={}", 
                    userConfig.getClientId(), userConfig.getUsername(), e);
                return false;
            }
        });
    }
    
    /**
     * 删除用户
     *
     * @param clientId 客户端ID
     * @return 操作结果
     */
    public CompletableFuture<Boolean> removeUser(String clientId) {
        return CompletableFuture.supplyAsync(() -> {
            try {
                log.info("删除用户: clientId={}", clientId);
                
                // 从配置中移除
                authConfig.getUsers().removeIf(user -> user.getClientId().equals(clientId));
                
                // 从认证服务中撤销凭据
                authService.revokeCredentials(clientId).get();
                
                log.info("用户删除成功: clientId={}", clientId);
                return true;
                
            } catch (Exception e) {
                log.error("删除用户失败: clientId={}", clientId, e);
                return false;
            }
        });
    }
    
    /**
     * 检查认证是否启用
     *
     * @return 认证启用状态
     */
    public boolean isAuthenticationEnabled() {
        return authConfig.isEnabled();
    }
    
    /**
     * 检查是否允许匿名连接
     *
     * @return 匿名连接允许状态
     */
    public boolean isAnonymousAllowed() {
        return authConfig.isAllowAnonymous();
    }
    
    /**
     * 获取认证配置
     *
     * @return 认证配置
     */
    public AuthenticationConfig getAuthenticationConfig() {
        return authConfig;
    }
    
    /**
     * 基于配置的客户端权限实现
     */
    private static class ConfigBasedClientPermissions implements AuthenticationService.ClientPermissions {
        
        private final AuthenticationConfig.User userConfig;
        private final AuthenticationConfig authConfig;
        
        public ConfigBasedClientPermissions(AuthenticationConfig.User userConfig, AuthenticationConfig authConfig) {
            this.userConfig = userConfig;
            this.authConfig = authConfig;
        }
        
        @Override
        public boolean canConnect() {
            return userConfig.getPermissions().isCanConnect();
        }
        
        @Override
        public boolean canPublishTo(String topic) {
            // 检查是否为管理员
            if (authConfig.isAdmin(userConfig)) {
                return true;
            }
            
            // 检查禁止列表
            for (String denyPattern : userConfig.getPermissions().getDenyPublishTopics()) {
                if (topicMatches(topic, denyPattern)) {
                    return false;
                }
            }
            
            // 检查允许列表
            for (String allowPattern : userConfig.getPermissions().getPublishTopics()) {
                if (topicMatches(topic, allowPattern)) {
                    return true;
                }
            }
            
            return false;
        }
        
        @Override
        public boolean canSubscribeTo(String topicFilter) {
            // 检查是否为管理员
            if (authConfig.isAdmin(userConfig)) {
                return true;
            }
            
            // 检查禁止列表
            for (String denyPattern : userConfig.getPermissions().getDenySubscribeTopics()) {
                if (topicMatches(topicFilter, denyPattern)) {
                    return false;
                }
            }
            
            // 检查允许列表
            for (String allowPattern : userConfig.getPermissions().getSubscribeTopics()) {
                if (topicMatches(topicFilter, allowPattern)) {
                    return true;
                }
            }
            
            return false;
        }
        
        @Override
        public int getMaxQos() {
            return userConfig.getPermissions().getMaxQos();
        }
        
        @Override
        public int getMaxMessageSize() {
            return userConfig.getPermissions().getMaxMessageSize();
        }
        
        @Override
        public int getMaxQueueSize() {
            return userConfig.getPermissions().getMaxQueueSize();
        }
        
        @Override
        public int getRateLimit() {
            return userConfig.getPermissions().getRateLimit();
        }
        
        @Override
        public int getSessionExpiryInterval() {
            return userConfig.getPermissions().getSessionExpiryInterval();
        }
        
        @Override
        public Map<String, Object> getAttributes() {
            return userConfig.getAttributes();
        }
        
        /**
         * 检查主题是否匹配模式
         *
         * @param topic 主题
         * @param pattern 模式（支持MQTT通配符 + 和 #）
         * @return 是否匹配
         */
        private boolean topicMatches(String topic, String pattern) {
            if (pattern.equals("#")) {
                return true; // # 匹配所有主题
            }
            
            String[] topicParts = topic.split("/");
            String[] patternParts = pattern.split("/");
            
            int topicIndex = 0;
            int patternIndex = 0;
            
            while (topicIndex < topicParts.length && patternIndex < patternParts.length) {
                String patternPart = patternParts[patternIndex];
                
                if (patternPart.equals("#")) {
                    return true; // # 匹配剩余所有层级
                } else if (patternPart.equals("+")) {
                    // + 匹配单个层级
                    topicIndex++;
                    patternIndex++;
                } else if (patternPart.equals(topicParts[topicIndex])) {
                    // 精确匹配
                    topicIndex++;
                    patternIndex++;
                } else {
                    return false;
                }
            }
            
            // 检查是否完全匹配
            return topicIndex == topicParts.length && patternIndex == patternParts.length;
        }
    }
}