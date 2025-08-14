/**
 * 认证配置类
 *
 * @author zhenglin
 * @date 2025/08/14
 */
package com.vmqtt.common.config;

import lombok.Data;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * V-MQTT认证配置
 * 支持认证开关控制和用户配置
 */
@Data
public class AuthenticationConfig {
    
    /**
     * 认证是否启用
     */
    private boolean enabled = true;
    
    /**
     * 认证模式
     */
    private AuthMode mode = AuthMode.MEMORY;
    
    /**
     * 是否允许匿名连接
     */
    private boolean allowAnonymous = false;
    
    /**
     * 默认用户权限
     */
    private UserPermissions defaultPermissions = new UserPermissions();
    
    /**
     * 预配置的用户列表
     */
    private List<User> users = new ArrayList<>();
    
    /**
     * 密码加密配置
     */
    private Password password = new Password();
    
    /**
     * 会话配置
     */
    private Session session = new Session();
    
    /**
     * 权限控制配置
     */
    private Authorization authorization = new Authorization();
    
    /**
     * 认证模式枚举
     */
    public enum AuthMode {
        /**
         * 内存模式 - 基于配置文件和内存存储
         */
        MEMORY,
        
        /**
         * 数据库模式 - 基于数据库存储
         */
        DATABASE,
        
        /**
         * 外部认证 - 集成外部认证系统
         */
        EXTERNAL
    }
    
    /**
     * 用户配置
     */
    @Data
    public static class User {
        /**
         * 客户端ID
         */
        private String clientId;
        
        /**
         * 用户名
         */
        private String username;
        
        /**
         * 密码（明文，启动时会自动哈希）
         */
        private String password;
        
        /**
         * 用户角色
         */
        private String role = "user";
        
        /**
         * 用户权限
         */
        private UserPermissions permissions = new UserPermissions();
        
        /**
         * 用户属性
         */
        private Map<String, Object> attributes = new HashMap<>();
        
        /**
         * 是否启用
         */
        private boolean enabled = true;
    }
    
    /**
     * 用户权限配置
     */
    @Data
    public static class UserPermissions {
        /**
         * 是否允许连接
         */
        private boolean canConnect = true;
        
        /**
         * 最大QoS级别
         */
        private int maxQos = 2;
        
        /**
         * 最大消息大小（字节）
         */
        private int maxMessageSize = 1024 * 1024; // 1MB
        
        /**
         * 最大队列大小
         */
        private int maxQueueSize = 1000;
        
        /**
         * 速率限制（消息/秒）
         */
        private int rateLimit = 100;
        
        /**
         * 会话过期间隔（秒）
         */
        private int sessionExpiryInterval = 3600; // 1小时
        
        /**
         * 允许发布的主题模式列表
         */
        private List<String> publishTopics = new ArrayList<>();
        
        /**
         * 允许订阅的主题模式列表
         */
        private List<String> subscribeTopics = new ArrayList<>();
        
        /**
         * 禁止发布的主题模式列表
         */
        private List<String> denyPublishTopics = new ArrayList<>();
        
        /**
         * 禁止订阅的主题模式列表
         */
        private List<String> denySubscribeTopics = new ArrayList<>();
    }
    
    /**
     * 密码配置
     */
    @Data
    public static class Password {
        /**
         * 密码哈希算法
         */
        private String algorithm = "SHA-256";
        
        /**
         * 是否使用盐值
         */
        private boolean useSalt = true;
        
        /**
         * 盐值长度
         */
        private int saltLength = 16;
        
        /**
         * 密码最小长度
         */
        private int minLength = 6;
        
        /**
         * 密码过期时间（天）
         */
        private int expiryDays = 365;
    }
    
    /**
     * 会话配置
     */
    @Data
    public static class Session {
        /**
         * 默认会话过期时间（秒）
         */
        private int defaultExpiryInterval = 3600; // 1小时
        
        /**
         * 最大会话过期时间（秒）
         */
        private int maxExpiryInterval = 86400; // 24小时
        
        /**
         * 是否允许重复连接
         */
        private boolean allowDuplicateConnections = false;
        
        /**
         * 会话清理间隔（秒）
         */
        private int cleanupInterval = 300; // 5分钟
    }
    
    /**
     * 权限控制配置
     */
    @Data
    public static class Authorization {
        /**
         * 是否启用主题权限检查
         */
        private boolean topicPermissionEnabled = true;
        
        /**
         * 是否启用QoS权限检查
         */
        private boolean qosPermissionEnabled = true;
        
        /**
         * 是否启用速率限制
         */
        private boolean rateLimitEnabled = true;
        
        /**
         * 权限缓存时间（秒）
         */
        private int permissionCacheTime = 300; // 5分钟
        
        /**
         * 系统主题前缀（需要管理员权限）
         */
        private List<String> systemTopicPrefixes = List.of("$SYS/");
        
        /**
         * 管理员角色列表
         */
        private List<String> adminRoles = List.of("admin", "root");
    }
    
    /**
     * 初始化方法 - 设置默认值
     */
    public void init() {
        // 如果没有配置用户，添加默认管理员
        if (users.isEmpty()) {
            User adminUser = new User();
            adminUser.setClientId("admin");
            adminUser.setUsername("admin");
            adminUser.setPassword("admin123");
            adminUser.setRole("admin");
            
            // 管理员权限
            UserPermissions adminPerms = new UserPermissions();
            adminPerms.setMaxQos(2);
            adminPerms.setMaxMessageSize(10 * 1024 * 1024); // 10MB
            adminPerms.setMaxQueueSize(10000);
            adminPerms.setRateLimit(1000);
            adminPerms.setSessionExpiryInterval(24 * 60 * 60); // 24小时
            adminPerms.setPublishTopics(List.of("#")); // 允许发布到所有主题
            adminPerms.setSubscribeTopics(List.of("#")); // 允许订阅所有主题
            
            adminUser.setPermissions(adminPerms);
            adminUser.getAttributes().put("role", "admin");
            adminUser.getAttributes().put("description", "默认管理员账户");
            
            users.add(adminUser);
        }
        
        // 设置默认权限
        if (defaultPermissions.getPublishTopics().isEmpty()) {
            defaultPermissions.setPublishTopics(List.of("user/+/data", "device/+/status"));
        }
        if (defaultPermissions.getSubscribeTopics().isEmpty()) {
            defaultPermissions.setSubscribeTopics(List.of("user/+/data", "device/+/status", "broadcast/+"));
        }
        if (defaultPermissions.getDenyPublishTopics().isEmpty()) {
            defaultPermissions.setDenyPublishTopics(List.of("$SYS/+"));
        }
        if (defaultPermissions.getDenySubscribeTopics().isEmpty()) {
            defaultPermissions.setDenySubscribeTopics(List.of("$SYS/+"));
        }
    }
    
    /**
     * 检查认证是否启用
     *
     * @return 认证启用状态
     */
    public boolean isAuthenticationEnabled() {
        return enabled;
    }
    
    /**
     * 检查是否允许匿名连接
     *
     * @return 匿名连接允许状态
     */
    public boolean isAnonymousAllowed() {
        return allowAnonymous;
    }
    
    /**
     * 根据客户端ID获取用户配置
     *
     * @param clientId 客户端ID
     * @return 用户配置，如果不存在返回null
     */
    public User getUserByClientId(String clientId) {
        return users.stream()
                .filter(user -> user.getClientId().equals(clientId) && user.isEnabled())
                .findFirst()
                .orElse(null);
    }
    
    /**
     * 根据用户名获取用户配置
     *
     * @param username 用户名
     * @return 用户配置，如果不存在返回null
     */
    public User getUserByUsername(String username) {
        return users.stream()
                .filter(user -> user.getUsername().equals(username) && user.isEnabled())
                .findFirst()
                .orElse(null);
    }
    
    /**
     * 检查用户是否为管理员
     *
     * @param user 用户配置
     * @return 是否为管理员
     */
    public boolean isAdmin(User user) {
        if (user == null) {
            return false;
        }
        return authorization.getAdminRoles().contains(user.getRole());
    }
}