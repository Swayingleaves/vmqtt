/**
 * 认证管理控制器
 *
 * @author zhenglin
 * @date 2025/08/14
 */
package com.vmqtt.vmqttcore.controller;

import com.vmqtt.common.config.AuthenticationConfig;
import com.vmqtt.common.service.AuthenticationConfigManager;
import com.vmqtt.common.service.AuthenticationService;
import com.vmqtt.vmqttcore.config.AuthenticationProperties;
import lombok.Data;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

/**
 * 认证管理REST API控制器
 * 提供认证配置和用户管理功能
 */
@Slf4j
@RestController
@RequestMapping("/api/auth")
@RequiredArgsConstructor
public class AuthenticationController {
    
    private final AuthenticationConfigManager authConfigManager;
    private final AuthenticationService authService;
    
    /**
     * 获取认证配置信息
     *
     * @return 认证配置信息
     */
    @GetMapping("/config")
    public ResponseEntity<AuthConfigResponse> getAuthConfig() {
        try {
            AuthenticationConfig config = authConfigManager.getAuthenticationConfig();
            
            AuthConfigResponse response = new AuthConfigResponse();
            response.setEnabled(config.isEnabled());
            response.setMode(config.getMode().name());
            response.setAllowAnonymous(config.isAllowAnonymous());
            response.setUserCount(config.getUsers().size());
            response.setDefaultPermissions(config.getDefaultPermissions());
            response.setPasswordConfig(config.getPassword());
            response.setSessionConfig(config.getSession());
            response.setAuthorizationConfig(config.getAuthorization());
            
            return ResponseEntity.ok(response);
            
        } catch (Exception e) {
            log.error("获取认证配置失败", e);
            return ResponseEntity.internalServerError().build();
        }
    }
    
    /**
     * 更新认证启用状态
     *
     * @param request 启用状态请求
     * @return 操作结果
     */
    @PostMapping("/config/enabled")
    public ResponseEntity<Map<String, Object>> updateAuthEnabled(@RequestBody AuthEnabledRequest request) {
        try {
            AuthenticationConfig config = authConfigManager.getAuthenticationConfig();
            config.setEnabled(request.isEnabled());
            
            Map<String, Object> response = new HashMap<>();
            response.put("success", true);
            response.put("enabled", config.isEnabled());
            response.put("message", "认证状态更新成功");
            
            log.info("认证状态已更新: enabled={}", request.isEnabled());
            
            return ResponseEntity.ok(response);
            
        } catch (Exception e) {
            log.error("更新认证状态失败", e);
            
            Map<String, Object> response = new HashMap<>();
            response.put("success", false);
            response.put("message", "更新认证状态失败: " + e.getMessage());
            
            return ResponseEntity.internalServerError().body(response);
        }
    }
    
    /**
     * 更新匿名连接设置
     *
     * @param request 匿名连接请求
     * @return 操作结果
     */
    @PostMapping("/config/anonymous")
    public ResponseEntity<Map<String, Object>> updateAnonymousAllowed(@RequestBody AnonymousRequest request) {
        try {
            AuthenticationConfig config = authConfigManager.getAuthenticationConfig();
            config.setAllowAnonymous(request.isAllowAnonymous());
            
            Map<String, Object> response = new HashMap<>();
            response.put("success", true);
            response.put("allowAnonymous", config.isAllowAnonymous());
            response.put("message", "匿名连接设置更新成功");
            
            log.info("匿名连接设置已更新: allowAnonymous={}", request.isAllowAnonymous());
            
            return ResponseEntity.ok(response);
            
        } catch (Exception e) {
            log.error("更新匿名连接设置失败", e);
            
            Map<String, Object> response = new HashMap<>();
            response.put("success", false);
            response.put("message", "更新匿名连接设置失败: " + e.getMessage());
            
            return ResponseEntity.internalServerError().body(response);
        }
    }
    
    /**
     * 获取用户列表
     *
     * @return 用户列表
     */
    @GetMapping("/users")
    public ResponseEntity<List<UserInfo>> getUsers() {
        try {
            List<AuthenticationConfig.User> users = authConfigManager.getAuthenticationConfig().getUsers();
            
            List<UserInfo> userInfos = users.stream()
                .map(user -> {
                    UserInfo info = new UserInfo();
                    info.setClientId(user.getClientId());
                    info.setUsername(user.getUsername());
                    info.setRole(user.getRole());
                    info.setEnabled(user.isEnabled());
                    info.setPermissions(user.getPermissions());
                    info.setAttributes(user.getAttributes());
                    return info;
                })
                .toList();
                
            return ResponseEntity.ok(userInfos);
            
        } catch (Exception e) {
            log.error("获取用户列表失败", e);
            return ResponseEntity.internalServerError().build();
        }
    }
    
    /**
     * 添加用户
     *
     * @param request 用户创建请求
     * @return 操作结果
     */
    @PostMapping("/users")
    public ResponseEntity<Map<String, Object>> addUser(@RequestBody CreateUserRequest request) {
        try {
            AuthenticationConfig.User user = new AuthenticationConfig.User();
            user.setClientId(request.getClientId());
            user.setUsername(request.getUsername());
            user.setPassword(request.getPassword());
            user.setRole(request.getRole());
            user.setEnabled(request.isEnabled());
            
            if (request.getPermissions() != null) {
                user.setPermissions(request.getPermissions());
            }
            if (request.getAttributes() != null) {
                user.setAttributes(request.getAttributes());
            }
            
            CompletableFuture<Boolean> result = authConfigManager.addUser(user);
            boolean success = result.get();
            
            Map<String, Object> response = new HashMap<>();
            response.put("success", success);
            response.put("message", success ? "用户添加成功" : "用户添加失败");
            
            return ResponseEntity.ok(response);
            
        } catch (Exception e) {
            log.error("添加用户失败", e);
            
            Map<String, Object> response = new HashMap<>();
            response.put("success", false);
            response.put("message", "添加用户失败: " + e.getMessage());
            
            return ResponseEntity.internalServerError().body(response);
        }
    }
    
    /**
     * 删除用户
     *
     * @param clientId 客户端ID
     * @return 操作结果
     */
    @DeleteMapping("/users/{clientId}")
    public ResponseEntity<Map<String, Object>> removeUser(@PathVariable String clientId) {
        try {
            CompletableFuture<Boolean> result = authConfigManager.removeUser(clientId);
            boolean success = result.get();
            
            Map<String, Object> response = new HashMap<>();
            response.put("success", success);
            response.put("message", success ? "用户删除成功" : "用户删除失败");
            
            return ResponseEntity.ok(response);
            
        } catch (Exception e) {
            log.error("删除用户失败: clientId={}", clientId, e);
            
            Map<String, Object> response = new HashMap<>();
            response.put("success", false);
            response.put("message", "删除用户失败: " + e.getMessage());
            
            return ResponseEntity.internalServerError().body(response);
        }
    }
    
    /**
     * 获取认证统计信息
     *
     * @return 认证统计信息
     */
    @GetMapping("/stats")
    public ResponseEntity<AuthenticationService.AuthenticationStats> getAuthStats() {
        try {
            return ResponseEntity.ok(authService.getAuthenticationStats());
        } catch (Exception e) {
            log.error("获取认证统计失败", e);
            return ResponseEntity.internalServerError().build();
        }
    }
    
    /**
     * 重新加载认证配置
     *
     * @return 操作结果
     */
    @PostMapping("/reload")
    public ResponseEntity<Map<String, Object>> reloadConfig() {
        try {
            authConfigManager.reloadConfiguration().get();
            
            Map<String, Object> response = new HashMap<>();
            response.put("success", true);
            response.put("message", "认证配置重新加载成功");
            
            return ResponseEntity.ok(response);
            
        } catch (Exception e) {
            log.error("重新加载认证配置失败", e);
            
            Map<String, Object> response = new HashMap<>();
            response.put("success", false);
            response.put("message", "重新加载认证配置失败: " + e.getMessage());
            
            return ResponseEntity.internalServerError().body(response);
        }
    }
    
    // ======= 请求和响应DTO类 =======
    
    /**
     * 认证配置响应
     */
    @Data
    public static class AuthConfigResponse {
        private boolean enabled;
        private String mode;
        private boolean allowAnonymous;
        private int userCount;
        private AuthenticationConfig.UserPermissions defaultPermissions;
        private AuthenticationConfig.Password passwordConfig;
        private AuthenticationConfig.Session sessionConfig;
        private AuthenticationConfig.Authorization authorizationConfig;
    }
    
    /**
     * 认证启用请求
     */
    @Data
    public static class AuthEnabledRequest {
        private boolean enabled;
    }
    
    /**
     * 匿名连接请求
     */
    @Data
    public static class AnonymousRequest {
        private boolean allowAnonymous;
    }
    
    /**
     * 用户信息
     */
    @Data
    public static class UserInfo {
        private String clientId;
        private String username;
        private String role;
        private boolean enabled;
        private AuthenticationConfig.UserPermissions permissions;
        private Map<String, Object> attributes;
    }
    
    /**
     * 创建用户请求
     */
    @Data
    public static class CreateUserRequest {
        private String clientId;
        private String username;
        private String password;
        private String role = "user";
        private boolean enabled = true;
        private AuthenticationConfig.UserPermissions permissions;
        private Map<String, Object> attributes = new HashMap<>();
    }
}