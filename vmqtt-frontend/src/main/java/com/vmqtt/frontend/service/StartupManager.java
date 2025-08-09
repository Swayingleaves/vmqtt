/**
 * 启动管理器
 *
 * @author zhenglin
 * @date 2025/08/09
 */
package com.vmqtt.frontend.service;

import com.vmqtt.common.service.ConnectionManager;
import com.vmqtt.common.service.AuthenticationService;
import com.vmqtt.common.service.MessageRouter;
import com.vmqtt.common.service.SessionManager;
import com.vmqtt.frontend.config.NettyServerConfig;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Service;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

/**
 * 前端服务启动管理器
 * 负责检查依赖服务状态和启动顺序控制
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class StartupManager {
    
    private final NettyServerConfig config;
    private final ConnectionManager connectionManager;
    private final AuthenticationService authService;
    private final MessageRouter messageRouter;
    private final SessionManager sessionManager;
    
    private volatile boolean servicesReady = false;
    
    /**
     * 应用启动完成后检查服务依赖
     */
    @EventListener(ApplicationReadyEvent.class)
    public void onApplicationReady() {
        log.info("应用启动完成，开始检查服务依赖...");
        
        CompletableFuture.runAsync(() -> {
            try {
                checkServiceDependencies();
                servicesReady = true;
                log.info("所有服务依赖检查完成，前端服务已就绪");
            } catch (Exception e) {
                log.error("服务依赖检查失败", e);
                servicesReady = false;
            }
        });
    }
    
    /**
     * 检查所有服务依赖
     */
    private void checkServiceDependencies() throws Exception {
        log.info("开始检查服务依赖状态...");
        
        // 1. 检查连接管理服务
        checkServiceWithRetry("连接管理服务", () -> {
            try {
                // 尝试调用服务方法验证可用性
                return connectionManager != null;
            } catch (Exception e) {
                log.debug("连接管理服务检查失败: {}", e.getMessage());
                return false;
            }
        });
        
        // 2. 检查认证服务
        checkServiceWithRetry("认证服务", () -> {
            try {
                return authService != null;
            } catch (Exception e) {
                log.debug("认证服务检查失败: {}", e.getMessage());
                return false;
            }
        });
        
        // 3. 检查消息路由服务
        checkServiceWithRetry("消息路由服务", () -> {
            try {
                return messageRouter != null;
            } catch (Exception e) {
                log.debug("消息路由服务检查失败: {}", e.getMessage());
                return false;
            }
        });
        
        // 4. 检查会话管理服务
        checkServiceWithRetry("会话管理服务", () -> {
            try {
                return sessionManager != null;
            } catch (Exception e) {
                log.debug("会话管理服务检查失败: {}", e.getMessage());
                return false;
            }
        });
        
        log.info("所有服务依赖检查通过");
    }
    
    /**
     * 带重试的服务检查
     *
     * @param serviceName 服务名称
     * @param healthCheck 健康检查函数
     * @throws Exception 检查失败异常
     */
    private void checkServiceWithRetry(String serviceName, ServiceHealthCheck healthCheck) throws Exception {
        int maxRetries = 10;
        int retryInterval = 2; // 秒
        
        for (int i = 0; i < maxRetries; i++) {
            try {
                if (healthCheck.check()) {
                    log.info("{}检查通过", serviceName);
                    return;
                }
            } catch (Exception e) {
                log.debug("{}检查异常: {}", serviceName, e.getMessage());
            }
            
            if (i < maxRetries - 1) {
                log.info("{}检查失败，{}秒后重试 ({}/{})", serviceName, retryInterval, i + 1, maxRetries);
                try {
                    TimeUnit.SECONDS.sleep(retryInterval);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new RuntimeException(serviceName + "检查被中断", e);
                }
            }
        }
        
        throw new RuntimeException(serviceName + "在" + maxRetries + "次重试后仍然不可用");
    }
    
    /**
     * 获取服务就绪状态
     *
     * @return 是否就绪
     */
    public boolean isServicesReady() {
        return servicesReady;
    }
    
    /**
     * 等待服务就绪
     *
     * @param timeoutSeconds 超时时间（秒）
     * @return 是否在超时前就绪
     */
    public boolean waitForServicesReady(int timeoutSeconds) {
        long startTime = System.currentTimeMillis();
        long timeoutMillis = timeoutSeconds * 1000L;
        
        while (!servicesReady && (System.currentTimeMillis() - startTime) < timeoutMillis) {
            try {
                TimeUnit.MILLISECONDS.sleep(100);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return false;
            }
        }
        
        return servicesReady;
    }
    
    /**
     * 获取启动状态信息
     *
     * @return 启动状态
     */
    public StartupStatus getStartupStatus() {
        return StartupStatus.builder()
            .servicesReady(servicesReady)
            .connectionManagerAvailable(connectionManager != null)
            .authServiceAvailable(authService != null)
            .messageRouterAvailable(messageRouter != null)
            .sessionManagerAvailable(sessionManager != null)
            .build();
    }
    
    /**
     * 服务健康检查接口
     */
    @FunctionalInterface
    private interface ServiceHealthCheck {
        boolean check() throws Exception;
    }
    
    /**
     * 启动状态信息
     */
    public static class StartupStatus {
        private boolean servicesReady;
        private boolean connectionManagerAvailable;
        private boolean authServiceAvailable;
        private boolean messageRouterAvailable;
        private boolean sessionManagerAvailable;
        
        public static StartupStatusBuilder builder() {
            return new StartupStatusBuilder();
        }
        
        public static class StartupStatusBuilder {
            private boolean servicesReady;
            private boolean connectionManagerAvailable;
            private boolean authServiceAvailable;
            private boolean messageRouterAvailable;
            private boolean sessionManagerAvailable;
            
            public StartupStatusBuilder servicesReady(boolean servicesReady) {
                this.servicesReady = servicesReady;
                return this;
            }
            
            public StartupStatusBuilder connectionManagerAvailable(boolean connectionManagerAvailable) {
                this.connectionManagerAvailable = connectionManagerAvailable;
                return this;
            }
            
            public StartupStatusBuilder authServiceAvailable(boolean authServiceAvailable) {
                this.authServiceAvailable = authServiceAvailable;
                return this;
            }
            
            public StartupStatusBuilder messageRouterAvailable(boolean messageRouterAvailable) {
                this.messageRouterAvailable = messageRouterAvailable;
                return this;
            }
            
            public StartupStatusBuilder sessionManagerAvailable(boolean sessionManagerAvailable) {
                this.sessionManagerAvailable = sessionManagerAvailable;
                return this;
            }
            
            public StartupStatus build() {
                StartupStatus status = new StartupStatus();
                status.servicesReady = this.servicesReady;
                status.connectionManagerAvailable = this.connectionManagerAvailable;
                status.authServiceAvailable = this.authServiceAvailable;
                status.messageRouterAvailable = this.messageRouterAvailable;
                status.sessionManagerAvailable = this.sessionManagerAvailable;
                return status;
            }
        }
        
        // Getters
        public boolean isServicesReady() { return servicesReady; }
        public boolean isConnectionManagerAvailable() { return connectionManagerAvailable; }
        public boolean isAuthServiceAvailable() { return authServiceAvailable; }
        public boolean isMessageRouterAvailable() { return messageRouterAvailable; }
        public boolean isSessionManagerAvailable() { return sessionManagerAvailable; }
    }
}