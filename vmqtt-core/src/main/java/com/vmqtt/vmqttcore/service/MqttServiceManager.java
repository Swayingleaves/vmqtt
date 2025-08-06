/**
 * MQTT服务管理器
 *
 * @author zhenglin
 * @date 2025/08/06
 */
package com.vmqtt.vmqttcore.service;

import com.vmqtt.common.service.AuthenticationService;
import com.vmqtt.common.service.ConnectionManager;
import com.vmqtt.common.service.MessageRouter;
import com.vmqtt.common.service.SessionManager;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * MQTT服务管理器
 * 负责协调和管理所有核心服务的生命周期和定期维护任务
 */
@Slf4j
@Service
public class MqttServiceManager {

    @Autowired
    private ConnectionManager connectionManager;

    @Autowired
    private SessionManager sessionManager;

    @Autowired
    private MessageRouter messageRouter;

    @Autowired
    private AuthenticationService authenticationService;

    /**
     * 定时任务执行器
     */
    private ScheduledExecutorService scheduledExecutor;

    /**
     * 服务状态
     */
    private volatile boolean initialized = false;
    private volatile boolean running = false;

    /**
     * 初始化服务管理器
     */
    @PostConstruct
    public void initialize() {
        try {
            log.info("开始初始化MQTT服务管理器");

            // 创建定时任务执行器
            scheduledExecutor = Executors.newScheduledThreadPool(4, r -> {
                Thread thread = new Thread(r, "mqtt-service-maintenance");
                thread.setDaemon(true);
                return thread;
            });

            // 启动定期维护任务
            startMaintenanceTasks();

            // 打印服务统计信息
            logServiceStatistics();

            initialized = true;
            running = true;
            
            log.info("MQTT服务管理器初始化完成");

        } catch (Exception e) {
            log.error("MQTT服务管理器初始化失败", e);
            throw new RuntimeException("服务管理器初始化失败", e);
        }
    }

    /**
     * 关闭服务管理器
     */
    @PreDestroy
    public void shutdown() {
        try {
            log.info("开始关闭MQTT服务管理器");
            
            running = false;

            // 关闭定时任务执行器
            if (scheduledExecutor != null && !scheduledExecutor.isShutdown()) {
                scheduledExecutor.shutdown();
                if (!scheduledExecutor.awaitTermination(10, TimeUnit.SECONDS)) {
                    log.warn("定时任务执行器在指定时间内未能正常关闭，强制关闭");
                    scheduledExecutor.shutdownNow();
                }
            }

            // 打印最终统计信息
            logServiceStatistics();

            log.info("MQTT服务管理器关闭完成");

        } catch (Exception e) {
            log.error("MQTT服务管理器关闭异常", e);
        }
    }

    /**
     * 启动定期维护任务
     */
    private void startMaintenanceTasks() {
        log.info("启动定期维护任务");

        // 1. 清理过期连接任务（每5分钟执行一次）
        scheduledExecutor.scheduleAtFixedRate(
            this::cleanupExpiredConnections,
            5, 5, TimeUnit.MINUTES
        );

        // 2. 清理过期会话任务（每10分钟执行一次）
        scheduledExecutor.scheduleAtFixedRate(
            this::cleanupExpiredSessions,
            10, 10, TimeUnit.MINUTES
        );

        // 3. 打印统计信息任务（每1分钟执行一次）
        scheduledExecutor.scheduleAtFixedRate(
            this::logServiceStatistics,
            1, 1, TimeUnit.MINUTES
        );

        // 4. 健康检查任务（每30秒执行一次）
        scheduledExecutor.scheduleAtFixedRate(
            this::performHealthCheck,
            30, 30, TimeUnit.SECONDS
        );

        log.info("定期维护任务启动完成");
    }

    /**
     * 清理过期连接
     */
    private void cleanupExpiredConnections() {
        if (!running) return;

        try {
            CompletableFuture<Integer> cleanupTask = connectionManager.cleanupExpiredConnections();
            cleanupTask.thenAccept(cleanedCount -> {
                if (cleanedCount > 0) {
                    log.info("清理过期连接完成，清理数量: {}", cleanedCount);
                }
            }).exceptionally(throwable -> {
                log.error("清理过期连接失败", throwable);
                return null;
            });

        } catch (Exception e) {
            log.error("清理过期连接异常", e);
        }
    }

    /**
     * 清理过期会话
     */
    private void cleanupExpiredSessions() {
        if (!running) return;

        try {
            CompletableFuture<Integer> cleanupTask = sessionManager.cleanupExpiredSessions();
            cleanupTask.thenAccept(cleanedCount -> {
                if (cleanedCount > 0) {
                    log.info("清理过期会话完成，清理数量: {}", cleanedCount);
                }
            }).exceptionally(throwable -> {
                log.error("清理过期会话失败", throwable);
                return null;
            });

        } catch (Exception e) {
            log.error("清理过期会话异常", e);
        }
    }

    /**
     * 打印服务统计信息
     */
    private void logServiceStatistics() {
        if (!initialized) return;

        try {
            // 连接统计
            ConnectionManager.ConnectionStats connStats = connectionManager.getConnectionStats();
            log.info("连接统计 - 总连接: {}, 活跃连接: {}, 失败连接: {}, 连接速率: {:.2f}/s",
                connStats.getTotalConnections(),
                connStats.getActiveConnections(),
                connStats.getFailedConnections(),
                connStats.getConnectionRate());

            // 会话统计
            SessionManager.SessionStats sessStats = sessionManager.getSessionStats();
            log.info("会话统计 - 总会话: {}, 活跃会话: {}, 总订阅: {}, 排队消息: {}, 传输中消息: {}",
                sessStats.getTotalSessions(),
                sessStats.getActiveSessions(),
                sessStats.getTotalSubscriptions(),
                sessStats.getQueuedMessageCount(),
                sessStats.getInflightMessageCount());

            // 路由统计
            MessageRouter.RouteStats routeStats = messageRouter.getRouteStats();
            log.info("路由统计 - 总路由: {}, 成功路由: {}, 失败路由: {}, 路由速率: {:.2f}/s, 平均延迟: {:.2f}ms",
                routeStats.getTotalRoutedMessages(),
                routeStats.getSuccessfulRoutes(),
                routeStats.getFailedRoutes(),
                routeStats.getRoutingRate(),
                routeStats.getAverageRoutingLatency());

            // 认证统计
            AuthenticationService.AuthenticationStats authStats = authenticationService.getAuthenticationStats();
            log.info("认证统计 - 总认证: {}, 成功认证: {}, 失败认证: {}, 成功率: {:.2f}%, 平均认证时间: {:.2f}ms",
                authStats.getTotalAuthentications(),
                authStats.getSuccessfulAuthentications(),
                authStats.getFailedAuthentications(),
                authStats.getAuthenticationSuccessRate() * 100,
                authStats.getAverageAuthenticationTime());

        } catch (Exception e) {
            log.error("获取服务统计信息异常", e);
        }
    }

    /**
     * 执行健康检查
     */
    private void performHealthCheck() {
        if (!running) return;

        try {
            boolean healthy = true;
            StringBuilder issues = new StringBuilder();

            // 检查连接管理器
            long activeConnections = connectionManager.getConnectionCount();
            if (activeConnections < 0) {
                healthy = false;
                issues.append("ConnectionManager返回负连接数; ");
            }

            // 检查会话管理器
            long activeSessions = sessionManager.getSessionCount();
            if (activeSessions < 0) {
                healthy = false;
                issues.append("SessionManager返回负会话数; ");
            }

            // 检查服务可用性（简单ping测试）
            // 这里可以添加更复杂的健康检查逻辑

            if (!healthy) {
                log.warn("健康检查发现问题: {}", issues.toString());
            } else {
                log.debug("健康检查通过 - 活跃连接: {}, 活跃会话: {}", activeConnections, activeSessions);
            }

        } catch (Exception e) {
            log.error("健康检查异常", e);
        }
    }

    /**
     * 获取服务状态信息
     *
     * @return 服务状态
     */
    public ServiceStatus getServiceStatus() {
        return ServiceStatus.builder()
                .initialized(initialized)
                .running(running)
                .connectionCount(connectionManager.getConnectionCount())
                .sessionCount(sessionManager.getSessionCount())
                .connectionStats(connectionManager.getConnectionStats())
                .sessionStats(sessionManager.getSessionStats())
                .routeStats(messageRouter.getRouteStats())
                .authStats(authenticationService.getAuthenticationStats())
                .build();
    }

    /**
     * 检查服务是否就绪
     *
     * @return 是否就绪
     */
    public boolean isReady() {
        return initialized && running;
    }

    /**
     * 检查服务是否健康
     *
     * @return 是否健康
     */
    public boolean isHealthy() {
        if (!isReady()) {
            return false;
        }

        try {
            // 基本的健康检查
            return connectionManager.getConnectionCount() >= 0 &&
                   sessionManager.getSessionCount() >= 0;
        } catch (Exception e) {
            log.error("健康检查异常", e);
            return false;
        }
    }

    /**
     * 服务状态信息
     */
    public static class ServiceStatus {
        private boolean initialized;
        private boolean running;
        private long connectionCount;
        private long sessionCount;
        private ConnectionManager.ConnectionStats connectionStats;
        private SessionManager.SessionStats sessionStats;
        private MessageRouter.RouteStats routeStats;
        private AuthenticationService.AuthenticationStats authStats;

        public static ServiceStatusBuilder builder() {
            return new ServiceStatusBuilder();
        }

        // Getters
        public boolean isInitialized() { return initialized; }
        public boolean isRunning() { return running; }
        public long getConnectionCount() { return connectionCount; }
        public long getSessionCount() { return sessionCount; }
        public ConnectionManager.ConnectionStats getConnectionStats() { return connectionStats; }
        public SessionManager.SessionStats getSessionStats() { return sessionStats; }
        public MessageRouter.RouteStats getRouteStats() { return routeStats; }
        public AuthenticationService.AuthenticationStats getAuthStats() { return authStats; }

        // Builder pattern
        public static class ServiceStatusBuilder {
            private boolean initialized;
            private boolean running;
            private long connectionCount;
            private long sessionCount;
            private ConnectionManager.ConnectionStats connectionStats;
            private SessionManager.SessionStats sessionStats;
            private MessageRouter.RouteStats routeStats;
            private AuthenticationService.AuthenticationStats authStats;

            public ServiceStatusBuilder initialized(boolean initialized) {
                this.initialized = initialized;
                return this;
            }

            public ServiceStatusBuilder running(boolean running) {
                this.running = running;
                return this;
            }

            public ServiceStatusBuilder connectionCount(long connectionCount) {
                this.connectionCount = connectionCount;
                return this;
            }

            public ServiceStatusBuilder sessionCount(long sessionCount) {
                this.sessionCount = sessionCount;
                return this;
            }

            public ServiceStatusBuilder connectionStats(ConnectionManager.ConnectionStats connectionStats) {
                this.connectionStats = connectionStats;
                return this;
            }

            public ServiceStatusBuilder sessionStats(SessionManager.SessionStats sessionStats) {
                this.sessionStats = sessionStats;
                return this;
            }

            public ServiceStatusBuilder routeStats(MessageRouter.RouteStats routeStats) {
                this.routeStats = routeStats;
                return this;
            }

            public ServiceStatusBuilder authStats(AuthenticationService.AuthenticationStats authStats) {
                this.authStats = authStats;
                return this;
            }

            public ServiceStatus build() {
                ServiceStatus status = new ServiceStatus();
                status.initialized = this.initialized;
                status.running = this.running;
                status.connectionCount = this.connectionCount;
                status.sessionCount = this.sessionCount;
                status.connectionStats = this.connectionStats;
                status.sessionStats = this.sessionStats;
                status.routeStats = this.routeStats;
                status.authStats = this.authStats;
                return status;
            }
        }
    }
}