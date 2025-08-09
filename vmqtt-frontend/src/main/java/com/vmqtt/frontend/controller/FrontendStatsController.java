/**
 * 前端统计信息控制器
 *
 * @author zhenglin
 * @date 2025/08/09
 */
package com.vmqtt.frontend.controller;

import com.vmqtt.frontend.server.NettyMqttServer;
import com.vmqtt.frontend.server.VirtualThreadManager;
import com.vmqtt.frontend.server.handler.MqttConnectionHandler;
import com.vmqtt.frontend.server.handler.SslContextHandler;
import com.vmqtt.frontend.service.FrontendAuthManager;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.actuate.health.Health;
import org.springframework.boot.actuate.health.HealthIndicator;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.Map;

/**
 * V-MQTT前端服务统计信息和健康检查
 */
@Slf4j
@RestController
@RequestMapping("/api/v1/frontend")
@RequiredArgsConstructor
public class FrontendStatsController implements HealthIndicator {
    
    private final NettyMqttServer mqttServer;
    private final VirtualThreadManager threadManager;
    private final MqttConnectionHandler connectionHandler;
    private final SslContextHandler sslHandler;
    private final FrontendAuthManager authManager;
    
    /**
     * 获取服务器状态信息
     *
     * @return 服务器状态
     */
    @GetMapping("/status")
    public Map<String, Object> getServerStatus() {
        NettyMqttServer.ServerStats serverStats = mqttServer.getServerStats();
        VirtualThreadManager.ThreadPoolStats threadStats = threadManager.getThreadPoolStats();
        MqttConnectionHandler.ConnectionStats connectionStats = connectionHandler.getConnectionStats();
        SslContextHandler.SslStats sslStats = sslHandler.getSslStats();
        FrontendAuthManager.AuthStats authStats = authManager.getAuthStats();
        
        return Map.of(
            "server", Map.of(
                "running", serverStats.isRunning(),
                "tcpPort", serverStats.getTcpPort(),
                "sslPort", serverStats.getSslPort(),
                "sslEnabled", serverStats.isSslEnabled(),
                "maxConnections", serverStats.getMaxConnections(),
                "virtualThreadEnabled", serverStats.isVirtualThreadEnabled(),
                "bossThreads", serverStats.getBossThreads(),
                "workerThreads", serverStats.getWorkerThreads()
            ),
            "threads", Map.of(
                "totalThreads", threadStats.getTotalThreads(),
                "virtualThreads", threadStats.getVirtualThreads(),
                "activeTasks", Map.of(
                    "mqtt", threadStats.getMqttActiveTasks(),
                    "connection", threadStats.getConnectionActiveTasks(),
                    "message", threadStats.getMessageActiveTasks(),
                    "auth", threadStats.getAuthActiveTasks(),
                    "heartbeat", threadStats.getHeartbeatActiveTasks()
                ),
                "completedTasks", Map.of(
                    "mqtt", threadStats.getMqttCompletedTasks(),
                    "connection", threadStats.getConnectionCompletedTasks(),
                    "message", threadStats.getMessageCompletedTasks(),
                    "auth", threadStats.getAuthCompletedTasks(),
                    "heartbeat", threadStats.getHeartbeatCompletedTasks()
                )
            ),
            "connections", Map.of(
                "active", connectionStats.getActiveConnections(),
                "total", connectionStats.getTotalConnections(),
                "failed", connectionStats.getFailedConnections(),
                "max", connectionStats.getMaxConnections(),
                "utilization", connectionStats.getConnectionUtilization()
            ),
            "ssl", Map.of(
                "enabled", sslStats.isEnabled(),
                "available", sslStats.isAvailable(),
                "clientAuthRequired", sslStats.isClientAuthRequired(),
                "protocols", sslStats.getProtocols(),
                "cipherSuites", sslStats.getCipherSuites()
            ),
            "auth", Map.of(
                "totalSessions", authStats.getTotalSessions(),
                "activeSessions", authStats.getActiveSessions()
            )
        );
    }
    
    /**
     * 获取连接统计信息
     *
     * @return 连接统计
     */
    @GetMapping("/connections")
    public MqttConnectionHandler.ConnectionStats getConnectionStats() {
        return connectionHandler.getConnectionStats();
    }
    
    /**
     * 获取线程池统计信息
     *
     * @return 线程池统计
     */
    @GetMapping("/threads")
    public VirtualThreadManager.ThreadPoolStats getThreadStats() {
        return threadManager.getThreadPoolStats();
    }
    
    /**
     * 获取认证统计信息
     *
     * @return 认证统计
     */
    @GetMapping("/auth")
    public FrontendAuthManager.AuthStats getAuthStats() {
        return authManager.getAuthStats();
    }
    
    /**
     * 获取SSL配置信息
     *
     * @return SSL配置
     */
    @GetMapping("/ssl")
    public SslContextHandler.SslStats getSslStats() {
        return sslHandler.getSslStats();
    }
    
    /**
     * 重置连接统计信息
     *
     * @return 操作结果
     */
    @GetMapping("/connections/reset")
    public Map<String, Object> resetConnectionStats() {
        try {
            connectionHandler.resetStats();
            return Map.of("success", true, "message", "连接统计信息已重置");
        } catch (Exception e) {
            log.error("重置连接统计信息失败", e);
            return Map.of("success", false, "message", "重置失败: " + e.getMessage());
        }
    }
    
    /**
     * 清理过期认证会话
     *
     * @return 操作结果
     */
    @GetMapping("/auth/cleanup")
    public Map<String, Object> cleanupExpiredSessions() {
        try {
            int cleanupCount = authManager.cleanupExpiredSessions();
            return Map.of(
                "success", true, 
                "message", "清理完成", 
                "cleanupCount", cleanupCount
            );
        } catch (Exception e) {
            log.error("清理过期会话失败", e);
            return Map.of("success", false, "message", "清理失败: " + e.getMessage());
        }
    }
    
    /**
     * Spring Boot Actuator健康检查
     *
     * @return 健康状态
     */
    @Override
    public Health health() {
        try {
            Health.Builder builder = Health.up();
            
            // 检查服务器运行状态
            if (!mqttServer.isRunning()) {
                builder = Health.down().withDetail("reason", "MQTT服务器未运行");
            }
            
            // 检查连接状态
            MqttConnectionHandler.ConnectionStats connectionStats = connectionHandler.getConnectionStats();
            double utilization = connectionStats.getConnectionUtilization();
            
            if (utilization > 0.9) {
                builder.withDetail("warning", "连接使用率过高: " + String.format("%.2f%%", utilization * 100));
            }
            
            // 添加基本信息
            builder.withDetail("activeConnections", connectionStats.getActiveConnections())
                   .withDetail("totalConnections", connectionStats.getTotalConnections())
                   .withDetail("connectionUtilization", String.format("%.2f%%", utilization * 100))
                   .withDetail("sslEnabled", sslHandler.isSslAvailable())
                   .withDetail("virtualThreads", threadManager.getVirtualThreadCount());
            
            return builder.build();
            
        } catch (Exception e) {
            log.error("健康检查失败", e);
            return Health.down()
                .withDetail("error", e.getMessage())
                .build();
        }
    }
}