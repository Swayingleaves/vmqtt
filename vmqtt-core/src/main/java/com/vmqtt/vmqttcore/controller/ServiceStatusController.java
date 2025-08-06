/**
 * 服务状态控制器
 *
 * @author zhenglin
 * @date 2025/08/06
 */
package com.vmqtt.vmqttcore.controller;

import com.vmqtt.vmqttcore.service.MqttServiceManager;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.HashMap;
import java.util.Map;

/**
 * 服务状态REST控制器
 * 提供服务状态查询和健康检查接口
 */
@Slf4j
@RestController
@RequestMapping("/api/v1/service")
public class ServiceStatusController {

    @Autowired
    private MqttServiceManager serviceManager;

    /**
     * 获取服务状态
     *
     * @return 服务状态信息
     */
    @GetMapping("/status")
    public ResponseEntity<Map<String, Object>> getServiceStatus() {
        try {
            MqttServiceManager.ServiceStatus status = serviceManager.getServiceStatus();
            
            Map<String, Object> response = new HashMap<>();
            response.put("initialized", status.isInitialized());
            response.put("running", status.isRunning());
            response.put("ready", serviceManager.isReady());
            response.put("healthy", serviceManager.isHealthy());
            response.put("connectionCount", status.getConnectionCount());
            response.put("sessionCount", status.getSessionCount());
            
            // 详细统计信息
            Map<String, Object> stats = new HashMap<>();
            
            // 连接统计
            if (status.getConnectionStats() != null) {
                Map<String, Object> connStats = new HashMap<>();
                connStats.put("totalConnections", status.getConnectionStats().getTotalConnections());
                connStats.put("activeConnections", status.getConnectionStats().getActiveConnections());
                connStats.put("failedConnections", status.getConnectionStats().getFailedConnections());
                connStats.put("averageConnectionDuration", status.getConnectionStats().getAverageConnectionDuration());
                connStats.put("connectionRate", status.getConnectionStats().getConnectionRate());
                connStats.put("disconnectionRate", status.getConnectionStats().getDisconnectionRate());
                stats.put("connections", connStats);
            }
            
            // 会话统计
            if (status.getSessionStats() != null) {
                Map<String, Object> sessStats = new HashMap<>();
                sessStats.put("totalSessions", status.getSessionStats().getTotalSessions());
                sessStats.put("activeSessions", status.getSessionStats().getActiveSessions());
                sessStats.put("inactiveSessions", status.getSessionStats().getInactiveSessions());
                sessStats.put("totalSubscriptions", status.getSessionStats().getTotalSubscriptions());
                sessStats.put("queuedMessageCount", status.getSessionStats().getQueuedMessageCount());
                sessStats.put("inflightMessageCount", status.getSessionStats().getInflightMessageCount());
                sessStats.put("averageSessionDuration", status.getSessionStats().getAverageSessionDuration());
                stats.put("sessions", sessStats);
            }
            
            // 路由统计
            if (status.getRouteStats() != null) {
                Map<String, Object> routeStats = new HashMap<>();
                routeStats.put("totalRoutedMessages", status.getRouteStats().getTotalRoutedMessages());
                routeStats.put("successfulRoutes", status.getRouteStats().getSuccessfulRoutes());
                routeStats.put("failedRoutes", status.getRouteStats().getFailedRoutes());
                routeStats.put("droppedMessages", status.getRouteStats().getDroppedMessages());
                routeStats.put("averageRoutingLatency", status.getRouteStats().getAverageRoutingLatency());
                routeStats.put("routingRate", status.getRouteStats().getRoutingRate());
                
                // QoS统计
                if (status.getRouteStats().getQosStats() != null) {
                    Map<String, Object> qosStats = new HashMap<>();
                    qosStats.put("qos0Routes", status.getRouteStats().getQosStats().getQos0Routes());
                    qosStats.put("qos1Routes", status.getRouteStats().getQosStats().getQos1Routes());
                    qosStats.put("qos2Routes", status.getRouteStats().getQosStats().getQos2Routes());
                    routeStats.put("qosStats", qosStats);
                }
                stats.put("routing", routeStats);
            }
            
            // 认证统计
            if (status.getAuthStats() != null) {
                Map<String, Object> authStats = new HashMap<>();
                authStats.put("totalAuthentications", status.getAuthStats().getTotalAuthentications());
                authStats.put("successfulAuthentications", status.getAuthStats().getSuccessfulAuthentications());
                authStats.put("failedAuthentications", status.getAuthStats().getFailedAuthentications());
                authStats.put("blockedClients", status.getAuthStats().getBlockedClients());
                authStats.put("activeCredentials", status.getAuthStats().getActiveCredentials());
                authStats.put("expiredCredentials", status.getAuthStats().getExpiredCredentials());
                authStats.put("authenticationSuccessRate", status.getAuthStats().getAuthenticationSuccessRate());
                authStats.put("averageAuthenticationTime", status.getAuthStats().getAverageAuthenticationTime());
                stats.put("authentication", authStats);
            }
            
            response.put("statistics", stats);
            response.put("timestamp", System.currentTimeMillis());
            
            return ResponseEntity.ok(response);
            
        } catch (Exception e) {
            log.error("获取服务状态失败", e);
            Map<String, Object> errorResponse = new HashMap<>();
            errorResponse.put("error", "Failed to get service status");
            errorResponse.put("message", e.getMessage());
            errorResponse.put("timestamp", System.currentTimeMillis());
            return ResponseEntity.internalServerError().body(errorResponse);
        }
    }

    /**
     * 健康检查接口
     *
     * @return 健康状态
     */
    @GetMapping("/health")
    public ResponseEntity<Map<String, Object>> healthCheck() {
        try {
            boolean healthy = serviceManager.isHealthy();
            boolean ready = serviceManager.isReady();
            
            Map<String, Object> response = new HashMap<>();
            response.put("status", healthy ? "UP" : "DOWN");
            response.put("healthy", healthy);
            response.put("ready", ready);
            response.put("timestamp", System.currentTimeMillis());
            
            if (healthy) {
                return ResponseEntity.ok(response);
            } else {
                return ResponseEntity.status(503).body(response);
            }
            
        } catch (Exception e) {
            log.error("健康检查失败", e);
            Map<String, Object> response = new HashMap<>();
            response.put("status", "DOWN");
            response.put("healthy", false);
            response.put("ready", false);
            response.put("error", e.getMessage());
            response.put("timestamp", System.currentTimeMillis());
            return ResponseEntity.status(503).body(response);
        }
    }

    /**
     * 就绪检查接口
     *
     * @return 就绪状态
     */
    @GetMapping("/ready")
    public ResponseEntity<Map<String, Object>> readinessCheck() {
        try {
            boolean ready = serviceManager.isReady();
            
            Map<String, Object> response = new HashMap<>();
            response.put("ready", ready);
            response.put("timestamp", System.currentTimeMillis());
            
            if (ready) {
                return ResponseEntity.ok(response);
            } else {
                return ResponseEntity.status(503).body(response);
            }
            
        } catch (Exception e) {
            log.error("就绪检查失败", e);
            Map<String, Object> response = new HashMap<>();
            response.put("ready", false);
            response.put("error", e.getMessage());
            response.put("timestamp", System.currentTimeMillis());
            return ResponseEntity.status(503).body(response);
        }
    }

    /**
     * 获取简化的服务信息
     *
     * @return 简化的服务信息
     */
    @GetMapping("/info")
    public ResponseEntity<Map<String, Object>> getServiceInfo() {
        try {
            Map<String, Object> response = new HashMap<>();
            response.put("service", "V-MQTT Server");
            response.put("version", "1.0.0-SNAPSHOT");
            response.put("description", "High-performance MQTT server with FE/BE architecture");
            response.put("ready", serviceManager.isReady());
            response.put("healthy", serviceManager.isHealthy());
            response.put("timestamp", System.currentTimeMillis());
            
            // 基本统计
            MqttServiceManager.ServiceStatus status = serviceManager.getServiceStatus();
            Map<String, Object> basicStats = new HashMap<>();
            basicStats.put("connections", status.getConnectionCount());
            basicStats.put("sessions", status.getSessionCount());
            response.put("basicStats", basicStats);
            
            return ResponseEntity.ok(response);
            
        } catch (Exception e) {
            log.error("获取服务信息失败", e);
            Map<String, Object> response = new HashMap<>();
            response.put("service", "V-MQTT Server");
            response.put("error", e.getMessage());
            response.put("timestamp", System.currentTimeMillis());
            return ResponseEntity.internalServerError().body(response);
        }
    }
}