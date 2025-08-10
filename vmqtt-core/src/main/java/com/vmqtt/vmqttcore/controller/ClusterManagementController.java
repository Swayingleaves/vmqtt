package com.vmqtt.vmqttcore.controller;

import com.vmqtt.common.grpc.cluster.LoadBalanceStrategy;
import com.vmqtt.common.grpc.cluster.NodeRole;
import com.vmqtt.vmqttcore.cluster.ClusterNodeManager;
import com.vmqtt.vmqttcore.cluster.ClusterStateSyncManager;
import com.vmqtt.vmqttcore.cluster.DynamicLoadBalancerManager;
import com.vmqtt.vmqttcore.cluster.HealthCheckManager;
import lombok.Data;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * 集群管理控制器
 * 提供集群状态查询、节点管理、负载均衡等REST API
 *
 * @author zhenglin
 * @date 2025/08/10
 */
@Slf4j
@RestController
@RequestMapping("/api/v1/cluster")
@RequiredArgsConstructor
public class ClusterManagementController {
    
    private final ClusterNodeManager nodeManager;
    private final HealthCheckManager healthCheckManager;
    private final ClusterStateSyncManager stateSyncManager;
    private final DynamicLoadBalancerManager loadBalancerManager;
    
    /**
     * 获取集群拓扑信息
     */
    @GetMapping("/topology")
    public ResponseEntity<Map<String, Object>> getClusterTopology() {
        try {
            ClusterNodeManager.ClusterTopology topology = nodeManager.getClusterTopology();
            
            Map<String, Object> response = new HashMap<>();
            response.put("totalNodes", topology.getTotalNodes());
            response.put("healthyNodes", topology.getHealthyNodes());
            response.put("unhealthyNodes", topology.getTotalNodes() - topology.getHealthyNodes());
            
            Map<String, Object> nodesByRole = new HashMap<>();
            topology.getNodesByRole().forEach((role, nodes) -> {
                nodesByRole.put(role.name(), nodes.size());
            });
            response.put("nodesByRole", nodesByRole);
            
            return ResponseEntity.ok(response);
        } catch (Exception e) {
            log.error("获取集群拓扑信息失败", e);
            return ResponseEntity.internalServerError().body(
                Map.of("error", "获取集群拓扑信息失败: " + e.getMessage()));
        }
    }
    
    /**
     * 获取指定角色的节点列表
     */
    @GetMapping("/nodes/{role}")
    public ResponseEntity<Map<String, Object>> getNodesByRole(@PathVariable String role) {
        try {
            NodeRole nodeRole = NodeRole.valueOf(role.toUpperCase());
            List<ClusterNodeManager.ClusterNode> nodes = nodeManager.getHealthyNodesByRole(nodeRole);
            
            Map<String, Object> response = new HashMap<>();
            response.put("role", nodeRole.name());
            response.put("nodeCount", nodes.size());
            response.put("nodes", nodes.stream()
                .map(node -> Map.of(
                    "nodeId", node.getNodeId(),
                    "address", node.getServiceInfo().getAddress(),
                    "port", node.getServiceInfo().getPort(),
                    "state", node.getState().name(),
                    "healthStatus", node.getServiceInfo().getHealthStatus().name(),
                    "lastHeartbeat", node.getLastHeartbeat().toString()
                ))
                .toList());
            
            return ResponseEntity.ok(response);
        } catch (IllegalArgumentException e) {
            return ResponseEntity.badRequest().body(
                Map.of("error", "无效的节点角色: " + role));
        } catch (Exception e) {
            log.error("获取节点列表失败", e);
            return ResponseEntity.internalServerError().body(
                Map.of("error", "获取节点列表失败: " + e.getMessage()));
        }
    }
    
    /**
     * 获取集群健康状态
     */
    @GetMapping("/health")
    public ResponseEntity<Map<String, Object>> getClusterHealth() {
        try {
            HealthCheckManager.ClusterHealthStatus healthStatus = healthCheckManager.getClusterHealthStatus();
            
            Map<String, Object> response = new HashMap<>();
            response.put("totalNodes", healthStatus.getTotalNodes());
            response.put("healthyNodes", healthStatus.getHealthyNodes());
            response.put("unhealthyNodes", healthStatus.getUnhealthyNodes());
            response.put("healthRatio", String.format("%.2f%%", healthStatus.getHealthRatio() * 100));
            response.put("totalHealthChecks", healthStatus.getTotalHealthChecks());
            response.put("failedHealthChecks", healthStatus.getFailedHealthChecks());
            response.put("activeCircuitBreakers", healthStatus.getActiveCircuitBreakers());
            
            // 健康状态评级
            String healthGrade;
            if (healthStatus.getHealthRatio() >= 0.9) {
                healthGrade = "EXCELLENT";
            } else if (healthStatus.getHealthRatio() >= 0.7) {
                healthGrade = "GOOD";
            } else if (healthStatus.getHealthRatio() >= 0.5) {
                healthGrade = "WARNING";
            } else {
                healthGrade = "CRITICAL";
            }
            response.put("healthGrade", healthGrade);
            
            return ResponseEntity.ok(response);
        } catch (Exception e) {
            log.error("获取集群健康状态失败", e);
            return ResponseEntity.internalServerError().body(
                Map.of("error", "获取集群健康状态失败: " + e.getMessage()));
        }
    }
    
    /**
     * 获取负载均衡统计
     */
    @GetMapping("/loadbalancing/stats")
    public ResponseEntity<Map<String, Object>> getLoadBalancingStats() {
        try {
            DynamicLoadBalancerManager.LoadBalancingStats stats = loadBalancerManager.getLoadBalancingStats();
            
            Map<String, Object> response = new HashMap<>();
            response.put("nodeCount", stats.getNodeStats().size());
            response.put("strategyCount", stats.getStrategyMetrics().size());
            response.put("weightAdjustmentCount", stats.getWeightAdjustments().size());
            
            // 策略性能统计
            Map<String, Object> strategyStats = new HashMap<>();
            stats.getStrategyMetrics().forEach((strategy, metrics) -> {
                Map<String, Object> metricData = new HashMap<>();
                metricData.put("totalSelections", metrics.getTotalSelections().get());
                metricData.put("successfulSelections", metrics.getSuccessfulSelections().get());
                metricData.put("successRate", String.format("%.2f%%", metrics.getSuccessRate() * 100));
                metricData.put("averageSelectionTime", String.format("%.3f ms", metrics.getAverageSelectionTime() / 1000000.0));
                strategyStats.put(strategy.name(), metricData);
            });
            response.put("strategyMetrics", strategyStats);
            
            // 节点负载统计
            Map<String, Object> nodeStats = new HashMap<>();
            stats.getNodeStats().forEach((nodeId, nodeLoadStats) -> {
                Map<String, Object> loadData = new HashMap<>();
                if (nodeLoadStats.getCurrentLoad() != null) {
                    loadData.put("cpuUsage", String.format("%.1f%%", nodeLoadStats.getCurrentLoad().getCpuUsage()));
                    loadData.put("memoryUsage", String.format("%.1f%%", nodeLoadStats.getCurrentLoad().getMemoryUsage()));
                    loadData.put("connectionCount", nodeLoadStats.getCurrentLoad().getConnectionCount());
                    loadData.put("responseTime", String.format("%.3f ms", nodeLoadStats.getCurrentLoad().getResponseTime()));
                }
                loadData.put("averageSelectionTime", String.format("%.3f ms", nodeLoadStats.getAverageSelectionTime() / 1000000.0));
                loadData.put("lastUpdated", nodeLoadStats.getLastUpdated().toString());
                nodeStats.put(nodeId, loadData);
            });
            response.put("nodeLoadStats", nodeStats);
            
            return ResponseEntity.ok(response);
        } catch (Exception e) {
            log.error("获取负载均衡统计失败", e);
            return ResponseEntity.internalServerError().body(
                Map.of("error", "获取负载均衡统计失败: " + e.getMessage()));
        }
    }
    
    /**
     * 选择最佳节点
     */
    @PostMapping("/loadbalancing/select")
    public ResponseEntity<Map<String, Object>> selectBestNode(@RequestBody NodeSelectionRequest request) {
        try {
            NodeRole role = NodeRole.valueOf(request.getRole().toUpperCase());
            LoadBalanceStrategy strategy = request.getStrategy() != null ? 
                LoadBalanceStrategy.valueOf(request.getStrategy().toUpperCase()) :
                loadBalancerManager.selectOptimalStrategy(role, 0);
            
            ClusterNodeManager.ClusterNode selectedNode = loadBalancerManager.selectBestNode(
                role, request.getClientKey(), strategy);
            
            if (selectedNode == null) {
                return ResponseEntity.ok(Map.of(
                    "success", false,
                    "message", "没有可用的健康节点",
                    "role", role.name(),
                    "strategy", strategy.name()
                ));
            }
            
            Map<String, Object> response = new HashMap<>();
            response.put("success", true);
            response.put("selectedNode", Map.of(
                "nodeId", selectedNode.getNodeId(),
                "address", selectedNode.getServiceInfo().getAddress(),
                "port", selectedNode.getServiceInfo().getPort(),
                "healthStatus", selectedNode.getServiceInfo().getHealthStatus().name()
            ));
            response.put("role", role.name());
            response.put("strategy", strategy.name());
            
            return ResponseEntity.ok(response);
        } catch (IllegalArgumentException e) {
            return ResponseEntity.badRequest().body(
                Map.of("error", "无效的参数: " + e.getMessage()));
        } catch (Exception e) {
            log.error("选择最佳节点失败", e);
            return ResponseEntity.internalServerError().body(
                Map.of("error", "选择最佳节点失败: " + e.getMessage()));
        }
    }
    
    /**
     * 初始化本地节点
     */
    @PostMapping("/nodes/init")
    public ResponseEntity<Map<String, Object>> initializeLocalNode(@RequestBody NodeInitRequest request) {
        try {
            NodeRole role = NodeRole.valueOf(request.getRole().toUpperCase());
            
            nodeManager.initializeLocalNode(
                role, 
                request.getAddress(), 
                request.getPort(), 
                request.getMetadata()
            );
            
            return ResponseEntity.ok(Map.of(
                "success", true,
                "message", "本地节点初始化成功",
                "role", role.name(),
                "address", request.getAddress(),
                "port", request.getPort()
            ));
        } catch (Exception e) {
            log.error("初始化本地节点失败", e);
            return ResponseEntity.internalServerError().body(
                Map.of("error", "初始化本地节点失败: " + e.getMessage()));
        }
    }
    
    /**
     * 发现集群节点
     */
    @PostMapping("/nodes/discover")
    public ResponseEntity<Map<String, Object>> discoverClusterNodes() {
        try {
            nodeManager.discoverClusterNodes();
            
            ClusterNodeManager.ClusterTopology topology = nodeManager.getClusterTopology();
            
            return ResponseEntity.ok(Map.of(
                "success", true,
                "message", "集群节点发现完成",
                "discoveredNodes", topology.getTotalNodes(),
                "healthyNodes", topology.getHealthyNodes()
            ));
        } catch (Exception e) {
            log.error("发现集群节点失败", e);
            return ResponseEntity.internalServerError().body(
                Map.of("error", "发现集群节点失败: " + e.getMessage()));
        }
    }
    
    /**
     * 获取可用的负载均衡策略
     */
    @GetMapping("/loadbalancing/strategies")
    public ResponseEntity<Map<String, Object>> getAvailableStrategies() {
        try {
            Map<String, String> strategies = new HashMap<>();
            strategies.put("ROUND_ROBIN", "轮询");
            strategies.put("WEIGHTED_ROUND_ROBIN", "加权轮询");
            strategies.put("LEAST_CONNECTIONS", "最少连接");
            strategies.put("LEAST_RESPONSE_TIME", "最短响应时间");
            strategies.put("CONSISTENT_HASH", "一致性哈希");
            strategies.put("RANDOM", "随机");
            
            return ResponseEntity.ok(Map.of(
                "strategies", strategies,
                "defaultStrategy", "WEIGHTED_ROUND_ROBIN"
            ));
        } catch (Exception e) {
            log.error("获取负载均衡策略失败", e);
            return ResponseEntity.internalServerError().body(
                Map.of("error", "获取负载均衡策略失败: " + e.getMessage()));
        }
    }
    
    /**
     * 获取支持的节点角色
     */
    @GetMapping("/nodes/roles")
    public ResponseEntity<Map<String, Object>> getSupportedRoles() {
        try {
            Map<String, String> roles = new HashMap<>();
            roles.put("FRONTEND_NODE", "前端节点 - 处理客户端连接");
            roles.put("BACKEND_NODE", "后端节点 - 数据持久化和业务逻辑");
            roles.put("COORDINATOR_NODE", "协调节点 - 集群协调和管理");
            roles.put("LOAD_BALANCER", "负载均衡器 - 流量分发");
            
            return ResponseEntity.ok(Map.of("roles", roles));
        } catch (Exception e) {
            log.error("获取支持的节点角色失败", e);
            return ResponseEntity.internalServerError().body(
                Map.of("error", "获取支持的节点角色失败: " + e.getMessage()));
        }
    }
    
    // 请求数据类
    @Data
    public static class NodeSelectionRequest {
        private String role;
        private String strategy;
        private String clientKey;
    }
    
    @Data
    public static class NodeInitRequest {
        private String role;
        private String address;
        private Integer port;
        private Map<String, String> metadata = new HashMap<>();
    }
}