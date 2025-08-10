package com.vmqtt.vmqttcore.cluster;

import com.vmqtt.common.grpc.cluster.HealthStatus;
import com.vmqtt.common.grpc.cluster.LoadInfo;
import com.vmqtt.common.grpc.cluster.NodeRole;
import lombok.Data;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.scheduling.annotation.Async;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.time.Instant;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 多层健康检查管理器
 * 实现分层健康检查、故障检测和自动恢复
 *
 * @author zhenglin
 * @date 2025/08/10
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class HealthCheckManager {
    
    private final ClusterNodeManager nodeManager;
    private final ServiceRegistry serviceRegistry;
    
    // 健康检查结果缓存: nodeId -> HealthCheckResult
    private final ConcurrentMap<String, HealthCheckResult> healthResults = new ConcurrentHashMap<>();
    
    // 故障检测阈值配置
    private static final int FAILURE_THRESHOLD = 3;           // 连续失败3次认定故障
    private static final int RECOVERY_THRESHOLD = 2;          // 连续成功2次认定恢复
    private static final long HEALTH_CHECK_TIMEOUT_MS = 5000; // 健康检查超时5秒
    private static final long CIRCUIT_BREAKER_RESET_MS = 30000; // 熔断器重置时间30秒
    
    // 系统级健康指标
    private final AtomicLong totalHealthChecks = new AtomicLong(0);
    private final AtomicLong failedHealthChecks = new AtomicLong(0);
    private final AtomicInteger circuitBreakerCount = new AtomicInteger(0);
    
    /**
     * 执行全集群健康检查
     */
    @Scheduled(fixedRate = 10000) // 每10秒执行一次
    public void performClusterHealthCheck() {
        ClusterNodeManager.ClusterTopology topology = nodeManager.getClusterTopology();
        
        // 并行检查所有节点
        List<CompletableFuture<Void>> checkTasks = new ArrayList<>();
        
        for (Map.Entry<NodeRole, List<ClusterNodeManager.ClusterNode>> entry : 
             topology.getNodesByRole().entrySet()) {
            
            NodeRole role = entry.getKey();
            List<ClusterNodeManager.ClusterNode> nodes = entry.getValue();
            
            for (ClusterNodeManager.ClusterNode node : nodes) {
                CompletableFuture<Void> task = performNodeHealthCheckAsync(node, role);
                checkTasks.add(task);
            }
        }
        
        // 等待所有检查完成
        CompletableFuture.allOf(checkTasks.toArray(new CompletableFuture[0]))
            .whenComplete((result, throwable) -> {
                if (throwable != null) {
                    log.error("集群健康检查出现异常", throwable);
                }
                updateClusterHealthMetrics();
            });
    }
    
    /**
     * 异步执行单个节点健康检查
     */
    @Async
    public CompletableFuture<Void> performNodeHealthCheckAsync(
            ClusterNodeManager.ClusterNode node, NodeRole role) {
        
        return CompletableFuture.runAsync(() -> {
            try {
                HealthCheckResult result = performDetailedHealthCheck(node, role);
                processHealthCheckResult(node, result);
            } catch (Exception e) {
                log.error("节点健康检查异常: nodeId={}", node.getNodeId(), e);
                handleHealthCheckFailure(node, "健康检查异常: " + e.getMessage());
            }
        });
    }
    
    /**
     * 执行详细的健康检查
     */
    private HealthCheckResult performDetailedHealthCheck(
            ClusterNodeManager.ClusterNode node, NodeRole role) {
        
        totalHealthChecks.incrementAndGet();
        Instant startTime = Instant.now();
        
        HealthCheckResult result = new HealthCheckResult();
        result.setNodeId(node.getNodeId());
        result.setRole(role);
        result.setCheckTime(startTime);
        
        try {
            // 1. 基础连通性检查
            boolean connectivity = checkConnectivity(node);
            result.setConnectivityOk(connectivity);
            
            if (!connectivity) {
                result.setOverallStatus(HealthStatus.UNHEALTHY);
                result.setFailureReason("节点连通性检查失败");
                return result;
            }
            
            // 2. 服务注册状态检查
            boolean registrationOk = checkServiceRegistration(node);
            result.setRegistrationOk(registrationOk);
            
            // 3. 负载状态检查
            LoadInfo loadInfo = checkNodeLoad(node);
            result.setLoadInfo(loadInfo);
            boolean loadOk = evaluateLoadHealth(loadInfo);
            result.setLoadOk(loadOk);
            
            // 4. 角色特定的健康检查
            boolean roleSpecificOk = performRoleSpecificCheck(node, role);
            result.setRoleSpecificOk(roleSpecificOk);
            
            // 5. 综合评估健康状态
            HealthStatus overallStatus = evaluateOverallHealth(
                connectivity, registrationOk, loadOk, roleSpecificOk, loadInfo);
            result.setOverallStatus(overallStatus);
            
            if (overallStatus == HealthStatus.HEALTHY) {
                result.setSuccessful(true);
            } else {
                result.setFailureReason(buildFailureReason(
                    connectivity, registrationOk, loadOk, roleSpecificOk));
            }
            
        } catch (Exception e) {
            result.setOverallStatus(HealthStatus.CRITICAL);
            result.setFailureReason("健康检查执行异常: " + e.getMessage());
            failedHealthChecks.incrementAndGet();
        } finally {
            result.setResponseTime(
                Instant.now().toEpochMilli() - startTime.toEpochMilli());
        }
        
        return result;
    }
    
    /**
     * 检查节点连通性
     */
    private boolean checkConnectivity(ClusterNodeManager.ClusterNode node) {
        try {
            // 基于服务发现检查节点是否还在注册表中
            String serviceName = "vmqtt-" + node.getRole().name().toLowerCase().replace("_", "-");
            return serviceRegistry.discover(serviceName).stream()
                .anyMatch(s -> s.getNodeId().equals(node.getNodeId()));
        } catch (Exception e) {
            log.warn("连通性检查失败: nodeId={}", node.getNodeId(), e);
            return false;
        }
    }
    
    /**
     * 检查服务注册状态
     */
    private boolean checkServiceRegistration(ClusterNodeManager.ClusterNode node) {
        try {
            // 检查心跳是否正常
            return node.getLastHeartbeat()
                .isAfter(Instant.now().minusMillis(HEALTH_CHECK_TIMEOUT_MS * 2));
        } catch (Exception e) {
            return false;
        }
    }
    
    /**
     * 检查节点负载
     */
    private LoadInfo checkNodeLoad(ClusterNodeManager.ClusterNode node) {
        try {
            // 从服务信息的元数据中获取负载指标
            Map<String, String> metadata = node.getServiceInfo().getMetadataMap();
            
            LoadInfo.Builder builder = LoadInfo.newBuilder()
                .setNodeId(node.getNodeId());
            
            if (metadata.containsKey("cpu_usage")) {
                builder.setCpuUsage(Double.parseDouble(metadata.get("cpu_usage")));
            }
            if (metadata.containsKey("memory_usage")) {
                builder.setMemoryUsage(Double.parseDouble(metadata.get("memory_usage")));
            }
            if (metadata.containsKey("connection_count")) {
                builder.setConnectionCount(Long.parseLong(metadata.get("connection_count")));
            }
            if (metadata.containsKey("message_rate")) {
                builder.setMessageRate(Long.parseLong(metadata.get("message_rate")));
            }
            if (metadata.containsKey("response_time")) {
                builder.setResponseTime(Double.parseDouble(metadata.get("response_time")));
            }
            
            return builder.build();
        } catch (Exception e) {
            log.warn("获取节点负载信息失败: nodeId={}", node.getNodeId(), e);
            return LoadInfo.newBuilder().setNodeId(node.getNodeId()).build();
        }
    }
    
    /**
     * 评估负载健康状态
     */
    private boolean evaluateLoadHealth(LoadInfo loadInfo) {
        // 定义健康阈值
        double CPU_THRESHOLD = 85.0;      // CPU使用率85%
        double MEMORY_THRESHOLD = 90.0;   // 内存使用率90%
        double RESPONSE_TIME_THRESHOLD = 1000.0; // 响应时间1秒
        
        boolean cpuOk = loadInfo.getCpuUsage() < CPU_THRESHOLD;
        boolean memoryOk = loadInfo.getMemoryUsage() < MEMORY_THRESHOLD;
        boolean responseTimeOk = loadInfo.getResponseTime() < RESPONSE_TIME_THRESHOLD;
        
        return cpuOk && memoryOk && responseTimeOk;
    }
    
    /**
     * 执行角色特定的健康检查
     */
    private boolean performRoleSpecificCheck(ClusterNodeManager.ClusterNode node, NodeRole role) {
        switch (role) {
            case FRONTEND_NODE:
                return checkFrontendNodeHealth(node);
            case BACKEND_NODE:
                return checkBackendNodeHealth(node);
            case COORDINATOR_NODE:
                return checkCoordinatorNodeHealth(node);
            case LOAD_BALANCER:
                return checkLoadBalancerHealth(node);
            default:
                return true;
        }
    }
    
    private boolean checkFrontendNodeHealth(ClusterNodeManager.ClusterNode node) {
        // 检查前端节点特定指标：连接数、网络I/O等
        Map<String, String> metadata = node.getServiceInfo().getMetadataMap();
        try {
            long connections = Long.parseLong(metadata.getOrDefault("connection_count", "0"));
            // 假设单个前端节点最大支持100万连接
            return connections < 1000000;
        } catch (Exception e) {
            return true; // 默认认为健康
        }
    }
    
    private boolean checkBackendNodeHealth(ClusterNodeManager.ClusterNode node) {
        // 检查后端节点特定指标：存储状态、数据一致性等
        // TODO: 实现RocksDB健康检查
        return true;
    }
    
    private boolean checkCoordinatorNodeHealth(ClusterNodeManager.ClusterNode node) {
        // 检查协调节点特定指标：集群状态同步、选举状态等
        // TODO: 实现Raft协议健康检查
        return true;
    }
    
    private boolean checkLoadBalancerHealth(ClusterNodeManager.ClusterNode node) {
        // 检查负载均衡器特定指标：分发延迟、均衡度等
        // TODO: 实现负载均衡器健康检查
        return true;
    }
    
    /**
     * 综合评估节点健康状态
     */
    private HealthStatus evaluateOverallHealth(boolean connectivity, boolean registration, 
                                              boolean load, boolean roleSpecific, LoadInfo loadInfo) {
        if (!connectivity) {
            return HealthStatus.CRITICAL;
        }
        
        if (!registration) {
            return HealthStatus.UNHEALTHY;
        }
        
        if (!roleSpecific) {
            return HealthStatus.UNHEALTHY;
        }
        
        if (!load) {
            // 负载高但其他指标正常，标记为警告状态
            if (loadInfo.getCpuUsage() > 90 || loadInfo.getMemoryUsage() > 95) {
                return HealthStatus.CRITICAL;
            } else {
                return HealthStatus.WARNING;
            }
        }
        
        return HealthStatus.HEALTHY;
    }
    
    /**
     * 构建失败原因描述
     */
    private String buildFailureReason(boolean connectivity, boolean registration, 
                                     boolean load, boolean roleSpecific) {
        List<String> reasons = new ArrayList<>();
        
        if (!connectivity) reasons.add("连通性检查失败");
        if (!registration) reasons.add("服务注册状态异常");
        if (!load) reasons.add("负载状态不健康");
        if (!roleSpecific) reasons.add("角色特定检查失败");
        
        return String.join(", ", reasons);
    }
    
    /**
     * 处理健康检查结果
     */
    private void processHealthCheckResult(ClusterNodeManager.ClusterNode node, 
                                         HealthCheckResult result) {
        String nodeId = node.getNodeId();
        HealthCheckResult previousResult = healthResults.get(nodeId);
        healthResults.put(nodeId, result);
        
        // 故障检测逻辑
        if (!result.isSuccessful()) {
            handleHealthCheckFailure(node, result.getFailureReason());
        } else {
            handleHealthCheckSuccess(node, previousResult);
        }
        
        // 更新节点状态
        updateNodeHealthStatus(node, result);
    }
    
    /**
     * 处理健康检查失败
     */
    private void handleHealthCheckFailure(ClusterNodeManager.ClusterNode node, String reason) {
        String nodeId = node.getNodeId();
        HealthCheckResult result = healthResults.get(nodeId);
        
        if (result == null) {
            result = new HealthCheckResult();
            result.setNodeId(nodeId);
            healthResults.put(nodeId, result);
        }
        
        result.incrementFailureCount();
        failedHealthChecks.incrementAndGet();
        
        log.warn("节点健康检查失败: nodeId={}, reason={}, failureCount={}", 
            nodeId, reason, result.getFailureCount());
        
        // 连续失败达到阈值，触发故障转移
        if (result.getFailureCount() >= FAILURE_THRESHOLD) {
            triggerNodeFailover(node, reason);
        }
    }
    
    /**
     * 处理健康检查成功
     */
    private void handleHealthCheckSuccess(ClusterNodeManager.ClusterNode node, 
                                         HealthCheckResult previousResult) {
        if (previousResult != null && previousResult.getFailureCount() > 0) {
            previousResult.incrementSuccessCount();
            
            // 连续成功达到阈值，节点恢复
            if (previousResult.getSuccessCount() >= RECOVERY_THRESHOLD) {
                handleNodeRecovery(node);
                previousResult.resetCounts();
            }
        }
    }
    
    /**
     * 触发节点故障转移
     */
    private void triggerNodeFailover(ClusterNodeManager.ClusterNode node, String reason) {
        log.error("触发节点故障转移: nodeId={}, role={}, reason={}", 
            node.getNodeId(), node.getRole(), reason);
        
        // 更新节点状态
        nodeManager.updateNodeState(node.getNodeId(), 
            com.vmqtt.common.grpc.cluster.NodeState.FAILED);
        
        // 启用熔断器
        circuitBreakerCount.incrementAndGet();
        
        // TODO: 实现具体的故障转移逻辑
        performFailoverActions(node, reason);
    }
    
    /**
     * 处理节点恢复
     */
    private void handleNodeRecovery(ClusterNodeManager.ClusterNode node) {
        log.info("节点健康状态恢复: nodeId={}, role={}", 
            node.getNodeId(), node.getRole());
        
        // 更新节点状态
        nodeManager.updateNodeState(node.getNodeId(), 
            com.vmqtt.common.grpc.cluster.NodeState.RUNNING);
        
        // 重新启用节点服务
        reactivateNodeServices(node);
    }
    
    /**
     * 执行故障转移动作
     */
    private void performFailoverActions(ClusterNodeManager.ClusterNode failedNode, String reason) {
        NodeRole role = failedNode.getRole();
        
        switch (role) {
            case FRONTEND_NODE:
                // 前端节点故障：迁移客户端连接
                handleFrontendNodeFailover(failedNode);
                break;
            case BACKEND_NODE:
                // 后端节点故障：数据备份和恢复
                handleBackendNodeFailover(failedNode);
                break;
            case COORDINATOR_NODE:
                // 协调节点故障：重新选举协调者
                handleCoordinatorNodeFailover(failedNode);
                break;
            case LOAD_BALANCER:
                // 负载均衡器故障：切换到备用均衡器
                handleLoadBalancerFailover(failedNode);
                break;
        }
    }
    
    private void handleFrontendNodeFailover(ClusterNodeManager.ClusterNode failedNode) {
        // TODO: 实现前端节点故障转移
        // 1. 通知其他前端节点准备接收迁移的连接
        // 2. 如果有连接迁移机制，执行连接迁移
        // 3. 更新负载均衡配置，移除故障节点
        log.info("执行前端节点故障转移: nodeId={}", failedNode.getNodeId());
    }
    
    private void handleBackendNodeFailover(ClusterNodeManager.ClusterNode failedNode) {
        // TODO: 实现后端节点故障转移
        // 1. 检查数据副本状态
        // 2. 从副本恢复数据到其他节点
        // 3. 更新数据路由配置
        log.info("执行后端节点故障转移: nodeId={}", failedNode.getNodeId());
    }
    
    private void handleCoordinatorNodeFailover(ClusterNodeManager.ClusterNode failedNode) {
        // TODO: 实现协调节点故障转移
        // 1. 触发新的领导者选举
        // 2. 选出新的协调节点
        // 3. 同步集群状态
        log.info("执行协调节点故障转移: nodeId={}", failedNode.getNodeId());
    }
    
    private void handleLoadBalancerFailover(ClusterNodeManager.ClusterNode failedNode) {
        // TODO: 实现负载均衡器故障转移
        // 1. 激活备用负载均衡器
        // 2. 同步负载均衡配置
        // 3. 更新服务发现配置
        log.info("执行负载均衡器故障转移: nodeId={}", failedNode.getNodeId());
    }
    
    /**
     * 重新激活节点服务
     */
    private void reactivateNodeServices(ClusterNodeManager.ClusterNode node) {
        // TODO: 实现节点服务重新激活逻辑
        log.info("重新激活节点服务: nodeId={}", node.getNodeId());
    }
    
    /**
     * 更新节点健康状态
     */
    private void updateNodeHealthStatus(ClusterNodeManager.ClusterNode node, 
                                       HealthCheckResult result) {
        // 更新服务注册中心的健康状态
        serviceRegistry.heartbeat(node.getNodeId(), result.getOverallStatus());
    }
    
    /**
     * 更新集群健康指标
     */
    private void updateClusterHealthMetrics() {
        long total = totalHealthChecks.get();
        long failed = failedHealthChecks.get();
        double successRate = total > 0 ? (double) (total - failed) / total * 100 : 100.0;
        
        log.debug("集群健康检查统计 - 总检查数: {}, 失败数: {}, 成功率: {:.2f}%, 熔断器数: {}", 
            total, failed, successRate, circuitBreakerCount.get());
    }
    
    /**
     * 获取集群健康状态概览
     */
    public ClusterHealthStatus getClusterHealthStatus() {
        ClusterNodeManager.ClusterTopology topology = nodeManager.getClusterTopology();
        
        int totalNodes = topology.getTotalNodes();
        int healthyNodes = topology.getHealthyNodes();
        int unhealthyNodes = totalNodes - healthyNodes;
        
        double healthRatio = totalNodes > 0 ? (double) healthyNodes / totalNodes : 1.0;
        
        return new ClusterHealthStatus(
            totalNodes, healthyNodes, unhealthyNodes, healthRatio,
            totalHealthChecks.get(), failedHealthChecks.get(),
            circuitBreakerCount.get()
        );
    }
    
    /**
     * 健康检查结果
     */
    @Data
    public static class HealthCheckResult {
        private String nodeId;
        private NodeRole role;
        private Instant checkTime;
        private long responseTime;
        private boolean successful;
        private HealthStatus overallStatus;
        private String failureReason;
        
        // 详细检查结果
        private boolean connectivityOk;
        private boolean registrationOk;
        private boolean loadOk;
        private boolean roleSpecificOk;
        private LoadInfo loadInfo;
        
        // 故障计数
        private int failureCount = 0;
        private int successCount = 0;
        
        public void incrementFailureCount() {
            failureCount++;
            successCount = 0; // 重置成功计数
        }
        
        public void incrementSuccessCount() {
            successCount++;
        }
        
        public void resetCounts() {
            failureCount = 0;
            successCount = 0;
        }
    }
    
    /**
     * 集群健康状态概览
     */
    @Data
    public static class ClusterHealthStatus {
        private int totalNodes;
        private int healthyNodes;
        private int unhealthyNodes;
        private double healthRatio;
        private long totalHealthChecks;
        private long failedHealthChecks;
        private int activeCircuitBreakers;
        
        public ClusterHealthStatus(int totalNodes, int healthyNodes, int unhealthyNodes,
                                  double healthRatio, long totalHealthChecks, 
                                  long failedHealthChecks, int activeCircuitBreakers) {
            this.totalNodes = totalNodes;
            this.healthyNodes = healthyNodes;
            this.unhealthyNodes = unhealthyNodes;
            this.healthRatio = healthRatio;
            this.totalHealthChecks = totalHealthChecks;
            this.failedHealthChecks = failedHealthChecks;
            this.activeCircuitBreakers = activeCircuitBreakers;
        }
    }
}