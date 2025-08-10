package com.vmqtt.vmqttcore.cluster;

import com.vmqtt.common.grpc.cluster.*;
import lombok.Data;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.time.Instant;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

/**
 * 高性能集群节点管理器
 * 负责节点注册、健康监控、故障检测和自动恢复
 *
 * @author zhenglin
 * @date 2025/08/10
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class ClusterNodeManager {
    
    private final ServiceRegistry serviceRegistry;
    
    // 本地节点信息
    private final AtomicReference<ClusterNode> localNode = new AtomicReference<>();
    
    // 集群节点缓存: nodeId -> ClusterNode
    private final ConcurrentMap<String, ClusterNode> clusterNodes = new ConcurrentHashMap<>();
    
    // 节点角色索引: role -> List<nodeId>
    private final ConcurrentMap<NodeRole, Set<String>> roleIndex = new ConcurrentHashMap<>();
    
    // 故障检测配置
    private static final int HEALTH_CHECK_INTERVAL_MS = 5000;
    private static final int NODE_TIMEOUT_MS = 15000;
    private static final int MAX_RETRY_ATTEMPTS = 3;
    
    /**
     * 初始化本地节点并注册到集群
     */
    public void initializeLocalNode(NodeRole role, String address, int port, Map<String, String> metadata) {
        String nodeId = generateNodeId();
        String serviceName = "vmqtt-" + role.name().toLowerCase().replace("_", "-");
        
        ServiceInfo serviceInfo = ServiceInfo.newBuilder()
            .setServiceId(nodeId)
            .setServiceName(serviceName)
            .setNodeId(nodeId)
            .setAddress(address)
            .setPort(port)
            .putAllMetadata(metadata)
            .setHealthStatus(HealthStatus.HEALTHY)
            .build();
        
        ClusterNode node = new ClusterNode(nodeId, role, NodeState.INITIALIZING, serviceInfo);
        localNode.set(node);
        clusterNodes.put(nodeId, node);
        
        // 注册到服务发现
        serviceRegistry.register(serviceInfo, 30);
        
        // 更新节点状态为运行中
        updateNodeState(nodeId, NodeState.RUNNING);
        
        log.info("本地节点初始化完成: nodeId={}, role={}, address={}:{}", 
            nodeId, role, address, port);
    }
    
    /**
     * 发现并缓存集群中的其他节点
     */
    public void discoverClusterNodes() {
        // 发现不同角色的节点
        for (NodeRole role : NodeRole.values()) {
            if (role == NodeRole.UNKNOWN_ROLE) continue;
            
            String serviceName = "vmqtt-" + role.name().toLowerCase().replace("_", "-");
            List<ServiceInfo> services = serviceRegistry.discover(serviceName);
            
            for (ServiceInfo service : services) {
                String nodeId = service.getNodeId();
                if (!clusterNodes.containsKey(nodeId)) {
                    ClusterNode node = new ClusterNode(nodeId, role, NodeState.RUNNING, service);
                    clusterNodes.put(nodeId, node);
                    addToRoleIndex(role, nodeId);
                    
                    log.info("发现新节点: nodeId={}, role={}, address={}:{}", 
                        nodeId, role, service.getAddress(), service.getPort());
                }
            }
        }
    }
    
    /**
     * 获取指定角色的健康节点列表
     */
    public List<ClusterNode> getHealthyNodesByRole(NodeRole role) {
        Set<String> nodeIds = roleIndex.getOrDefault(role, new HashSet<>());
        return nodeIds.stream()
            .map(clusterNodes::get)
            .filter(Objects::nonNull)
            .filter(node -> node.getState() == NodeState.RUNNING)
            .filter(node -> node.getServiceInfo().getHealthStatus() == HealthStatus.HEALTHY)
            .collect(Collectors.toList());
    }
    
    /**
     * 获取集群拓扑信息
     */
    public ClusterTopology getClusterTopology() {
        Map<NodeRole, List<ClusterNode>> topology = new HashMap<>();
        
        for (NodeRole role : NodeRole.values()) {
            topology.put(role, getHealthyNodesByRole(role));
        }
        
        return new ClusterTopology(
            clusterNodes.size(),
            (int) clusterNodes.values().stream()
                .filter(node -> node.getServiceInfo().getHealthStatus() == HealthStatus.HEALTHY)
                .count(),
            topology
        );
    }
    
    /**
     * 定期健康检查和节点维护
     */
    @Scheduled(fixedRate = HEALTH_CHECK_INTERVAL_MS)
    public void performHealthCheck() {
        long now = System.currentTimeMillis();
        
        for (ClusterNode node : clusterNodes.values()) {
            if (isLocalNode(node.getNodeId())) {
                // 本地节点发送心跳
                sendHeartbeat(node);
            } else {
                // 检查远程节点健康状态
                checkRemoteNodeHealth(node, now);
            }
        }
        
        // 清理失效节点
        cleanupFailedNodes(now);
    }
    
    /**
     * 检查远程节点健康状态
     */
    private void checkRemoteNodeHealth(ClusterNode node, long currentTime) {
        // 基于服务发现的TTL机制检查节点是否过期
        String serviceName = "vmqtt-" + node.getRole().name().toLowerCase().replace("_", "-");
        List<ServiceInfo> services = serviceRegistry.discover(serviceName);
        
        boolean nodeExists = services.stream()
            .anyMatch(s -> s.getNodeId().equals(node.getNodeId()));
        
        if (!nodeExists && node.getState() == NodeState.RUNNING) {
            // 节点从服务发现中消失，标记为失败
            updateNodeState(node.getNodeId(), NodeState.FAILED);
            log.warn("节点健康检查失败，标记为失败状态: nodeId={}", node.getNodeId());
        }
    }
    
    /**
     * 发送心跳到服务注册中心
     */
    private void sendHeartbeat(ClusterNode node) {
        try {
            // 计算当前负载指标
            LoadInfo loadInfo = calculateNodeLoad();
            
            // 更新元数据中的负载信息
            Map<String, String> metadata = new HashMap<>(node.getServiceInfo().getMetadataMap());
            metadata.put("cpu_usage", String.valueOf(loadInfo.getCpuUsage()));
            metadata.put("memory_usage", String.valueOf(loadInfo.getMemoryUsage()));
            metadata.put("connection_count", String.valueOf(loadInfo.getConnectionCount()));
            metadata.put("message_rate", String.valueOf(loadInfo.getMessageRate()));
            metadata.put("response_time", String.valueOf(loadInfo.getResponseTime()));
            
            // 发送心跳
            boolean success = serviceRegistry.heartbeat(node.getNodeId(), HealthStatus.HEALTHY);
            if (!success) {
                log.warn("心跳发送失败: nodeId={}", node.getNodeId());
            }
            
            node.setLastHeartbeat(Instant.now());
            
        } catch (Exception e) {
            log.error("发送心跳时出错: nodeId={}", node.getNodeId(), e);
        }
    }
    
    /**
     * 计算节点负载信息
     */
    private LoadInfo calculateNodeLoad() {
        // 获取JVM和系统指标
        Runtime runtime = Runtime.getRuntime();
        long totalMemory = runtime.totalMemory();
        long freeMemory = runtime.freeMemory();
        double memoryUsage = (double) (totalMemory - freeMemory) / totalMemory * 100;
        
        // TODO: 集成更详细的系统指标收集
        return LoadInfo.newBuilder()
            .setNodeId(getLocalNodeId())
            .setCpuUsage(getCpuUsage())
            .setMemoryUsage(memoryUsage)
            .setConnectionCount(getCurrentConnectionCount())
            .setMessageRate(getCurrentMessageRate())
            .setResponseTime(getAverageResponseTime())
            .setActiveThreads(Thread.activeCount())
            .build();
    }
    
    /**
     * 更新节点状态
     */
    public void updateNodeState(String nodeId, NodeState newState) {
        ClusterNode node = clusterNodes.get(nodeId);
        if (node != null) {
            NodeState oldState = node.getState();
            node.setState(newState);
            node.setLastStateChange(Instant.now());
            
            log.info("节点状态更新: nodeId={}, {} -> {}", nodeId, oldState, newState);
            
            // 状态变化事件处理
            handleNodeStateChange(node, oldState, newState);
        }
    }
    
    /**
     * 处理节点状态变化事件
     */
    private void handleNodeStateChange(ClusterNode node, NodeState oldState, NodeState newState) {
        if (newState == NodeState.FAILED && oldState == NodeState.RUNNING) {
            // 节点故障，触发故障转移
            triggerFailoverForNode(node);
        } else if (newState == NodeState.RUNNING && oldState == NodeState.FAILED) {
            // 节点恢复，重新加入集群
            rejoinNodeToCluster(node);
        }
    }
    
    /**
     * 触发节点故障转移
     */
    private void triggerFailoverForNode(ClusterNode failedNode) {
        log.warn("触发节点故障转移: nodeId={}, role={}", 
            failedNode.getNodeId(), failedNode.getRole());
        
        // 从角色索引中移除
        removeFromRoleIndex(failedNode.getRole(), failedNode.getNodeId());
        
        // TODO: 实现具体的故障转移逻辑
        // 1. 如果是COORDINATOR_NODE，重新选举协调者
        // 2. 如果是FRONTEND_NODE，迁移连接到其他节点
        // 3. 如果是BACKEND_NODE，确保数据一致性
    }
    
    /**
     * 节点重新加入集群
     */
    private void rejoinNodeToCluster(ClusterNode node) {
        log.info("节点重新加入集群: nodeId={}, role={}", 
            node.getNodeId(), node.getRole());
        
        addToRoleIndex(node.getRole(), node.getNodeId());
    }
    
    /**
     * 清理失效节点
     */
    private void cleanupFailedNodes(long currentTime) {
        Iterator<Map.Entry<String, ClusterNode>> iterator = clusterNodes.entrySet().iterator();
        
        while (iterator.hasNext()) {
            Map.Entry<String, ClusterNode> entry = iterator.next();
            ClusterNode node = entry.getValue();
            
            // 跳过本地节点
            if (isLocalNode(node.getNodeId())) {
                continue;
            }
            
            // 检查节点是否长时间失效
            if (node.getState() == NodeState.FAILED && 
                node.getLastStateChange().plusMillis(NODE_TIMEOUT_MS * 2).isBefore(Instant.now())) {
                
                // 从集群中移除节点
                iterator.remove();
                removeFromRoleIndex(node.getRole(), node.getNodeId());
                
                log.info("清理长时间失效节点: nodeId={}", node.getNodeId());
            }
        }
    }
    
    // 工具方法
    private String generateNodeId() {
        return "node-" + System.currentTimeMillis() + "-" + 
               Integer.toHexString(new Random().nextInt());
    }
    
    private boolean isLocalNode(String nodeId) {
        ClusterNode local = localNode.get();
        return local != null && local.getNodeId().equals(nodeId);
    }
    
    private String getLocalNodeId() {
        ClusterNode local = localNode.get();
        return local != null ? local.getNodeId() : "unknown";
    }
    
    /**
     * 获取当前节点ID（公开方法）
     */
    public String getCurrentNodeId() {
        return getLocalNodeId();
    }
    
    private void addToRoleIndex(NodeRole role, String nodeId) {
        roleIndex.computeIfAbsent(role, k -> ConcurrentHashMap.newKeySet()).add(nodeId);
    }
    
    private void removeFromRoleIndex(NodeRole role, String nodeId) {
        Set<String> nodes = roleIndex.get(role);
        if (nodes != null) {
            nodes.remove(nodeId);
        }
    }
    
    // TODO: 实现具体的指标收集方法
    private double getCpuUsage() { return 0.0; }
    private long getCurrentConnectionCount() { return 0; }
    private long getCurrentMessageRate() { return 0; }
    private double getAverageResponseTime() { return 0.0; }
    
    /**
     * 集群节点信息
     */
    @Data
    public static class ClusterNode {
        private String nodeId;
        private NodeRole role;
        private NodeState state;
        private ServiceInfo serviceInfo;
        private Instant lastHeartbeat;
        private Instant lastStateChange;
        private int retryCount;
        
        public ClusterNode(String nodeId, NodeRole role, NodeState state, ServiceInfo serviceInfo) {
            this.nodeId = nodeId;
            this.role = role;
            this.state = state;
            this.serviceInfo = serviceInfo;
            this.lastHeartbeat = Instant.now();
            this.lastStateChange = Instant.now();
            this.retryCount = 0;
        }
    }
    
    /**
     * 集群拓扑信息
     */
    @Data
    public static class ClusterTopology {
        private int totalNodes;
        private int healthyNodes;
        private Map<NodeRole, List<ClusterNode>> nodesByRole;
        
        public ClusterTopology(int totalNodes, int healthyNodes, 
                              Map<NodeRole, List<ClusterNode>> nodesByRole) {
            this.totalNodes = totalNodes;
            this.healthyNodes = healthyNodes;
            this.nodesByRole = nodesByRole;
        }
    }
}