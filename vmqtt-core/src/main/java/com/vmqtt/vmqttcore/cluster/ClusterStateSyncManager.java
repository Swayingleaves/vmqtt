package com.vmqtt.vmqttcore.cluster;

import com.vmqtt.common.grpc.cluster.NodeRole;
import com.vmqtt.common.grpc.cluster.ServiceInfo;
import lombok.Data;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.time.Instant;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 集群状态同步管理器
 * 基于Gossip协议实现分布式状态同步和最终一致性
 *
 * @author zhenglin
 * @date 2025/08/10
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class ClusterStateSyncManager {
    
    private final ClusterNodeManager nodeManager;
    private final ServiceRegistry serviceRegistry;
    private final ServiceDiscoveryController discoveryController;
    
    // 状态版本控制
    private final AtomicLong stateVersion = new AtomicLong(0);
    
    // 集群状态快照: stateId -> ClusterStateSnapshot
    private final ConcurrentMap<String, ClusterStateSnapshot> stateSnapshots = new ConcurrentHashMap<>();
    
    // Gossip传播记录: nodeId -> Set<stateId>
    private final ConcurrentMap<String, Set<String>> gossipLog = new ConcurrentHashMap<>();
    
    // 状态冲突解决记录
    private final ConcurrentMap<String, StateConflictRecord> conflictRecords = new ConcurrentHashMap<>();
    
    // Gossip协议配置
    private static final int GOSSIP_FANOUT = 3;           // 每轮Gossip向3个节点传播
    private static final int GOSSIP_INTERVAL_MS = 2000;   // Gossip间隔2秒
    private static final int MAX_STATE_HISTORY = 100;     // 最大状态历史记录
    private static final int STATE_TTL_MINUTES = 30;      // 状态TTL 30分钟
    
    /**
     * 定期执行Gossip状态同步
     */
    @Scheduled(fixedRate = GOSSIP_INTERVAL_MS)
    public void performGossipSync() {
        try {
            // 生成当前集群状态快照
            ClusterStateSnapshot currentSnapshot = generateClusterStateSnapshot();
            
            // 选择Gossip目标节点
            List<ClusterNodeManager.ClusterNode> targetNodes = selectGossipTargets();
            
            if (targetNodes.isEmpty()) {
                log.debug("没有可用的Gossip目标节点");
                return;
            }
            
            // 并行向目标节点传播状态
            List<CompletableFuture<Void>> gossipTasks = new ArrayList<>();
            
            for (ClusterNodeManager.ClusterNode targetNode : targetNodes) {
                CompletableFuture<Void> task = gossipStateToNodeAsync(currentSnapshot, targetNode);
                gossipTasks.add(task);
            }
            
            // 等待所有Gossip任务完成
            CompletableFuture.allOf(gossipTasks.toArray(new CompletableFuture[0]))
                .whenComplete((result, throwable) -> {
                    if (throwable != null) {
                        log.warn("Gossip状态同步部分失败", throwable);
                    } else {
                        log.debug("Gossip状态同步完成，传播到{}个节点", targetNodes.size());
                    }
                });
            
            // 清理过期状态
            cleanupExpiredStates();
            
        } catch (Exception e) {
            log.error("Gossip状态同步执行异常", e);
        }
    }
    
    /**
     * 生成当前集群状态快照
     */
    private ClusterStateSnapshot generateClusterStateSnapshot() {
        long version = stateVersion.incrementAndGet();
        String snapshotId = "snapshot-" + version + "-" + System.currentTimeMillis();
        
        ClusterNodeManager.ClusterTopology topology = nodeManager.getClusterTopology();
        
        // 收集所有节点信息
        Map<String, NodeStateInfo> nodeStates = new HashMap<>();
        
        for (Map.Entry<NodeRole, List<ClusterNodeManager.ClusterNode>> entry : 
             topology.getNodesByRole().entrySet()) {
            
            for (ClusterNodeManager.ClusterNode node : entry.getValue()) {
                NodeStateInfo stateInfo = new NodeStateInfo(
                    node.getNodeId(),
                    node.getRole(),
                    node.getState(),
                    node.getServiceInfo(),
                    node.getLastHeartbeat(),
                    extractNodeMetrics(node)
                );
                
                nodeStates.put(node.getNodeId(), stateInfo);
            }
        }
        
        ClusterStateSnapshot snapshot = new ClusterStateSnapshot(
            snapshotId, version, Instant.now(), nodeStates, generateChecksum(nodeStates)
        );
        
        // 缓存状态快照
        stateSnapshots.put(snapshotId, snapshot);
        
        // 限制历史记录数量
        if (stateSnapshots.size() > MAX_STATE_HISTORY) {
            cleanupOldSnapshots();
        }
        
        return snapshot;
    }
    
    /**
     * 提取节点指标信息
     */
    private Map<String, Object> extractNodeMetrics(ClusterNodeManager.ClusterNode node) {
        Map<String, Object> metrics = new HashMap<>();
        Map<String, String> metadata = node.getServiceInfo().getMetadataMap();
        
        // 提取关键指标
        metrics.put("cpu_usage", parseDoubleOrDefault(metadata.get("cpu_usage"), 0.0));
        metrics.put("memory_usage", parseDoubleOrDefault(metadata.get("memory_usage"), 0.0));
        metrics.put("connection_count", parseLongOrDefault(metadata.get("connection_count"), 0L));
        metrics.put("message_rate", parseLongOrDefault(metadata.get("message_rate"), 0L));
        metrics.put("response_time", parseDoubleOrDefault(metadata.get("response_time"), 0.0));
        
        return metrics;
    }
    
    /**
     * 选择Gossip传播目标节点
     */
    private List<ClusterNodeManager.ClusterNode> selectGossipTargets() {
        ClusterNodeManager.ClusterTopology topology = nodeManager.getClusterTopology();
        List<ClusterNodeManager.ClusterNode> allNodes = new ArrayList<>();
        
        // 收集所有健康节点（除本地节点外）
        for (List<ClusterNodeManager.ClusterNode> roleNodes : topology.getNodesByRole().values()) {
            for (ClusterNodeManager.ClusterNode node : roleNodes) {
                if (!isLocalNode(node.getNodeId()) && 
                    node.getState() == com.vmqtt.common.grpc.cluster.NodeState.RUNNING) {
                    allNodes.add(node);
                }
            }
        }
        
        if (allNodes.isEmpty()) {
            return Collections.emptyList();
        }
        
        // 随机选择目标节点（确保多样性）
        Collections.shuffle(allNodes, ThreadLocalRandom.current());
        int targetCount = Math.min(GOSSIP_FANOUT, allNodes.size());
        
        return allNodes.subList(0, targetCount);
    }
    
    /**
     * 异步向目标节点传播状态
     */
    private CompletableFuture<Void> gossipStateToNodeAsync(
            ClusterStateSnapshot snapshot, ClusterNodeManager.ClusterNode targetNode) {
        
        return CompletableFuture.runAsync(() -> {
            try {
                // 构建Gossip负载
                ServiceDiscoveryController.GossipPayload payload = buildGossipPayload(snapshot);
                
                // 发送Gossip消息
                boolean success = sendGossipMessage(targetNode, payload);
                
                if (success) {
                    // 记录成功传播
                    recordGossipPropagation(targetNode.getNodeId(), snapshot.getSnapshotId());
                    log.debug("状态Gossip传播成功: target={}, snapshot={}", 
                        targetNode.getNodeId(), snapshot.getSnapshotId());
                } else {
                    log.warn("状态Gossip传播失败: target={}, snapshot={}", 
                        targetNode.getNodeId(), snapshot.getSnapshotId());
                }
                
            } catch (Exception e) {
                log.error("Gossip传播异常: target={}", targetNode.getNodeId(), e);
            }
        });
    }
    
    /**
     * 构建Gossip负载
     */
    private ServiceDiscoveryController.GossipPayload buildGossipPayload(ClusterStateSnapshot snapshot) {
        ServiceDiscoveryController.GossipPayload payload = new ServiceDiscoveryController.GossipPayload();
        payload.setTtlSeconds(30); // 设置TTL为30秒
        
        List<ServiceDiscoveryController.RegisterRequest> services = new ArrayList<>();
        
        for (NodeStateInfo nodeState : snapshot.getNodeStates().values()) {
            ServiceDiscoveryController.RegisterRequest request = new ServiceDiscoveryController.RegisterRequest();
            ServiceInfo serviceInfo = nodeState.getServiceInfo();
            
            request.setServiceId(serviceInfo.getServiceId());
            request.setServiceName(serviceInfo.getServiceName());
            request.setServiceVersion(serviceInfo.getServiceVersion());
            request.setNodeId(serviceInfo.getNodeId());
            request.setAddress(serviceInfo.getAddress());
            request.setPort(serviceInfo.getPort());
            
            // 包含状态快照元数据
            Map<String, String> metadata = new HashMap<>(serviceInfo.getMetadataMap());
            metadata.put("snapshot_id", snapshot.getSnapshotId());
            metadata.put("snapshot_version", String.valueOf(snapshot.getVersion()));
            metadata.put("snapshot_checksum", snapshot.getChecksum());
            
            request.setMetadata(metadata);
            request.setTags(serviceInfo.getTagsMap());
            
            services.add(request);
        }
        
        payload.setServices(services);
        return payload;
    }
    
    /**
     * 发送Gossip消息
     */
    private boolean sendGossipMessage(ClusterNodeManager.ClusterNode targetNode, 
                                     ServiceDiscoveryController.GossipPayload payload) {
        try {
            // 通过HTTP调用目标节点的Gossip接口
            // 这里使用ServiceDiscoveryController的gossip方法
            discoveryController.gossip(payload);
            return true;
        } catch (Exception e) {
            log.warn("Gossip消息发送失败: target={}", targetNode.getNodeId(), e);
            return false;
        }
    }
    
    /**
     * 处理接收到的Gossip消息
     */
    public void handleIncomingGossip(ServiceDiscoveryController.GossipPayload payload) {
        try {
            // 解析状态快照信息
            ClusterStateSnapshot incomingSnapshot = parseGossipPayload(payload);
            
            if (incomingSnapshot == null) {
                log.warn("无法解析Gossip负载中的状态快照");
                return;
            }
            
            // 检查是否已经处理过此快照
            if (stateSnapshots.containsKey(incomingSnapshot.getSnapshotId())) {
                log.debug("状态快照已存在，跳过处理: {}", incomingSnapshot.getSnapshotId());
                return;
            }
            
            // 合并状态信息
            mergeIncomingState(incomingSnapshot);
            
            // 检测和解决冲突
            resolveStateConflicts(incomingSnapshot);
            
            log.debug("处理Gossip状态同步完成: snapshot={}", incomingSnapshot.getSnapshotId());
            
        } catch (Exception e) {
            log.error("处理Gossip消息异常", e);
        }
    }
    
    /**
     * 解析Gossip负载为状态快照
     */
    private ClusterStateSnapshot parseGossipPayload(ServiceDiscoveryController.GossipPayload payload) {
        if (payload.getServices() == null || payload.getServices().isEmpty()) {
            return null;
        }
        
        // 从第一个服务的元数据中提取快照信息
        ServiceDiscoveryController.RegisterRequest firstService = payload.getServices().get(0);
        Map<String, String> metadata = firstService.getMetadata();
        
        String snapshotId = metadata.get("snapshot_id");
        String versionStr = metadata.get("snapshot_version");
        String checksum = metadata.get("snapshot_checksum");
        
        if (snapshotId == null || versionStr == null) {
            return null;
        }
        
        long version;
        try {
            version = Long.parseLong(versionStr);
        } catch (NumberFormatException e) {
            return null;
        }
        
        // 构建节点状态信息
        Map<String, NodeStateInfo> nodeStates = new HashMap<>();
        
        for (ServiceDiscoveryController.RegisterRequest service : payload.getServices()) {
            ServiceInfo serviceInfo = service.toServiceInfo();
            
            NodeStateInfo stateInfo = new NodeStateInfo(
                serviceInfo.getNodeId(),
                determineNodeRole(serviceInfo.getServiceName()),
                com.vmqtt.common.grpc.cluster.NodeState.RUNNING,
                serviceInfo,
                Instant.now(),
                extractMetricsFromMetadata(serviceInfo.getMetadataMap())
            );
            
            nodeStates.put(serviceInfo.getNodeId(), stateInfo);
        }
        
        return new ClusterStateSnapshot(snapshotId, version, Instant.now(), nodeStates, checksum);
    }
    
    /**
     * 合并传入的状态信息
     */
    private void mergeIncomingState(ClusterStateSnapshot incomingSnapshot) {
        // 获取当前状态快照
        ClusterStateSnapshot currentSnapshot = getCurrentStateSnapshot();
        
        if (currentSnapshot == null) {
            // 如果没有当前状态，直接采用传入状态
            stateSnapshots.put(incomingSnapshot.getSnapshotId(), incomingSnapshot);
            updateLocalClusterState(incomingSnapshot);
            return;
        }
        
        // 版本比较
        if (incomingSnapshot.getVersion() > currentSnapshot.getVersion()) {
            // 传入状态版本更新，直接采用
            stateSnapshots.put(incomingSnapshot.getSnapshotId(), incomingSnapshot);
            updateLocalClusterState(incomingSnapshot);
            
            log.info("采用更新的集群状态: version {} -> {}", 
                currentSnapshot.getVersion(), incomingSnapshot.getVersion());
        } else if (incomingSnapshot.getVersion() == currentSnapshot.getVersion()) {
            // 版本相同，需要进行状态合并
            ClusterStateSnapshot mergedSnapshot = mergeStateSnapshots(currentSnapshot, incomingSnapshot);
            stateSnapshots.put(mergedSnapshot.getSnapshotId(), mergedSnapshot);
            updateLocalClusterState(mergedSnapshot);
        }
        // 版本较旧的快照忽略
    }
    
    /**
     * 合并两个状态快照
     */
    private ClusterStateSnapshot mergeStateSnapshots(ClusterStateSnapshot current, 
                                                     ClusterStateSnapshot incoming) {
        Map<String, NodeStateInfo> mergedStates = new HashMap<>(current.getNodeStates());
        
        // 合并节点状态（以最新心跳时间为准）
        for (Map.Entry<String, NodeStateInfo> entry : incoming.getNodeStates().entrySet()) {
            String nodeId = entry.getKey();
            NodeStateInfo incomingState = entry.getValue();
            
            NodeStateInfo currentState = mergedStates.get(nodeId);
            if (currentState == null || 
                incomingState.getLastHeartbeat().isAfter(currentState.getLastHeartbeat())) {
                mergedStates.put(nodeId, incomingState);
            }
        }
        
        // 生成合并后的快照
        String mergedSnapshotId = "merged-" + System.currentTimeMillis();
        long mergedVersion = Math.max(current.getVersion(), incoming.getVersion());
        
        return new ClusterStateSnapshot(
            mergedSnapshotId, mergedVersion, Instant.now(), 
            mergedStates, generateChecksum(mergedStates)
        );
    }
    
    /**
     * 检测和解决状态冲突
     */
    private void resolveStateConflicts(ClusterStateSnapshot incomingSnapshot) {
        ClusterStateSnapshot currentSnapshot = getCurrentStateSnapshot();
        if (currentSnapshot == null) return;
        
        // 检测冲突节点
        List<String> conflictNodes = new ArrayList<>();
        
        for (String nodeId : incomingSnapshot.getNodeStates().keySet()) {
            if (currentSnapshot.getNodeStates().containsKey(nodeId)) {
                NodeStateInfo currentState = currentSnapshot.getNodeStates().get(nodeId);
                NodeStateInfo incomingState = incomingSnapshot.getNodeStates().get(nodeId);
                
                if (hasStateConflict(currentState, incomingState)) {
                    conflictNodes.add(nodeId);
                }
            }
        }
        
        if (!conflictNodes.isEmpty()) {
            log.warn("检测到状态冲突，涉及节点: {}", conflictNodes);
            
            // 记录冲突并尝试解决
            for (String nodeId : conflictNodes) {
                recordStateConflict(nodeId, currentSnapshot, incomingSnapshot);
                resolveNodeStateConflict(nodeId, currentSnapshot, incomingSnapshot);
            }
        }
    }
    
    /**
     * 检查是否存在状态冲突
     */
    private boolean hasStateConflict(NodeStateInfo current, NodeStateInfo incoming) {
        // 检查关键状态是否一致
        return !current.getNodeState().equals(incoming.getNodeState()) ||
               !current.getServiceInfo().getHealthStatus().equals(incoming.getServiceInfo().getHealthStatus()) ||
               Math.abs(current.getLastHeartbeat().toEpochMilli() - incoming.getLastHeartbeat().toEpochMilli()) > 10000;
    }
    
    /**
     * 记录状态冲突
     */
    private void recordStateConflict(String nodeId, ClusterStateSnapshot current, 
                                   ClusterStateSnapshot incoming) {
        StateConflictRecord record = new StateConflictRecord(
            nodeId, Instant.now(), current.getSnapshotId(), incoming.getSnapshotId(),
            current.getNodeStates().get(nodeId), incoming.getNodeStates().get(nodeId)
        );
        
        conflictRecords.put(nodeId + "-" + System.currentTimeMillis(), record);
    }
    
    /**
     * 解决节点状态冲突
     */
    private void resolveNodeStateConflict(String nodeId, ClusterStateSnapshot current, 
                                         ClusterStateSnapshot incoming) {
        NodeStateInfo currentState = current.getNodeStates().get(nodeId);
        NodeStateInfo incomingState = incoming.getNodeStates().get(nodeId);
        
        // 冲突解决策略：以最新心跳时间为准
        NodeStateInfo resolvedState;
        if (incomingState.getLastHeartbeat().isAfter(currentState.getLastHeartbeat())) {
            resolvedState = incomingState;
            log.info("状态冲突解决：采用更新的状态 nodeId={}", nodeId);
        } else {
            resolvedState = currentState;
            log.info("状态冲突解决：保持当前状态 nodeId={}", nodeId);
        }
        
        // 更新本地集群状态
        updateNodeStateInCluster(nodeId, resolvedState);
    }
    
    /**
     * 更新本地集群状态
     */
    private void updateLocalClusterState(ClusterStateSnapshot snapshot) {
        for (NodeStateInfo nodeState : snapshot.getNodeStates().values()) {
            updateNodeStateInCluster(nodeState.getNodeId(), nodeState);
        }
    }
    
    /**
     * 更新集群中的节点状态
     */
    private void updateNodeStateInCluster(String nodeId, NodeStateInfo stateInfo) {
        // 更新NodeManager中的节点信息
        nodeManager.updateNodeState(nodeId, stateInfo.getNodeState());
        
        // 更新服务注册表
        serviceRegistry.heartbeat(nodeId, stateInfo.getServiceInfo().getHealthStatus());
    }
    
    /**
     * 获取当前状态快照
     */
    private ClusterStateSnapshot getCurrentStateSnapshot() {
        return stateSnapshots.values().stream()
            .max(Comparator.comparing(ClusterStateSnapshot::getVersion))
            .orElse(null);
    }
    
    /**
     * 清理过期状态
     */
    private void cleanupExpiredStates() {
        Instant cutoff = Instant.now().minusSeconds(STATE_TTL_MINUTES * 60);
        
        stateSnapshots.entrySet().removeIf(entry -> 
            entry.getValue().getTimestamp().isBefore(cutoff));
        
        conflictRecords.entrySet().removeIf(entry ->
            entry.getValue().getTimestamp().isBefore(cutoff));
    }
    
    /**
     * 清理旧的状态快照
     */
    private void cleanupOldSnapshots() {
        if (stateSnapshots.size() <= MAX_STATE_HISTORY) return;
        
        List<ClusterStateSnapshot> sortedSnapshots = stateSnapshots.values().stream()
            .sorted(Comparator.comparing(ClusterStateSnapshot::getTimestamp))
            .collect(ArrayList::new, ArrayList::add, ArrayList::addAll);
        
        int toRemove = stateSnapshots.size() - MAX_STATE_HISTORY;
        for (int i = 0; i < toRemove; i++) {
            stateSnapshots.remove(sortedSnapshots.get(i).getSnapshotId());
        }
    }
    
    /**
     * 记录Gossip传播
     */
    private void recordGossipPropagation(String targetNodeId, String snapshotId) {
        gossipLog.computeIfAbsent(targetNodeId, k -> ConcurrentHashMap.newKeySet())
                .add(snapshotId);
    }
    
    /**
     * 生成状态校验和
     */
    private String generateChecksum(Map<String, NodeStateInfo> nodeStates) {
        // 简单的校验和实现（实际应用中可以使用更复杂的哈希算法）
        int hash = 0;
        for (NodeStateInfo state : nodeStates.values()) {
            hash += state.getNodeId().hashCode();
            hash += state.getNodeState().hashCode();
            hash += state.getServiceInfo().getHealthStatus().hashCode();
        }
        return String.valueOf(Math.abs(hash));
    }
    
    // 工具方法
    private boolean isLocalNode(String nodeId) {
        // 检查是否是本地节点
        return false; // TODO: 实现本地节点检查逻辑
    }
    
    private NodeRole determineNodeRole(String serviceName) {
        if (serviceName.contains("frontend")) return NodeRole.FRONTEND_NODE;
        if (serviceName.contains("backend")) return NodeRole.BACKEND_NODE;
        if (serviceName.contains("coordinator")) return NodeRole.COORDINATOR_NODE;
        if (serviceName.contains("load-balancer")) return NodeRole.LOAD_BALANCER;
        return NodeRole.UNKNOWN_ROLE;
    }
    
    private Map<String, Object> extractMetricsFromMetadata(Map<String, String> metadata) {
        Map<String, Object> metrics = new HashMap<>();
        metrics.put("cpu_usage", parseDoubleOrDefault(metadata.get("cpu_usage"), 0.0));
        metrics.put("memory_usage", parseDoubleOrDefault(metadata.get("memory_usage"), 0.0));
        metrics.put("connection_count", parseLongOrDefault(metadata.get("connection_count"), 0L));
        return metrics;
    }
    
    private double parseDoubleOrDefault(String value, double defaultValue) {
        try {
            return value != null ? Double.parseDouble(value) : defaultValue;
        } catch (NumberFormatException e) {
            return defaultValue;
        }
    }
    
    private long parseLongOrDefault(String value, long defaultValue) {
        try {
            return value != null ? Long.parseLong(value) : defaultValue;
        } catch (NumberFormatException e) {
            return defaultValue;
        }
    }
    
    // 数据类定义
    @Data
    public static class ClusterStateSnapshot {
        private String snapshotId;
        private long version;
        private Instant timestamp;
        private Map<String, NodeStateInfo> nodeStates;
        private String checksum;
        
        public ClusterStateSnapshot(String snapshotId, long version, Instant timestamp,
                                   Map<String, NodeStateInfo> nodeStates, String checksum) {
            this.snapshotId = snapshotId;
            this.version = version;
            this.timestamp = timestamp;
            this.nodeStates = nodeStates;
            this.checksum = checksum;
        }
    }
    
    @Data
    public static class NodeStateInfo {
        private String nodeId;
        private NodeRole role;
        private com.vmqtt.common.grpc.cluster.NodeState nodeState;
        private ServiceInfo serviceInfo;
        private Instant lastHeartbeat;
        private Map<String, Object> metrics;
        
        public NodeStateInfo(String nodeId, NodeRole role, 
                           com.vmqtt.common.grpc.cluster.NodeState nodeState,
                           ServiceInfo serviceInfo, Instant lastHeartbeat,
                           Map<String, Object> metrics) {
            this.nodeId = nodeId;
            this.role = role;
            this.nodeState = nodeState;
            this.serviceInfo = serviceInfo;
            this.lastHeartbeat = lastHeartbeat;
            this.metrics = metrics;
        }
    }
    
    @Data
    public static class StateConflictRecord {
        private String nodeId;
        private Instant timestamp;
        private String currentSnapshotId;
        private String incomingSnapshotId;
        private NodeStateInfo currentState;
        private NodeStateInfo incomingState;
        
        public StateConflictRecord(String nodeId, Instant timestamp,
                                 String currentSnapshotId, String incomingSnapshotId,
                                 NodeStateInfo currentState, NodeStateInfo incomingState) {
            this.nodeId = nodeId;
            this.timestamp = timestamp;
            this.currentSnapshotId = currentSnapshotId;
            this.incomingSnapshotId = incomingSnapshotId;
            this.currentState = currentState;
            this.incomingState = incomingState;
        }
    }
}