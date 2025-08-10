package com.vmqtt.vmqttcore.cluster;

import com.vmqtt.common.grpc.cluster.ServiceInfo;
import com.vmqtt.vmqttcore.service.router.LoadBalancer;
import lombok.Data;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.time.Instant;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

/**
 * 集群感知负载均衡器
 * 扩展现有LoadBalancer，支持集群节点的智能负载均衡
 *
 * @author zhenglin
 * @date 2025/08/10
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class ClusterAwareLoadBalancer implements LoadBalancer<ServiceInfo> {

    private final ServiceRegistry serviceRegistry;
    private final ClusterRoutingOptimizer routingOptimizer;
    
    // 负载均衡策略
    private volatile LoadBalanceStrategy currentStrategy = LoadBalanceStrategy.WEIGHTED_ROUND_ROBIN;
    
    // 轮询计数器
    private final ConcurrentHashMap<String, AtomicInteger> roundRobinCounters = new ConcurrentHashMap<>();
    
    // 节点权重和状态缓存
    private final ConcurrentHashMap<String, NodeWeight> nodeWeights = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, NodeHealthStatus> nodeHealthStatus = new ConcurrentHashMap<>();
    
    // 一致性哈希环（用于一致性哈希策略）
    private final TreeMap<Long, ServiceInfo> hashRing = new TreeMap<>();
    private volatile boolean hashRingNeedsRebuild = true;
    
    // 统计信息
    private final AtomicLong totalSelections = new AtomicLong(0);
    private final ConcurrentHashMap<String, AtomicLong> nodeSelectionCounts = new ConcurrentHashMap<>();
    
    @Override
    public ServiceInfo select(List<ServiceInfo> candidates, String key) {
        if (candidates == null || candidates.isEmpty()) {
            log.warn("No candidates available for load balancing");
            return null;
        }
        
        totalSelections.incrementAndGet();
        
        // 过滤健康的节点
        List<ServiceInfo> healthyNodes = filterHealthyNodes(candidates);
        if (healthyNodes.isEmpty()) {
            log.warn("No healthy nodes available, falling back to all candidates");
            healthyNodes = candidates;
        }
        
        ServiceInfo selected;
        
        try {
            switch (currentStrategy) {
                case ROUND_ROBIN:
                    selected = selectRoundRobin(healthyNodes, key);
                    break;
                case WEIGHTED_ROUND_ROBIN:
                    selected = selectWeightedRoundRobin(healthyNodes, key);
                    break;
                case LEAST_CONNECTIONS:
                    selected = selectLeastConnections(healthyNodes, key);
                    break;
                case LEAST_RESPONSE_TIME:
                    selected = selectLeastResponseTime(healthyNodes, key);
                    break;
                case CONSISTENT_HASH:
                    selected = selectConsistentHash(healthyNodes, key);
                    break;
                case RANDOM:
                    selected = selectRandom(healthyNodes, key);
                    break;
                default:
                    selected = selectWeightedRoundRobin(healthyNodes, key);
                    break;
            }
            
            if (selected != null) {
                updateSelectionStats(selected);
                log.debug("Selected node: {} using strategy: {}", selected.getNodeId(), currentStrategy);
            }
            
            return selected;
            
        } catch (Exception e) {
            log.error("Failed to select node using strategy: {}, falling back to random selection", 
                currentStrategy, e);
            return selectRandom(healthyNodes, key);
        }
    }
    
    /**
     * 设置负载均衡策略
     *
     * @param strategy 负载均衡策略
     */
    public void setStrategy(LoadBalanceStrategy strategy) {
        if (this.currentStrategy != strategy) {
            log.info("Changing load balance strategy from {} to {}", this.currentStrategy, strategy);
            this.currentStrategy = strategy;
            
            // 如果切换到一致性哈希，需要重建哈希环
            if (strategy == LoadBalanceStrategy.CONSISTENT_HASH) {
                hashRingNeedsRebuild = true;
            }
        }
    }
    
    /**
     * 更新节点权重
     *
     * @param nodeId 节点ID
     * @param weight 权重值
     */
    public void updateNodeWeight(String nodeId, int weight) {
        NodeWeight nodeWeight = nodeWeights.computeIfAbsent(nodeId, k -> new NodeWeight(k, 1));
        nodeWeight.setWeight(weight);
        nodeWeight.setLastUpdated(Instant.now());
        
        log.debug("Updated node weight: {} = {}", nodeId, weight);
    }
    
    /**
     * 更新节点健康状态
     *
     * @param nodeId 节点ID
     * @param healthy 是否健康
     * @param reason 状态变更原因
     */
    public void updateNodeHealth(String nodeId, boolean healthy, String reason) {
        NodeHealthStatus status = nodeHealthStatus.computeIfAbsent(
            nodeId, k -> new NodeHealthStatus(k, true, ""));
        
        if (status.isHealthy() != healthy) {
            log.info("Node health status changed: {} -> {}, reason: {}", 
                nodeId, healthy ? "HEALTHY" : "UNHEALTHY", reason);
        }
        
        status.setHealthy(healthy);
        status.setReason(reason);
        status.setLastUpdated(Instant.now());
        
        // 如果节点状态改变且使用一致性哈希，需要重建哈希环
        if (currentStrategy == LoadBalanceStrategy.CONSISTENT_HASH) {
            hashRingNeedsRebuild = true;
        }
    }
    
    /**
     * 轮询选择
     */
    private ServiceInfo selectRoundRobin(List<ServiceInfo> nodes, String key) {
        if (nodes.size() == 1) {
            return nodes.get(0);
        }
        
        String counterKey = "rr_" + nodes.stream()
            .map(ServiceInfo::getNodeId)
            .sorted()
            .collect(Collectors.joining(","));
            
        AtomicInteger counter = roundRobinCounters.computeIfAbsent(counterKey, k -> new AtomicInteger(0));
        int index = counter.getAndIncrement() % nodes.size();
        
        return nodes.get(index);
    }
    
    /**
     * 加权轮询选择
     */
    private ServiceInfo selectWeightedRoundRobin(List<ServiceInfo> nodes, String key) {
        if (nodes.size() == 1) {
            return nodes.get(0);
        }
        
        // 计算总权重
        int totalWeight = 0;
        for (ServiceInfo node : nodes) {
            int weight = getNodeWeight(node.getNodeId());
            totalWeight += weight;
        }
        
        if (totalWeight == 0) {
            return selectRoundRobin(nodes, key); // 退化为普通轮询
        }
        
        // 使用权重进行选择
        String counterKey = "wrr_" + nodes.stream()
            .map(ServiceInfo::getNodeId)
            .sorted()
            .collect(Collectors.joining(","));
            
        AtomicInteger counter = roundRobinCounters.computeIfAbsent(counterKey, k -> new AtomicInteger(0));
        int target = counter.getAndIncrement() % totalWeight;
        
        int currentWeight = 0;
        for (ServiceInfo node : nodes) {
            currentWeight += getNodeWeight(node.getNodeId());
            if (currentWeight > target) {
                return node;
            }
        }
        
        // 理论上不应该到达这里
        return nodes.get(nodes.size() - 1);
    }
    
    /**
     * 最少连接选择
     */
    private ServiceInfo selectLeastConnections(List<ServiceInfo> nodes, String key) {
        return nodes.stream()
            .min(Comparator.comparingLong(node -> getNodeConnections(node.getNodeId())))
            .orElse(nodes.get(0));
    }
    
    /**
     * 最短响应时间选择
     */
    private ServiceInfo selectLeastResponseTime(List<ServiceInfo> nodes, String key) {
        return nodes.stream()
            .min(Comparator.comparingDouble(node -> 
                routingOptimizer.getNodePerformanceScore(node.getNodeId())))
            .orElse(nodes.get(0));
    }
    
    /**
     * 一致性哈希选择
     */
    private ServiceInfo selectConsistentHash(List<ServiceInfo> nodes, String key) {
        if (key == null || key.isEmpty()) {
            return selectRandom(nodes, key);
        }
        
        // 重建哈希环（如果需要）
        if (hashRingNeedsRebuild) {
            rebuildHashRing(nodes);
        }
        
        if (hashRing.isEmpty()) {
            return selectRandom(nodes, key);
        }
        
        long hash = hash(key);
        Map.Entry<Long, ServiceInfo> entry = hashRing.ceilingEntry(hash);
        
        if (entry == null) {
            // 环形特性，如果没有找到大于等于hash值的节点，选择第一个节点
            entry = hashRing.firstEntry();
        }
        
        return entry.getValue();
    }
    
    /**
     * 随机选择
     */
    private ServiceInfo selectRandom(List<ServiceInfo> nodes, String key) {
        int index = ThreadLocalRandom.current().nextInt(nodes.size());
        return nodes.get(index);
    }
    
    /**
     * 过滤健康节点
     */
    private List<ServiceInfo> filterHealthyNodes(List<ServiceInfo> nodes) {
        return nodes.stream()
            .filter(node -> {
                NodeHealthStatus status = nodeHealthStatus.get(node.getNodeId());
                return status == null || status.isHealthy();
            })
            .collect(Collectors.toList());
    }
    
    /**
     * 获取节点权重
     */
    private int getNodeWeight(String nodeId) {
        NodeWeight weight = nodeWeights.get(nodeId);
        return weight != null ? weight.getWeight() : 1;
    }
    
    /**
     * 获取节点连接数（模拟实现）
     */
    private long getNodeConnections(String nodeId) {
        // 实际应该从监控系统获取真实连接数
        return ThreadLocalRandom.current().nextLong(0, 1000);
    }
    
    /**
     * 重建一致性哈希环
     */
    private synchronized void rebuildHashRing(List<ServiceInfo> nodes) {
        if (!hashRingNeedsRebuild) {
            return;
        }
        
        log.debug("Rebuilding consistent hash ring with {} nodes", nodes.size());
        
        hashRing.clear();
        
        // 为每个节点创建多个虚拟节点
        int virtualNodes = 160; // 每个物理节点对应160个虚拟节点
        
        for (ServiceInfo node : nodes) {
            for (int i = 0; i < virtualNodes; i++) {
                String virtualNodeKey = node.getNodeId() + "#" + i;
                long hash = hash(virtualNodeKey);
                hashRing.put(hash, node);
            }
        }
        
        hashRingNeedsRebuild = false;
        log.debug("Hash ring rebuilt with {} virtual nodes", hashRing.size());
    }
    
    /**
     * 哈希函数
     */
    private long hash(String key) {
        // 简单的FNV-1a哈希
        long hash = 2166136261L;
        for (byte b : key.getBytes()) {
            hash ^= b;
            hash *= 16777619L;
        }
        return hash;
    }
    
    /**
     * 更新选择统计
     */
    private void updateSelectionStats(ServiceInfo selected) {
        String nodeId = selected.getNodeId();
        nodeSelectionCounts.computeIfAbsent(nodeId, k -> new AtomicLong(0)).incrementAndGet();
    }
    
    /**
     * 获取负载均衡统计信息
     */
    public LoadBalanceStats getStats() {
        Map<String, Long> selectionCounts = nodeSelectionCounts.entrySet().stream()
            .collect(Collectors.toMap(
                Map.Entry::getKey,
                entry -> entry.getValue().get()
            ));
            
        return new LoadBalanceStats(
            currentStrategy,
            totalSelections.get(),
            selectionCounts,
            nodeWeights.size(),
            hashRing.size() / 160, // 物理节点数
            System.currentTimeMillis()
        );
    }
    
    /**
     * 获取节点状态信息
     */
    public Map<String, NodeStatus> getNodeStatus() {
        Map<String, NodeStatus> status = new HashMap<>();
        
        // 合并权重和健康状态信息
        Set<String> allNodeIds = new HashSet<>();
        allNodeIds.addAll(nodeWeights.keySet());
        allNodeIds.addAll(nodeHealthStatus.keySet());
        
        for (String nodeId : allNodeIds) {
            NodeWeight weight = nodeWeights.get(nodeId);
            NodeHealthStatus health = nodeHealthStatus.get(nodeId);
            
            NodeStatus nodeStatus = new NodeStatus(
                nodeId,
                weight != null ? weight.getWeight() : 1,
                health != null ? health.isHealthy() : true,
                health != null ? health.getReason() : "",
                nodeSelectionCounts.getOrDefault(nodeId, new AtomicLong(0)).get(),
                routingOptimizer.getNodePerformanceScore(nodeId)
            );
            
            status.put(nodeId, nodeStatus);
        }
        
        return status;
    }
    
    // ========== 数据类定义 ==========
    
    /**
     * 负载均衡策略枚举
     */
    public enum LoadBalanceStrategy {
        ROUND_ROBIN,
        WEIGHTED_ROUND_ROBIN,
        LEAST_CONNECTIONS,
        LEAST_RESPONSE_TIME,
        CONSISTENT_HASH,
        RANDOM
    }
    
    /**
     * 节点权重信息
     */
    @Data
    private static class NodeWeight {
        private final String nodeId;
        private volatile int weight;
        private volatile Instant lastUpdated;
        
        public NodeWeight(String nodeId, int weight) {
            this.nodeId = nodeId;
            this.weight = weight;
            this.lastUpdated = Instant.now();
        }
    }
    
    /**
     * 节点健康状态
     */
    @Data
    private static class NodeHealthStatus {
        private final String nodeId;
        private volatile boolean healthy;
        private volatile String reason;
        private volatile Instant lastUpdated;
        
        public NodeHealthStatus(String nodeId, boolean healthy, String reason) {
            this.nodeId = nodeId;
            this.healthy = healthy;
            this.reason = reason;
            this.lastUpdated = Instant.now();
        }
    }
    
    /**
     * 负载均衡统计信息
     */
    @Data
    public static class LoadBalanceStats {
        private final LoadBalanceStrategy strategy;
        private final long totalSelections;
        private final Map<String, Long> selectionsByNode;
        private final int managedNodes;
        private final int hashRingSize;
        private final long timestamp;
    }
    
    /**
     * 节点状态信息
     */
    @Data
    public static class NodeStatus {
        private final String nodeId;
        private final int weight;
        private final boolean healthy;
        private final String healthReason;
        private final long selectionCount;
        private final double performanceScore;
    }
}