package com.vmqtt.vmqttcore.cluster;

import com.vmqtt.common.grpc.cluster.LoadBalanceStrategy;
import com.vmqtt.common.grpc.cluster.LoadInfo;
import com.vmqtt.common.grpc.cluster.NodeRole;
import com.vmqtt.vmqttcore.service.router.LoadBalancer;
import lombok.Data;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.time.Instant;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

/**
 * 动态负载均衡管理器
 * 实现多策略智能负载均衡和自适应权重调整
 *
 * @author zhenglin
 * @date 2025/08/10
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class DynamicLoadBalancerManager {
    
    private final ClusterNodeManager nodeManager;
    private final HealthCheckManager healthCheckManager;
    
    // 负载均衡策略实例
    private final Map<LoadBalanceStrategy, LoadBalancer<ClusterNodeManager.ClusterNode>> strategies = new HashMap<>();
    
    // 节点负载统计: nodeId -> NodeLoadStats
    private final ConcurrentMap<String, NodeLoadStats> nodeLoadStats = new ConcurrentHashMap<>();
    
    // 策略性能统计: strategy -> StrategyMetrics
    private final ConcurrentMap<LoadBalanceStrategy, StrategyMetrics> strategyMetrics = new ConcurrentHashMap<>();
    
    // 动态权重调整记录: nodeId -> WeightAdjustment
    private final ConcurrentMap<String, WeightAdjustment> weightAdjustments = new ConcurrentHashMap<>();
    
    // 负载均衡配置
    private static final double LOAD_THRESHOLD_HIGH = 0.8;    // 高负载阈值
    private static final double LOAD_THRESHOLD_LOW = 0.3;     // 低负载阈值
    private static final int WEIGHT_ADJUSTMENT_INTERVAL = 30; // 权重调整间隔（秒）
    private static final int MIN_WEIGHT = 1;                  // 最小权重
    private static final int MAX_WEIGHT = 100;                // 最大权重
    
    /**
     * 初始化负载均衡策略
     */
    public void initializeLoadBalanceStrategies() {
        strategies.put(LoadBalanceStrategy.ROUND_ROBIN, new RoundRobinLoadBalancer<>());
        strategies.put(LoadBalanceStrategy.WEIGHTED_ROUND_ROBIN, new WeightedRoundRobinLoadBalancer());
        strategies.put(LoadBalanceStrategy.LEAST_CONNECTIONS, new LeastConnectionsLoadBalancer());
        strategies.put(LoadBalanceStrategy.LEAST_RESPONSE_TIME, new LeastResponseTimeLoadBalancer());
        strategies.put(LoadBalanceStrategy.CONSISTENT_HASH, new ConsistentHashLoadBalancer());
        strategies.put(LoadBalanceStrategy.RANDOM, new RandomLoadBalancer<>());
        
        // 初始化策略指标
        for (LoadBalanceStrategy strategy : strategies.keySet()) {
            strategyMetrics.put(strategy, new StrategyMetrics(strategy));
        }
        
        log.info("负载均衡策略初始化完成，共{}种策略", strategies.size());
    }
    
    /**
     * 选择最佳节点
     */
    public ClusterNodeManager.ClusterNode selectBestNode(NodeRole role, String clientKey, 
                                                        LoadBalanceStrategy strategy) {
        List<ClusterNodeManager.ClusterNode> candidates = getHealthyCandidates(role);
        
        if (candidates.isEmpty()) {
            log.warn("没有可用的健康节点: role={}", role);
            return null;
        }
        
        // 更新候选节点的负载统计
        updateCandidateLoadStats(candidates);
        
        // 根据策略选择节点
        LoadBalancer<ClusterNodeManager.ClusterNode> balancer = strategies.get(strategy);
        if (balancer == null) {
            log.warn("未知的负载均衡策略，使用默认轮询策略: {}", strategy);
            balancer = strategies.get(LoadBalanceStrategy.ROUND_ROBIN);
        }
        
        long startTime = System.nanoTime();
        ClusterNodeManager.ClusterNode selectedNode = balancer.select(candidates, clientKey);
        long selectionTime = System.nanoTime() - startTime;
        
        if (selectedNode != null) {
            // 记录选择统计
            recordNodeSelection(selectedNode, strategy, selectionTime);
            updateStrategyMetrics(strategy, true, selectionTime);
        } else {
            updateStrategyMetrics(strategy, false, selectionTime);
        }
        
        return selectedNode;
    }
    
    /**
     * 获取指定角色的健康候选节点
     */
    private List<ClusterNodeManager.ClusterNode> getHealthyCandidates(NodeRole role) {
        List<ClusterNodeManager.ClusterNode> healthyNodes = nodeManager.getHealthyNodesByRole(role);
        
        // 过滤超载节点
        return healthyNodes.stream()
            .filter(this::isNodeLoadAcceptable)
            .collect(Collectors.toList());
    }
    
    /**
     * 检查节点负载是否可接受
     */
    private boolean isNodeLoadAcceptable(ClusterNodeManager.ClusterNode node) {
        NodeLoadStats stats = nodeLoadStats.get(node.getNodeId());
        if (stats == null) {
            return true; // 没有负载统计，认为可接受
        }
        
        // 综合负载评分
        double loadScore = calculateNodeLoadScore(stats);
        return loadScore < LOAD_THRESHOLD_HIGH;
    }
    
    /**
     * 计算节点负载评分（0-1，越高越负载重）
     */
    private double calculateNodeLoadScore(NodeLoadStats stats) {
        LoadInfo loadInfo = stats.getCurrentLoad();
        if (loadInfo == null) {
            return 0.0;
        }
        
        // 权重配置
        double CPU_WEIGHT = 0.4;
        double MEMORY_WEIGHT = 0.3;
        double RESPONSE_TIME_WEIGHT = 0.2;
        double CONNECTION_WEIGHT = 0.1;
        
        // 标准化各项指标（转换为0-1范围）
        double cpuScore = Math.min(loadInfo.getCpuUsage() / 100.0, 1.0);
        double memoryScore = Math.min(loadInfo.getMemoryUsage() / 100.0, 1.0);
        double responseTimeScore = Math.min(loadInfo.getResponseTime() / 1000.0, 1.0); // 假设1秒为最大可接受响应时间
        double connectionScore = Math.min(loadInfo.getConnectionCount() / 1000000.0, 1.0); // 假设100万连接为最大
        
        return cpuScore * CPU_WEIGHT + 
               memoryScore * MEMORY_WEIGHT + 
               responseTimeScore * RESPONSE_TIME_WEIGHT + 
               connectionScore * CONNECTION_WEIGHT;
    }
    
    /**
     * 更新候选节点负载统计
     */
    private void updateCandidateLoadStats(List<ClusterNodeManager.ClusterNode> candidates) {
        for (ClusterNodeManager.ClusterNode node : candidates) {
            updateNodeLoadStats(node);
        }
    }
    
    /**
     * 更新单个节点负载统计
     */
    private void updateNodeLoadStats(ClusterNodeManager.ClusterNode node) {
        String nodeId = node.getNodeId();
        NodeLoadStats stats = nodeLoadStats.computeIfAbsent(nodeId, k -> new NodeLoadStats(nodeId));
        
        // 从节点元数据中获取最新负载信息
        LoadInfo loadInfo = extractLoadInfoFromNode(node);
        stats.updateLoad(loadInfo);
    }
    
    /**
     * 从节点提取负载信息
     */
    private LoadInfo extractLoadInfoFromNode(ClusterNodeManager.ClusterNode node) {
        Map<String, String> metadata = node.getServiceInfo().getMetadataMap();
        
        return LoadInfo.newBuilder()
            .setNodeId(node.getNodeId())
            .setCpuUsage(parseDoubleOrDefault(metadata.get("cpu_usage"), 0.0))
            .setMemoryUsage(parseDoubleOrDefault(metadata.get("memory_usage"), 0.0))
            .setConnectionCount(parseLongOrDefault(metadata.get("connection_count"), 0L))
            .setMessageRate(parseLongOrDefault(metadata.get("message_rate"), 0L))
            .setResponseTime(parseDoubleOrDefault(metadata.get("response_time"), 0.0))
            .setActiveThreads((int) parseLongOrDefault(metadata.get("active_threads"), 0L))
            .build();
    }
    
    /**
     * 记录节点选择统计
     */
    private void recordNodeSelection(ClusterNodeManager.ClusterNode node, 
                                    LoadBalanceStrategy strategy, long selectionTime) {
        NodeLoadStats stats = nodeLoadStats.get(node.getNodeId());
        if (stats != null) {
            stats.recordSelection(strategy, selectionTime);
        }
    }
    
    /**
     * 更新策略指标
     */
    private void updateStrategyMetrics(LoadBalanceStrategy strategy, boolean success, long selectionTime) {
        StrategyMetrics metrics = strategyMetrics.get(strategy);
        if (metrics != null) {
            metrics.recordSelection(success, selectionTime);
        }
    }
    
    /**
     * 定期执行权重调整
     */
    @Scheduled(fixedRate = WEIGHT_ADJUSTMENT_INTERVAL * 1000)
    public void performDynamicWeightAdjustment() {
        try {
            log.debug("开始执行动态权重调整");
            
            // 获取所有节点的负载统计
            Map<String, NodeLoadStats> currentStats = new HashMap<>(nodeLoadStats);
            
            if (currentStats.isEmpty()) {
                log.debug("没有节点负载统计，跳过权重调整");
                return;
            }
            
            // 计算权重调整建议
            Map<String, Integer> weightAdjustments = calculateWeightAdjustments(currentStats);
            
            // 应用权重调整
            for (Map.Entry<String, Integer> entry : weightAdjustments.entrySet()) {
                applyWeightAdjustment(entry.getKey(), entry.getValue());
            }
            
            log.info("动态权重调整完成，调整了{}个节点", weightAdjustments.size());
            
        } catch (Exception e) {
            log.error("动态权重调整执行异常", e);
        }
    }
    
    /**
     * 计算权重调整建议
     */
    private Map<String, Integer> calculateWeightAdjustments(Map<String, NodeLoadStats> currentStats) {
        Map<String, Integer> adjustments = new HashMap<>();
        
        // 计算整体负载情况
        double avgLoadScore = currentStats.values().stream()
            .mapToDouble(stats -> calculateNodeLoadScore(stats))
            .average()
            .orElse(0.5);
        
        for (Map.Entry<String, NodeLoadStats> entry : currentStats.entrySet()) {
            String nodeId = entry.getKey();
            NodeLoadStats stats = entry.getValue();
            
            double nodeLoadScore = calculateNodeLoadScore(stats);
            int currentWeight = getCurrentNodeWeight(nodeId);
            
            int newWeight = calculateNewWeight(currentWeight, nodeLoadScore, avgLoadScore);
            
            if (newWeight != currentWeight) {
                adjustments.put(nodeId, newWeight);
            }
        }
        
        return adjustments;
    }
    
    /**
     * 计算新的权重值
     */
    private int calculateNewWeight(int currentWeight, double nodeLoadScore, double avgLoadScore) {
        // 权重调整策略：
        // 1. 低负载节点增加权重
        // 2. 高负载节点减少权重
        // 3. 调整幅度与负载差异成正比
        
        double loadDiff = nodeLoadScore - avgLoadScore;
        double adjustmentFactor = -loadDiff; // 负载高则因子为负，减少权重
        
        // 限制调整幅度（每次最多调整20%）
        adjustmentFactor = Math.max(-0.2, Math.min(0.2, adjustmentFactor));
        
        int weightDelta = (int) (currentWeight * adjustmentFactor);
        int newWeight = currentWeight + weightDelta;
        
        // 确保权重在合理范围内
        return Math.max(MIN_WEIGHT, Math.min(MAX_WEIGHT, newWeight));
    }
    
    /**
     * 获取节点当前权重
     */
    private int getCurrentNodeWeight(String nodeId) {
        WeightAdjustment adjustment = weightAdjustments.get(nodeId);
        return adjustment != null ? adjustment.getCurrentWeight() : 100; // 默认权重100
    }
    
    /**
     * 应用权重调整
     */
    private void applyWeightAdjustment(String nodeId, int newWeight) {
        WeightAdjustment adjustment = weightAdjustments.computeIfAbsent(nodeId, 
            k -> new WeightAdjustment(nodeId, 100));
        
        int oldWeight = adjustment.getCurrentWeight();
        adjustment.updateWeight(newWeight, "动态负载调整");
        
        log.info("节点权重调整: nodeId={}, {} -> {}, reason={}", 
            nodeId, oldWeight, newWeight, "动态负载调整");
        
        // TODO: 将权重调整应用到实际的负载均衡器中
        // 可以通过更新服务注册表中的元数据来实现
        updateNodeWeightInRegistry(nodeId, newWeight);
    }
    
    /**
     * 更新服务注册表中的节点权重
     */
    private void updateNodeWeightInRegistry(String nodeId, int weight) {
        // TODO: 实现权重更新逻辑
        // 这可能需要更新节点的服务信息元数据
        log.debug("更新服务注册表中的节点权重: nodeId={}, weight={}", nodeId, weight);
    }
    
    /**
     * 获取负载均衡统计信息
     */
    public LoadBalancingStats getLoadBalancingStats() {
        Map<String, NodeLoadStats> nodeStats = new HashMap<>(nodeLoadStats);
        Map<LoadBalanceStrategy, StrategyMetrics> strategyStats = new HashMap<>(strategyMetrics);
        Map<String, WeightAdjustment> weightStats = new HashMap<>(weightAdjustments);
        
        return new LoadBalancingStats(nodeStats, strategyStats, weightStats);
    }
    
    /**
     * 自适应策略选择
     */
    public LoadBalanceStrategy selectOptimalStrategy(NodeRole role, int candidateCount) {
        // 根据候选节点数量和角色特性选择最优策略
        if (candidateCount <= 1) {
            return LoadBalanceStrategy.ROUND_ROBIN;
        }
        
        // 分析历史性能选择最佳策略
        return strategyMetrics.entrySet().stream()
            .filter(entry -> entry.getValue().getTotalSelections().get() > 10) // 至少有10次选择记录
            .max(Comparator.comparing(entry -> entry.getValue().getSuccessRate()))
            .map(Map.Entry::getKey)
            .orElse(LoadBalanceStrategy.WEIGHTED_ROUND_ROBIN); // 默认策略
    }
    
    // 具体负载均衡策略实现
    
    /**
     * 轮询负载均衡器
     */
    public static class RoundRobinLoadBalancer<T> implements LoadBalancer<T> {
        private final AtomicLong counter = new AtomicLong(0);
        
        @Override
        public T select(List<T> candidates, String key) {
            if (candidates.isEmpty()) return null;
            int index = (int) (counter.getAndIncrement() % candidates.size());
            return candidates.get(index);
        }
    }
    
    /**
     * 加权轮询负载均衡器
     */
    public class WeightedRoundRobinLoadBalancer implements LoadBalancer<ClusterNodeManager.ClusterNode> {
        private final Map<String, Integer> currentWeights = new ConcurrentHashMap<>();
        
        @Override
        public ClusterNodeManager.ClusterNode select(List<ClusterNodeManager.ClusterNode> candidates, String key) {
            if (candidates.isEmpty()) return null;
            
            ClusterNodeManager.ClusterNode selected = null;
            int totalWeight = 0;
            
            for (ClusterNodeManager.ClusterNode node : candidates) {
                int weight = getCurrentNodeWeight(node.getNodeId());
                totalWeight += weight;
                
                int currentWeight = currentWeights.getOrDefault(node.getNodeId(), 0) + weight;
                currentWeights.put(node.getNodeId(), currentWeight);
                
                if (selected == null || currentWeight > currentWeights.get(selected.getNodeId())) {
                    selected = node;
                }
            }
            
            if (selected != null) {
                currentWeights.put(selected.getNodeId(), 
                    currentWeights.get(selected.getNodeId()) - totalWeight);
            }
            
            return selected;
        }
    }
    
    /**
     * 最少连接负载均衡器
     */
    public class LeastConnectionsLoadBalancer implements LoadBalancer<ClusterNodeManager.ClusterNode> {
        @Override
        public ClusterNodeManager.ClusterNode select(List<ClusterNodeManager.ClusterNode> candidates, String key) {
            if (candidates.isEmpty()) return null;
            
            return candidates.stream()
                .min(Comparator.comparing(node -> {
                    LoadInfo loadInfo = extractLoadInfoFromNode(node);
                    return loadInfo.getConnectionCount();
                }))
                .orElse(candidates.get(0));
        }
    }
    
    /**
     * 最短响应时间负载均衡器
     */
    public class LeastResponseTimeLoadBalancer implements LoadBalancer<ClusterNodeManager.ClusterNode> {
        @Override
        public ClusterNodeManager.ClusterNode select(List<ClusterNodeManager.ClusterNode> candidates, String key) {
            if (candidates.isEmpty()) return null;
            
            return candidates.stream()
                .min(Comparator.comparing(node -> {
                    LoadInfo loadInfo = extractLoadInfoFromNode(node);
                    return loadInfo.getResponseTime();
                }))
                .orElse(candidates.get(0));
        }
    }
    
    /**
     * 一致性哈希负载均衡器
     */
    public class ConsistentHashLoadBalancer implements LoadBalancer<ClusterNodeManager.ClusterNode> {
        @Override
        public ClusterNodeManager.ClusterNode select(List<ClusterNodeManager.ClusterNode> candidates, String key) {
            if (candidates.isEmpty()) return null;
            if (key == null) key = "";
            
            // 简单的一致性哈希实现
            int hash = Math.abs(key.hashCode());
            int index = hash % candidates.size();
            return candidates.get(index);
        }
    }
    
    /**
     * 随机负载均衡器
     */
    public static class RandomLoadBalancer<T> implements LoadBalancer<T> {
        @Override
        public T select(List<T> candidates, String key) {
            if (candidates.isEmpty()) return null;
            int index = ThreadLocalRandom.current().nextInt(candidates.size());
            return candidates.get(index);
        }
    }
    
    // 工具方法
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
    
    /**
     * 节点负载统计
     */
    @Data
    public static class NodeLoadStats {
        private String nodeId;
        private LoadInfo currentLoad;
        private Map<LoadBalanceStrategy, Long> selectionCounts = new ConcurrentHashMap<>();
        private List<Long> recentSelectionTimes = new ArrayList<>();
        private Instant lastUpdated;
        
        public NodeLoadStats(String nodeId) {
            this.nodeId = nodeId;
            this.lastUpdated = Instant.now();
        }
        
        public void updateLoad(LoadInfo loadInfo) {
            this.currentLoad = loadInfo;
            this.lastUpdated = Instant.now();
        }
        
        public void recordSelection(LoadBalanceStrategy strategy, long selectionTime) {
            selectionCounts.merge(strategy, 1L, Long::sum);
            recentSelectionTimes.add(selectionTime);
            
            // 保持最近100次选择的时间记录
            if (recentSelectionTimes.size() > 100) {
                recentSelectionTimes.remove(0);
            }
        }
        
        public double getAverageSelectionTime() {
            return recentSelectionTimes.stream()
                .mapToLong(Long::longValue)
                .average()
                .orElse(0.0);
        }
    }
    
    /**
     * 策略性能指标
     */
    @Data
    public static class StrategyMetrics {
        private LoadBalanceStrategy strategy;
        private AtomicLong totalSelections = new AtomicLong(0);
        private AtomicLong successfulSelections = new AtomicLong(0);
        private AtomicLong totalSelectionTime = new AtomicLong(0);
        
        public StrategyMetrics(LoadBalanceStrategy strategy) {
            this.strategy = strategy;
        }
        
        public void recordSelection(boolean success, long selectionTime) {
            totalSelections.incrementAndGet();
            if (success) {
                successfulSelections.incrementAndGet();
            }
            totalSelectionTime.addAndGet(selectionTime);
        }
        
        public double getSuccessRate() {
            long total = totalSelections.get();
            return total > 0 ? (double) successfulSelections.get() / total : 0.0;
        }
        
        public double getAverageSelectionTime() {
            long total = totalSelections.get();
            return total > 0 ? (double) totalSelectionTime.get() / total : 0.0;
        }
    }
    
    /**
     * 权重调整记录
     */
    @Data
    public static class WeightAdjustment {
        private String nodeId;
        private int currentWeight;
        private int previousWeight;
        private String adjustmentReason;
        private Instant lastAdjustment;
        
        public WeightAdjustment(String nodeId, int initialWeight) {
            this.nodeId = nodeId;
            this.currentWeight = initialWeight;
            this.previousWeight = initialWeight;
            this.lastAdjustment = Instant.now();
        }
        
        public void updateWeight(int newWeight, String reason) {
            this.previousWeight = this.currentWeight;
            this.currentWeight = newWeight;
            this.adjustmentReason = reason;
            this.lastAdjustment = Instant.now();
        }
    }
    
    /**
     * 负载均衡统计信息
     */
    @Data
    public static class LoadBalancingStats {
        private Map<String, NodeLoadStats> nodeStats;
        private Map<LoadBalanceStrategy, StrategyMetrics> strategyMetrics;
        private Map<String, WeightAdjustment> weightAdjustments;
        
        public LoadBalancingStats(Map<String, NodeLoadStats> nodeStats,
                                 Map<LoadBalanceStrategy, StrategyMetrics> strategyMetrics,
                                 Map<String, WeightAdjustment> weightAdjustments) {
            this.nodeStats = nodeStats;
            this.strategyMetrics = strategyMetrics;
            this.weightAdjustments = weightAdjustments;
        }
    }
}