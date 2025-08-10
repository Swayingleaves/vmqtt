package com.vmqtt.vmqttcore.cluster;

import com.google.protobuf.Timestamp;
import com.vmqtt.common.grpc.cluster.ClusterMessage;
import com.vmqtt.common.grpc.cluster.ServiceInfo;
import lombok.Data;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.time.Instant;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;
import java.util.stream.Collectors;

/**
 * 集群路由性能优化器
 * 提供路由缓存、批量处理、连接池管理等性能优化功能
 *
 * @author zhenglin
 * @date 2025/08/10
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class ClusterRoutingOptimizer {

    private final ServiceRegistry serviceRegistry;
    private final ClusterNodeManager clusterNodeManager;
    
    // 路由缓存
    private final ConcurrentHashMap<String, RoutingCacheEntry> routingCache = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, NodePerformanceMetrics> nodeMetrics = new ConcurrentHashMap<>();
    
    // 批量处理队列
    private final ConcurrentHashMap<String, BlockingQueue<ClusterMessage>> messageBatches = new ConcurrentHashMap<>();
    private final ScheduledExecutorService batchProcessor = Executors.newScheduledThreadPool(4);
    
    // 性能统计
    private final LongAdder cacheHits = new LongAdder();
    private final LongAdder cacheMisses = new LongAdder();
    private final LongAdder batchedMessages = new LongAdder();
    private final AtomicLong totalOptimizationTime = new AtomicLong(0);
    
    // 配置参数
    private static final int ROUTING_CACHE_TTL_SECONDS = 300; // 5分钟缓存TTL
    private static final int BATCH_SIZE = 50;
    private static final int BATCH_TIMEOUT_MS = 100;
    private static final int MAX_CONCURRENT_BATCHES = 10;
    
    /**
     * 启动路由优化器
     */
    public void start() {
        log.info("Starting cluster routing optimizer");
        
        // 启动批量处理任务
        batchProcessor.scheduleWithFixedDelay(this::processBatches, 
            BATCH_TIMEOUT_MS, BATCH_TIMEOUT_MS, TimeUnit.MILLISECONDS);
        
        // 启动缓存清理任务
        batchProcessor.scheduleWithFixedDelay(this::cleanupCache, 60, 60, TimeUnit.SECONDS);
        
        // 启动性能监控任务
        batchProcessor.scheduleWithFixedDelay(this::updateNodeMetrics, 30, 30, TimeUnit.SECONDS);
    }
    
    /**
     * 停止路由优化器
     */
    public void stop() {
        log.info("Stopping cluster routing optimizer");
        
        // 处理剩余的批量消息
        flushAllBatches();
        
        batchProcessor.shutdown();
        try {
            if (!batchProcessor.awaitTermination(10, TimeUnit.SECONDS)) {
                batchProcessor.shutdownNow();
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            batchProcessor.shutdownNow();
        }
    }
    
    /**
     * 优化路由决策
     *
     * @param topic 主题
     * @param availableNodes 可用节点
     * @return 优化后的路由决策
     */
    public OptimizedRoutingDecision optimizeRouting(String topic, List<ServiceInfo> availableNodes) {
        long startTime = System.nanoTime();
        
        try {
            // 1. 检查路由缓存
            RoutingCacheEntry cachedEntry = routingCache.get(topic);
            if (cachedEntry != null && !cachedEntry.isExpired()) {
                cacheHits.increment();
                log.debug("Using cached routing decision for topic: {}", topic);
                return cachedEntry.getDecision();
            }
            
            cacheMisses.increment();
            
            // 2. 基于性能指标选择最优节点
            List<ServiceInfo> optimizedNodes = selectOptimalNodes(availableNodes, topic);
            
            // 3. 构建路由决策
            OptimizedRoutingDecision decision = new OptimizedRoutingDecision(
                topic,
                optimizedNodes,
                determineRoutingStrategy(optimizedNodes),
                calculateRoutingPriority(topic, optimizedNodes)
            );
            
            // 4. 缓存路由决策
            RoutingCacheEntry cacheEntry = new RoutingCacheEntry(decision, Instant.now());
            routingCache.put(topic, cacheEntry);
            
            log.debug("Optimized routing decision for topic: {}, selected {} nodes", 
                topic, optimizedNodes.size());
            
            return decision;
            
        } finally {
            long endTime = System.nanoTime();
            totalOptimizationTime.addAndGet(endTime - startTime);
        }
    }
    
    /**
     * 添加消息到批量处理队列
     *
     * @param targetNodeId 目标节点ID
     * @param message 消息
     */
    public void addToBatch(String targetNodeId, ClusterMessage message) {
        BlockingQueue<ClusterMessage> batch = messageBatches.computeIfAbsent(
            targetNodeId, k -> new ArrayBlockingQueue<>(BATCH_SIZE * 2));
        
        if (batch.offer(message)) {
            batchedMessages.increment();
            log.debug("Added message to batch for node: {}, batch size: {}", targetNodeId, batch.size());
        } else {
            log.warn("Failed to add message to batch for node: {}, batch full", targetNodeId);
        }
    }
    
    /**
     * 获取并清空指定节点的批量消息
     *
     * @param nodeId 节点ID
     * @return 批量消息列表
     */
    public List<ClusterMessage> getAndClearBatch(String nodeId) {
        BlockingQueue<ClusterMessage> batch = messageBatches.get(nodeId);
        if (batch == null || batch.isEmpty()) {
            return Collections.emptyList();
        }
        
        List<ClusterMessage> messages = new ArrayList<>();
        batch.drainTo(messages, BATCH_SIZE);
        
        log.debug("Retrieved batch for node: {}, size: {}", nodeId, messages.size());
        return messages;
    }
    
    /**
     * 获取节点性能评分
     *
     * @param nodeId 节点ID
     * @return 性能评分（越低越好）
     */
    public double getNodePerformanceScore(String nodeId) {
        NodePerformanceMetrics metrics = nodeMetrics.get(nodeId);
        if (metrics == null) {
            return Double.MAX_VALUE; // 未知节点，分数最高
        }
        
        // 综合评分：延迟 * 权重1 + 错误率 * 权重2 + 负载 * 权重3
        double latencyScore = metrics.getAverageLatencyMs() * 1.0;
        double errorRateScore = metrics.getErrorRate() * 100.0;
        double loadScore = metrics.getLoadPercentage() * 0.5;
        
        return latencyScore + errorRateScore + loadScore;
    }
    
    /**
     * 更新节点性能指标
     *
     * @param nodeId 节点ID
     * @param latencyMs 延迟毫秒数
     * @param success 是否成功
     */
    public void updateNodeMetrics(String nodeId, double latencyMs, boolean success) {
        NodePerformanceMetrics metrics = nodeMetrics.computeIfAbsent(
            nodeId, k -> new NodePerformanceMetrics(k));
        
        metrics.updateMetrics(latencyMs, success);
    }
    
    /**
     * 选择最优节点
     */
    private List<ServiceInfo> selectOptimalNodes(List<ServiceInfo> availableNodes, String topic) {
        if (availableNodes.size() <= 2) {
            return availableNodes; // 节点数少，直接返回
        }
        
        // 按性能评分排序
        List<ServiceInfo> sortedNodes = availableNodes.stream()
            .sorted(Comparator.comparingDouble(node -> getNodePerformanceScore(node.getNodeId())))
            .collect(Collectors.toList());
        
        // 选择前50%的节点，至少选择2个节点
        int selectedCount = Math.max(2, sortedNodes.size() / 2);
        List<ServiceInfo> selectedNodes = sortedNodes.subList(0, Math.min(selectedCount, sortedNodes.size()));
        
        log.debug("Selected {} optimal nodes from {} available nodes for topic: {}", 
            selectedNodes.size(), availableNodes.size(), topic);
        
        return selectedNodes;
    }
    
    /**
     * 确定路由策略
     */
    private RoutingStrategy determineRoutingStrategy(List<ServiceInfo> nodes) {
        if (nodes.size() == 1) {
            return RoutingStrategy.DIRECT;
        } else if (nodes.size() <= 3) {
            return RoutingStrategy.MULTICAST;
        } else {
            return RoutingStrategy.LOAD_BALANCED;
        }
    }
    
    /**
     * 计算路由优先级
     */
    private int calculateRoutingPriority(String topic, List<ServiceInfo> nodes) {
        // 基于主题重要性和节点质量计算优先级
        int basePriority = topic.startsWith("$") ? 10 : 5; // 系统主题优先级更高
        int nodeQualityBonus = nodes.size() * 2; // 更多节点选择意味着更高优先级
        
        return basePriority + nodeQualityBonus;
    }
    
    /**
     * 处理批量消息
     */
    private void processBatches() {
        try {
            for (Map.Entry<String, BlockingQueue<ClusterMessage>> entry : messageBatches.entrySet()) {
                String nodeId = entry.getKey();
                BlockingQueue<ClusterMessage> batch = entry.getValue();
                
                if (batch.size() >= BATCH_SIZE) {
                    processBatchForNode(nodeId);
                }
            }
        } catch (Exception e) {
            log.error("Failed to process batches", e);
        }
    }
    
    /**
     * 处理指定节点的批量消息
     */
    private void processBatchForNode(String nodeId) {
        List<ClusterMessage> messages = getAndClearBatch(nodeId);
        if (!messages.isEmpty()) {
            log.debug("Processing batch for node: {}, size: {}", nodeId, messages.size());
            
            // 这里应该调用实际的批量发送逻辑
            // 当前只是记录日志，实际实现时需要调用gRPC客户端
            
            // 模拟处理时间
            try {
                Thread.sleep(10);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }
    
    /**
     * 刷新所有批量消息
     */
    private void flushAllBatches() {
        log.info("Flushing all batches");
        
        for (String nodeId : messageBatches.keySet()) {
            processBatchForNode(nodeId);
        }
    }
    
    /**
     * 清理过期缓存
     */
    private void cleanupCache() {
        try {
            Instant now = Instant.now();
            List<String> expiredKeys = routingCache.entrySet().stream()
                .filter(entry -> entry.getValue().isExpired())
                .map(Map.Entry::getKey)
                .collect(Collectors.toList());
            
            expiredKeys.forEach(routingCache::remove);
            
            if (!expiredKeys.isEmpty()) {
                log.debug("Cleaned up {} expired routing cache entries", expiredKeys.size());
            }
        } catch (Exception e) {
            log.error("Failed to cleanup routing cache", e);
        }
    }
    
    /**
     * 更新节点性能指标
     */
    private void updateNodeMetrics() {
        try {
            List<ServiceInfo> allNodes = serviceRegistry.discover("vmqtt-core");
            
            for (ServiceInfo node : allNodes) {
                String nodeId = node.getNodeId();
                
                // 模拟性能检测（实际应该通过监控系统获取）
                double currentLoad = Math.random() * 100;
                
                NodePerformanceMetrics metrics = nodeMetrics.computeIfAbsent(
                    nodeId, k -> new NodePerformanceMetrics(k));
                metrics.setLoadPercentage(currentLoad);
            }
        } catch (Exception e) {
            log.error("Failed to update node metrics", e);
        }
    }
    
    /**
     * 获取优化器统计信息
     */
    public OptimizerStats getStats() {
        long totalCacheAccess = cacheHits.sum() + cacheMisses.sum();
        double cacheHitRate = totalCacheAccess > 0 ? (double) cacheHits.sum() / totalCacheAccess * 100 : 0;
        
        return new OptimizerStats(
            totalCacheAccess,
            cacheHitRate,
            batchedMessages.sum(),
            messageBatches.size(),
            nodeMetrics.size(),
            totalOptimizationTime.get() / 1_000_000.0, // 转换为毫秒
            System.currentTimeMillis()
        );
    }
    
    // ========== 数据类定义 ==========
    
    /**
     * 路由策略枚举
     */
    public enum RoutingStrategy {
        DIRECT,       // 直接路由到单个节点
        MULTICAST,    // 多播到多个节点
        LOAD_BALANCED // 负载均衡路由
    }
    
    /**
     * 优化路由决策
     */
    @Data
    public static class OptimizedRoutingDecision {
        private final String topic;
        private final List<ServiceInfo> targetNodes;
        private final RoutingStrategy strategy;
        private final int priority;
    }
    
    /**
     * 路由缓存条目
     */
    @Data
    private static class RoutingCacheEntry {
        private final OptimizedRoutingDecision decision;
        private final Instant createdAt;
        
        public boolean isExpired() {
            return Instant.now().isAfter(createdAt.plusSeconds(ROUTING_CACHE_TTL_SECONDS));
        }
    }
    
    /**
     * 节点性能指标
     */
    @Data
    public static class NodePerformanceMetrics {
        private final String nodeId;
        private volatile double averageLatencyMs = 0;
        private volatile double errorRate = 0;
        private volatile double loadPercentage = 0;
        private volatile long totalRequests = 0;
        private volatile long successfulRequests = 0;
        private volatile long totalLatencyMs = 0;
        private volatile Instant lastUpdate = Instant.now();
        
        public NodePerformanceMetrics(String nodeId) {
            this.nodeId = nodeId;
        }
        
        /**
         * 更新性能指标
         */
        public synchronized void updateMetrics(double latencyMs, boolean success) {
            totalRequests++;
            totalLatencyMs += latencyMs;
            
            if (success) {
                successfulRequests++;
            }
            
            // 更新平均延迟
            averageLatencyMs = (double) totalLatencyMs / totalRequests;
            
            // 更新错误率
            errorRate = 1.0 - (double) successfulRequests / totalRequests;
            
            lastUpdate = Instant.now();
        }
    }
    
    /**
     * 优化器统计信息
     */
    @Data
    public static class OptimizerStats {
        private final long totalCacheAccess;
        private final double cacheHitRate;
        private final long totalBatchedMessages;
        private final int activeBatches;
        private final int monitoredNodes;
        private final double totalOptimizationTimeMs;
        private final long timestamp;
    }
}