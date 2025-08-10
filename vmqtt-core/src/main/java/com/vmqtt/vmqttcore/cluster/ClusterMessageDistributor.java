package com.vmqtt.vmqttcore.cluster;

import com.google.protobuf.Timestamp;
import com.vmqtt.common.grpc.cluster.*;
import com.vmqtt.common.protocol.MqttQos;
import lombok.Data;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.time.Instant;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

/**
 * 集群消息分发器
 * 负责跨节点的消息分发逻辑，支持多种分发策略和性能优化
 *
 * @author zhenglin
 * @date 2025/08/10
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class ClusterMessageDistributor {

    private final ServiceRegistry serviceRegistry;
    private final ClusterNodeManager clusterNodeManager;
    private final ClusterSubscriptionManager subscriptionManager;
    
    // 分发线程池
    private final ExecutorService distributionExecutor = Executors.newFixedThreadPool(
        Math.max(4, Runtime.getRuntime().availableProcessors()));
    
    // 性能统计
    private final AtomicLong totalDistributions = new AtomicLong(0);
    private final AtomicLong successfulDistributions = new AtomicLong(0);
    private final AtomicLong failedDistributions = new AtomicLong(0);
    private final AtomicLong averageLatency = new AtomicLong(0);
    
    // 分发缓存和去重
    private final ConcurrentHashMap<String, DistributionRecord> distributionHistory = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, CompletableFuture<DistributionResult>> pendingDistributions = new ConcurrentHashMap<>();
    
    // 分发配置
    private static final int MAX_RETRIES = 3;
    private static final long RETRY_DELAY_MS = 1000;
    private static final int BATCH_SIZE = 100;
    private static final long DISTRIBUTION_TIMEOUT_MS = 10000; // 10秒超时
    
    /**
     * 分发消息到集群节点
     *
     * @param message 要分发的消息
     * @param strategy 分发策略
     * @return 分发结果
     */
    public CompletableFuture<DistributionResult> distributeMessage(ClusterMessage message, 
                                                                  DistributionStrategy strategy) {
        long startTime = System.nanoTime();
        String distributionId = generateDistributionId(message);
        
        // 检查是否已有相同的分发任务
        CompletableFuture<DistributionResult> pendingResult = pendingDistributions.get(distributionId);
        if (pendingResult != null && !pendingResult.isDone()) {
            log.debug("Reusing pending distribution: {}", distributionId);
            return pendingResult;
        }
        
        CompletableFuture<DistributionResult> distributionFuture = CompletableFuture.supplyAsync(() -> {
            try {
                totalDistributions.incrementAndGet();
                
                log.debug("Starting message distribution: id={}, topic={}, strategy={}", 
                    distributionId, message.getTopic(), strategy);
                
                // 1. 获取目标节点列表
                List<String> targetNodes = getTargetNodes(message, strategy);
                
                if (targetNodes.isEmpty()) {
                    log.debug("No target nodes found for message: {}", message.getTopic());
                    return DistributionResult.success(distributionId, Collections.emptyMap(), 0);
                }
                
                // 2. 执行分发
                Map<String, NodeDistributionResult> nodeResults = performDistribution(message, targetNodes, strategy);
                
                // 3. 计算总体结果
                long endTime = System.nanoTime();
                double latencyMs = (endTime - startTime) / 1_000_000.0;
                updateLatencyStats(latencyMs);
                
                boolean allSuccessful = nodeResults.values().stream().allMatch(NodeDistributionResult::isSuccess);
                if (allSuccessful) {
                    successfulDistributions.incrementAndGet();
                } else {
                    failedDistributions.incrementAndGet();
                }
                
                // 4. 记录分发历史
                recordDistribution(distributionId, message, nodeResults, latencyMs);
                
                DistributionResult result = new DistributionResult(
                    distributionId, nodeResults, latencyMs, allSuccessful, null);
                
                log.debug("Distribution completed: id={}, success={}, latency={}ms, nodes={}", 
                    distributionId, allSuccessful, latencyMs, nodeResults.size());
                
                return result;
                
            } catch (Exception e) {
                log.error("Distribution failed: id={}, topic={}", distributionId, message.getTopic(), e);
                failedDistributions.incrementAndGet();
                return DistributionResult.failure(distributionId, "Distribution failed: " + e.getMessage());
            }
        }, distributionExecutor).orTimeout(DISTRIBUTION_TIMEOUT_MS, TimeUnit.MILLISECONDS);
        
        // 缓存分发任务
        pendingDistributions.put(distributionId, distributionFuture);
        
        // 清理完成的任务
        distributionFuture.whenComplete((result, throwable) -> {
            pendingDistributions.remove(distributionId);
        });
        
        return distributionFuture;
    }
    
    /**
     * 批量分发消息
     *
     * @param messages 消息列表
     * @param strategy 分发策略
     * @return 批量分发结果
     */
    public CompletableFuture<BatchDistributionResult> distributeMessageBatch(
            List<ClusterMessage> messages, DistributionStrategy strategy) {
        
        return CompletableFuture.supplyAsync(() -> {
            try {
                log.debug("Starting batch distribution: size={}, strategy={}", messages.size(), strategy);
                
                // 分批处理以控制并发
                List<List<ClusterMessage>> batches = partitionList(messages, BATCH_SIZE);
                List<CompletableFuture<List<DistributionResult>>> batchFutures = new ArrayList<>();
                
                for (List<ClusterMessage> batch : batches) {
                    CompletableFuture<List<DistributionResult>> batchFuture = processBatch(batch, strategy);
                    batchFutures.add(batchFuture);
                }
                
                // 等待所有批次完成
                CompletableFuture<Void> allBatches = CompletableFuture.allOf(
                    batchFutures.toArray(new CompletableFuture[0]));
                
                return allBatches.thenApply(v -> {
                    List<DistributionResult> allResults = batchFutures.stream()
                        .map(CompletableFuture::join)
                        .flatMap(List::stream)
                        .collect(Collectors.toList());
                    
                    boolean allSuccessful = allResults.stream().allMatch(DistributionResult::isSuccess);
                    return new BatchDistributionResult(allResults, allSuccessful, null);
                }).join();
                
            } catch (Exception e) {
                log.error("Batch distribution failed", e);
                return new BatchDistributionResult(Collections.emptyList(), false, 
                    "Batch distribution failed: " + e.getMessage());
            }
        }, distributionExecutor);
    }
    
    /**
     * 处理单个批次
     */
    private CompletableFuture<List<DistributionResult>> processBatch(List<ClusterMessage> batch, 
                                                                    DistributionStrategy strategy) {
        List<CompletableFuture<DistributionResult>> futures = batch.stream()
            .map(message -> distributeMessage(message, strategy))
            .collect(Collectors.toList());
        
        CompletableFuture<Void> allFutures = CompletableFuture.allOf(
            futures.toArray(new CompletableFuture[0]));
        
        return allFutures.thenApply(v -> futures.stream()
            .map(CompletableFuture::join)
            .collect(Collectors.toList()));
    }
    
    /**
     * 获取目标节点列表
     */
    private List<String> getTargetNodes(ClusterMessage message, DistributionStrategy strategy) {
        // 获取有订阅者的节点
        List<ClusterSubscriptionInfo> subscribers = subscriptionManager.getSubscribersForTopic(message.getTopic());
        Set<String> subscriberNodes = subscribers.stream()
            .map(ClusterSubscriptionInfo::getNodeId)
            .collect(Collectors.toSet());
        
        // 移除当前节点
        String currentNodeId = clusterNodeManager.getCurrentNodeId();
        subscriberNodes.remove(currentNodeId);
        
        List<String> targetNodes = new ArrayList<>(subscriberNodes);
        
        // 根据策略调整目标节点
        switch (strategy) {
            case DISTRIBUTION_SELECTIVE:
                targetNodes = selectOptimalNodes(targetNodes, 1); // 选择1个最优节点
                break;
            case DISTRIBUTION_LOAD_BALANCED:
                targetNodes = selectOptimalNodes(targetNodes, Math.min(2, targetNodes.size())); // 选择最多2个节点
                break;
            case DISTRIBUTION_BROADCAST:
            default:
                // 广播到所有节点，不做调整
                break;
        }
        
        log.debug("Selected target nodes for {}: strategy={}, nodes={}", 
            message.getTopic(), strategy, targetNodes);
        
        return targetNodes;
    }
    
    /**
     * 选择最优节点
     */
    private List<String> selectOptimalNodes(List<String> candidates, int maxNodes) {
        if (candidates.size() <= maxNodes) {
            return candidates;
        }
        
        // 获取节点负载信息并排序
        List<NodeLoadInfo> nodeLoads = candidates.stream()
            .map(this::getNodeLoadInfo)
            .sorted(Comparator.comparingDouble(NodeLoadInfo::getLoad))
            .collect(Collectors.toList());
        
        return nodeLoads.stream()
            .limit(maxNodes)
            .map(NodeLoadInfo::getNodeId)
            .collect(Collectors.toList());
    }
    
    /**
     * 获取节点负载信息
     */
    private NodeLoadInfo getNodeLoadInfo(String nodeId) {
        // 简化实现，实际应从监控系统获取
        double load = Math.random() * 100; // 模拟负载
        return new NodeLoadInfo(nodeId, load, true);
    }
    
    /**
     * 执行分发
     */
    private Map<String, NodeDistributionResult> performDistribution(ClusterMessage message, 
                                                                   List<String> targetNodes, 
                                                                   DistributionStrategy strategy) {
        
        Map<String, NodeDistributionResult> results = new ConcurrentHashMap<>();
        List<CompletableFuture<Void>> distributionTasks = new ArrayList<>();
        
        for (String nodeId : targetNodes) {
            CompletableFuture<Void> task = CompletableFuture.runAsync(() -> {
                NodeDistributionResult result = distributeToNode(message, nodeId);
                results.put(nodeId, result);
            }, distributionExecutor);
            
            distributionTasks.add(task);
        }
        
        // 等待所有分发任务完成
        CompletableFuture<Void> allTasks = CompletableFuture.allOf(
            distributionTasks.toArray(new CompletableFuture[0]));
        
        try {
            allTasks.get(DISTRIBUTION_TIMEOUT_MS, TimeUnit.MILLISECONDS);
        } catch (TimeoutException e) {
            log.warn("Distribution timeout for message: {}", message.getTopic());
            // 标记未完成的节点为失败
            for (String nodeId : targetNodes) {
                results.computeIfAbsent(nodeId, k -> 
                    new NodeDistributionResult(k, false, "Distribution timeout", 0, 0));
            }
        } catch (Exception e) {
            log.error("Distribution execution failed", e);
        }
        
        return results;
    }
    
    /**
     * 分发到单个节点
     */
    private NodeDistributionResult distributeToNode(ClusterMessage message, String nodeId) {
        long startTime = System.nanoTime();
        
        for (int attempt = 1; attempt <= MAX_RETRIES; attempt++) {
            try {
                log.debug("Distributing to node {}, attempt {}: topic={}", nodeId, attempt, message.getTopic());
                
                // 构建分发请求
                DistributeMessageRequest request = DistributeMessageRequest.newBuilder()
                    .setSourceNodeId(clusterNodeManager.getCurrentNodeId())
                    .setMessage(message)
                    .addTargetNodes(nodeId)
                    .setStrategy(DistributionStrategy.DISTRIBUTION_BROADCAST)
                    .build();
                
                // 这里应该通过gRPC调用目标节点
                // 暂时模拟成功，实际实现时需要调用具体的gRPC客户端
                boolean success = simulateDistribution(nodeId, message);
                
                long endTime = System.nanoTime();
                double latencyMs = (endTime - startTime) / 1_000_000.0;
                
                if (success) {
                    log.debug("Distribution successful: node={}, topic={}, latency={}ms", 
                        nodeId, message.getTopic(), latencyMs);
                    return new NodeDistributionResult(nodeId, true, "Success", latencyMs, 1);
                } else {
                    log.warn("Distribution failed: node={}, topic={}, attempt={}", 
                        nodeId, message.getTopic(), attempt);
                }
                
            } catch (Exception e) {
                log.error("Distribution error: node={}, topic={}, attempt={}", 
                    nodeId, message.getTopic(), attempt, e);
            }
            
            // 重试延迟
            if (attempt < MAX_RETRIES) {
                try {
                    Thread.sleep(RETRY_DELAY_MS * attempt);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    break;
                }
            }
        }
        
        long endTime = System.nanoTime();
        double latencyMs = (endTime - startTime) / 1_000_000.0;
        
        log.error("Distribution failed after {} attempts: node={}, topic={}", 
            MAX_RETRIES, nodeId, message.getTopic());
        return new NodeDistributionResult(nodeId, false, "Max retries exceeded", latencyMs, 0);
    }
    
    /**
     * 模拟分发执行
     */
    private boolean simulateDistribution(String nodeId, ClusterMessage message) {
        // 模拟网络延迟
        try {
            Thread.sleep(10 + (int)(Math.random() * 20)); // 10-30ms延迟
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return false;
        }
        
        // 模拟90%成功率
        return Math.random() < 0.9;
    }
    
    /**
     * 记录分发历史
     */
    private void recordDistribution(String distributionId, ClusterMessage message,
                                  Map<String, NodeDistributionResult> results, double latencyMs) {
        DistributionRecord record = new DistributionRecord(
            distributionId,
            message.getTopic(),
            message.getMessageId(),
            results.size(),
            (int) results.values().stream().mapToLong(r -> r.isSuccess() ? 1 : 0).sum(),
            latencyMs,
            Instant.now()
        );
        
        distributionHistory.put(distributionId, record);
        
        // 限制历史记录数量
        if (distributionHistory.size() > 1000) {
            cleanupDistributionHistory();
        }
    }
    
    /**
     * 清理分发历史
     */
    private void cleanupDistributionHistory() {
        long cutoffTime = Instant.now().minusSeconds(3600).toEpochMilli(); // 保留1小时
        
        List<String> expiredRecords = distributionHistory.entrySet().stream()
            .filter(entry -> entry.getValue().getTimestamp().toEpochMilli() < cutoffTime)
            .map(Map.Entry::getKey)
            .collect(Collectors.toList());
        
        expiredRecords.forEach(distributionHistory::remove);
        
        log.debug("Cleaned up {} expired distribution records", expiredRecords.size());
    }
    
    /**
     * 更新延迟统计
     */
    private void updateLatencyStats(double latencyMs) {
        long currentAvg = averageLatency.get();
        long count = totalDistributions.get();
        long newAvg = (long) ((currentAvg * (count - 1) + latencyMs) / count);
        averageLatency.set(newAvg);
    }
    
    /**
     * 生成分发ID
     */
    private String generateDistributionId(ClusterMessage message) {
        return "dist_" + message.getMessageId() + "_" + message.getTopic().hashCode();
    }
    
    /**
     * 分割列表
     */
    private <T> List<List<T>> partitionList(List<T> list, int batchSize) {
        List<List<T>> partitions = new ArrayList<>();
        for (int i = 0; i < list.size(); i += batchSize) {
            partitions.add(list.subList(i, Math.min(i + batchSize, list.size())));
        }
        return partitions;
    }
    
    /**
     * 获取分发统计信息
     */
    public DistributionStats getDistributionStats() {
        return new DistributionStats(
            totalDistributions.get(),
            successfulDistributions.get(),
            failedDistributions.get(),
            averageLatency.get(),
            distributionHistory.size(),
            System.currentTimeMillis()
        );
    }
    
    /**
     * 处理分发消息请求（用于gRPC服务调用）
     *
     * @param request 分发请求
     * @return 分发响应
     */
    public DistributeMessageResponse handleDistributeMessage(DistributeMessageRequest request) {
        try {
            String sourceNodeId = request.getSourceNodeId();
            ClusterMessage message = request.getMessage();
            List<String> targetNodes = request.getTargetNodesList();
            DistributionStrategy strategy = request.getStrategy();
            
            log.debug("Handling distribute message request from node: {} to {} target nodes", 
                sourceNodeId, targetNodes.size());
            
            Map<String, Boolean> results = new HashMap<>();
            
            // 根据分发策略执行分发
            for (String nodeId : targetNodes) {
                try {
                    // 模拟消息分发到节点
                    // 实际实现时应该通过gRPC发送到目标节点
                    log.debug("Distributing message to node: {}", nodeId);
                    results.put(nodeId, true);
                } catch (Exception e) {
                    log.error("Failed to distribute message to node: {}", nodeId, e);
                    results.put(nodeId, false);
                }
            }
            
            return DistributeMessageResponse.newBuilder()
                .setSuccess(true)
                .putAllNodeResults(results)
                .build();
                
        } catch (Exception e) {
            log.error("Failed to handle distribute message", e);
            return DistributeMessageResponse.newBuilder()
                .setSuccess(false)
                .setErrorMessage("Distribution failed: " + e.getMessage())
                .build();
        }
    }

    /**
     * 关闭分发器
     */
    public void shutdown() {
        log.info("Shutting down cluster message distributor");
        distributionExecutor.shutdown();
        try {
            if (!distributionExecutor.awaitTermination(10, TimeUnit.SECONDS)) {
                distributionExecutor.shutdownNow();
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            distributionExecutor.shutdownNow();
        }
    }
    
    // ========== 数据类定义 ==========
    
    @Data
    public static class NodeLoadInfo {
        private final String nodeId;
        private final double load;
        private final boolean healthy;
    }
    
    @Data
    public static class NodeDistributionResult {
        private final String nodeId;
        private final boolean success;
        private final String message;
        private final double latencyMs;
        private final int deliveredCount;
    }
    
    @Data
    public static class DistributionResult {
        private final String distributionId;
        private final Map<String, NodeDistributionResult> nodeResults;
        private final double totalLatencyMs;
        private final boolean success;
        private final String errorMessage;
        
        public static DistributionResult success(String distributionId, 
                                               Map<String, NodeDistributionResult> nodeResults, 
                                               double latencyMs) {
            return new DistributionResult(distributionId, nodeResults, latencyMs, true, null);
        }
        
        public static DistributionResult failure(String distributionId, String errorMessage) {
            return new DistributionResult(distributionId, Collections.emptyMap(), 0, false, errorMessage);
        }
    }
    
    @Data
    public static class BatchDistributionResult {
        private final List<DistributionResult> results;
        private final boolean success;
        private final String errorMessage;
    }
    
    @Data
    public static class DistributionRecord {
        private final String distributionId;
        private final String topic;
        private final String messageId;
        private final int targetNodeCount;
        private final int successfulNodeCount;
        private final double latencyMs;
        private final Instant timestamp;
    }
    
    @Data
    public static class DistributionStats {
        private final long totalDistributions;
        private final long successfulDistributions;
        private final long failedDistributions;
        private final long averageLatencyMs;
        private final int historySize;
        private final long timestamp;
        
        public double getSuccessRate() {
            return totalDistributions > 0 ? (double) successfulDistributions / totalDistributions * 100 : 0;
        }
        
        public double getFailureRate() {
            return totalDistributions > 0 ? (double) failedDistributions / totalDistributions * 100 : 0;
        }
    }
}