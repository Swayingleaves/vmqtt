package com.vmqtt.vmqttcore.cluster;

import com.google.protobuf.Timestamp;
import com.vmqtt.common.grpc.cluster.*;
import com.vmqtt.common.model.ClientSession;
import com.vmqtt.common.protocol.MqttQos;
import com.vmqtt.common.service.MessageRouter;
import com.vmqtt.vmqttcore.service.MessageRoutingEngine;
import com.vmqtt.vmqttcore.service.router.LoadBalancer;
import com.vmqtt.vmqttcore.service.router.RoundRobinLoadBalancer;
import lombok.Data;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.time.Instant;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

/**
 * 集群消息路由引擎
 * 扩展标准MessageRoutingEngine，支持跨节点消息路由
 *
 * @author zhenglin
 * @date 2025/08/10
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class ClusterMessageRoutingEngine {

    private final MessageRoutingEngine localRoutingEngine;
    private final ClusterSubscriptionManager clusterSubscriptionManager;
    private final ServiceRegistry serviceRegistry;
    private final ClusterNodeManager clusterNodeManager;
    
    // 节点负载均衡器
    private final LoadBalancer<ServiceInfo> nodeLoadBalancer = new RoundRobinLoadBalancer<>();
    
    // 消息路由统计
    private final AtomicLong totalMessages = new AtomicLong(0);
    private final AtomicLong localMessages = new AtomicLong(0);
    private final AtomicLong clusterMessages = new AtomicLong(0);
    private final AtomicLong routingErrors = new AtomicLong(0);
    
    // 路由缓存和性能优化
    private final ConcurrentHashMap<String, NodeSubscriberDistribution> distributionCache = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, RoutingDecision> routingDecisionCache = new ConcurrentHashMap<>();
    
    /**
     * 集群消息路由主入口
     *
     * @param publisherClientId 发布者客户端ID
     * @param topic 主题
     * @param payload 消息内容
     * @param qos QoS级别
     * @param retain 是否为保留消息
     * @return 路由结果
     */
    public CompletableFuture<ClusterRouteResult> routeMessage(
            String publisherClientId, String topic, byte[] payload, MqttQos qos, boolean retain) {
        
        long startTime = System.nanoTime();
        totalMessages.incrementAndGet();
        
        return CompletableFuture.supplyAsync(() -> {
            try {
                log.debug("Routing cluster message: publisher={}, topic={}, qos={}, retain={}", 
                    publisherClientId, topic, qos, retain);
                
                // 1. 查询订阅者分布
                NodeSubscriberDistribution distribution = getSubscriberDistribution(topic);
                
                // 2. 制定路由决策
                RoutingDecision decision = makeRoutingDecision(topic, distribution, qos);
                
                // 3. 执行路由
                ClusterRouteResult result = executeRouting(
                    publisherClientId, topic, payload, qos, retain, decision, startTime);
                
                // 4. 更新统计信息
                updateRoutingStats(result);
                
                return result;
                
            } catch (Exception e) {
                log.error("Failed to route cluster message: publisher={}, topic={}", 
                    publisherClientId, topic, e);
                routingErrors.incrementAndGet();
                return ClusterRouteResult.error("Routing failed: " + e.getMessage());
            }
        });
    }
    
    /**
     * 批量路由消息（性能优化）
     *
     * @param messages 消息列表
     * @return 批量路由结果
     */
    public CompletableFuture<BatchClusterRouteResult> routeMessageBatch(List<ClusterMessageRequest> messages) {
        return CompletableFuture.supplyAsync(() -> {
            try {
                log.debug("Routing message batch: size={}", messages.size());
                
                List<CompletableFuture<ClusterRouteResult>> routingFutures = messages.stream()
                    .map(msg -> routeMessage(msg.getPublisherClientId(), msg.getTopic(), 
                        msg.getPayload(), msg.getQos(), msg.isRetain()))
                    .collect(Collectors.toList());
                
                CompletableFuture<Void> allFutures = CompletableFuture.allOf(
                    routingFutures.toArray(new CompletableFuture[0]));
                
                return allFutures.thenApply(v -> {
                    List<ClusterRouteResult> results = routingFutures.stream()
                        .map(CompletableFuture::join)
                        .collect(Collectors.toList());
                    
                    return new BatchClusterRouteResult(results);
                }).join();
                
            } catch (Exception e) {
                log.error("Failed to route message batch", e);
                return BatchClusterRouteResult.error("Batch routing failed: " + e.getMessage());
            }
        });
    }
    
    /**
     * 处理来自其他节点的路由消息请求
     *
     * @param request 路由消息请求
     * @return 路由响应
     */
    public RouteMessageResponse handleRemoteRouteMessage(RouteMessageRequest request) {
        try {
            String sourceNodeId = request.getSourceNodeId();
            String targetNodeId = request.getTargetNodeId();
            ClusterMessage clusterMessage = request.getMessage();
            
            log.debug("Handling remote route message: from={}, to={}, topic={}", 
                sourceNodeId, targetNodeId, clusterMessage.getTopic());
            
            // 检查目标节点是否为当前节点
            if (!clusterNodeManager.getCurrentNodeId().equals(targetNodeId)) {
                return RouteMessageResponse.newBuilder()
                    .setSuccess(false)
                    .setErrorMessage("Target node mismatch")
                    .build();
            }
            
            // 转换为本地消息格式并路由
            CompletableFuture<MessageRouter.RouteResult> localRouting = localRoutingEngine.route(
                clusterMessage.getClientId(),
                clusterMessage.getTopic(),
                clusterMessage.getPayload().toByteArray(),
                MqttQos.fromValue(clusterMessage.getQos()),
                clusterMessage.getRetain()
            );
            
            MessageRouter.RouteResult localResult = localRouting.join();
            
            // 构建路由结果
            RoutingResult routingResult = RoutingResult.newBuilder()
                .setMessageId(clusterMessage.getMessageId())
                .setDelivered(localResult.isAllSuccessful())
                .setSubscriberCount(localResult.getSuccessfulDeliveries())
                .setRoutingLatencyMs(localResult.getRoutingTimeMs())
                .build();
            
            return RouteMessageResponse.newBuilder()
                .setSuccess(true)
                .setRoutingResult(routingResult)
                .build();
                
        } catch (Exception e) {
            log.error("Failed to handle remote route message", e);
            return RouteMessageResponse.newBuilder()
                .setSuccess(false)
                .setErrorMessage("Remote routing failed: " + e.getMessage())
                .build();
        }
    }
    
    /**
     * 处理跨节点消息分发请求
     *
     * @param request 分发消息请求
     * @return 分发响应
     */
    public DistributeMessageResponse handleDistributeMessage(DistributeMessageRequest request) {
        try {
            String sourceNodeId = request.getSourceNodeId();
            ClusterMessage message = request.getMessage();
            List<String> targetNodes = request.getTargetNodesList();
            DistributionStrategy strategy = request.getStrategy();
            
            log.debug("Handling distribute message: from={}, targets={}, strategy={}", 
                sourceNodeId, targetNodes.size(), strategy);
            
            Map<String, Boolean> results = new HashMap<>();
            
            // 根据分发策略执行分发
            switch (strategy) {
                case DISTRIBUTION_BROADCAST:
                    results = distributeBroadcast(message, targetNodes);
                    break;
                case DISTRIBUTION_SELECTIVE:
                    results = distributeSelective(message, targetNodes);
                    break;
                case DISTRIBUTION_LOAD_BALANCED:
                    results = distributeLoadBalanced(message, targetNodes);
                    break;
                default:
                    throw new IllegalArgumentException("Unknown distribution strategy: " + strategy);
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
     * 查询订阅者分布
     */
    private NodeSubscriberDistribution getSubscriberDistribution(String topic) {
        // 尝试从缓存获取
        NodeSubscriberDistribution cached = distributionCache.get(topic);
        if (cached != null && !cached.isExpired()) {
            return cached;
        }
        
        // 查询集群订阅信息
        List<ClusterSubscriptionInfo> clusterSubscribers = 
            clusterSubscriptionManager.getSubscribersForTopic(topic);
        
        // 按节点分组
        Map<String, List<ClusterSubscriptionInfo>> nodeGroups = clusterSubscribers.stream()
            .collect(Collectors.groupingBy(ClusterSubscriptionInfo::getNodeId));
        
        // 构建分布信息
        List<NodeSubscriberInfo> nodeInfos = new ArrayList<>();
        for (Map.Entry<String, List<ClusterSubscriptionInfo>> entry : nodeGroups.entrySet()) {
            String nodeId = entry.getKey();
            List<ClusterSubscriptionInfo> subs = entry.getValue();
            
            NodeSubscriberInfo nodeInfo = NodeSubscriberInfo.newBuilder()
                .setNodeId(nodeId)
                .setSubscriberCount(subs.size())
                .addAllClientIds(subs.stream().map(ClusterSubscriptionInfo::getClientId).collect(Collectors.toList()))
                .setNodeLoad(getNodeLoad(nodeId))
                .build();
            nodeInfos.add(nodeInfo);
        }
        
        NodeSubscriberDistribution distribution = new NodeSubscriberDistribution(topic, nodeInfos);
        
        // 缓存结果
        distributionCache.put(topic, distribution);
        
        return distribution;
    }
    
    /**
     * 制定路由决策
     */
    private RoutingDecision makeRoutingDecision(String topic, NodeSubscriberDistribution distribution, MqttQos qos) {
        // 尝试从缓存获取
        String cacheKey = topic + ":" + qos.getValue();
        RoutingDecision cached = routingDecisionCache.get(cacheKey);
        if (cached != null && !cached.isExpired()) {
            return cached;
        }
        
        String currentNodeId = clusterNodeManager.getCurrentNodeId();
        List<String> localNodes = new ArrayList<>();
        List<String> remoteNodes = new ArrayList<>();
        
        for (NodeSubscriberInfo nodeInfo : distribution.getNodeInfos()) {
            if (currentNodeId.equals(nodeInfo.getNodeId())) {
                localNodes.add(nodeInfo.getNodeId());
            } else {
                remoteNodes.add(nodeInfo.getNodeId());
            }
        }
        
        RoutingDecision decision = new RoutingDecision(topic, qos, localNodes, remoteNodes);
        
        // 缓存决策
        routingDecisionCache.put(cacheKey, decision);
        
        return decision;
    }
    
    /**
     * 执行路由
     */
    private ClusterRouteResult executeRouting(String publisherClientId, String topic, byte[] payload, 
                                            MqttQos qos, boolean retain, RoutingDecision decision, long startTime) {
        
        List<CompletableFuture<RouteResult>> routingFutures = new ArrayList<>();
        
        // 处理本地路由
        if (!decision.getLocalNodes().isEmpty()) {
            CompletableFuture<RouteResult> localFuture = executeLocalRouting(
                publisherClientId, topic, payload, qos, retain)
                .thenApply(result -> new RouteResult(clusterNodeManager.getCurrentNodeId(), result, null));
            routingFutures.add(localFuture);
            localMessages.incrementAndGet();
        }
        
        // 处理远程路由
        for (String remoteNodeId : decision.getRemoteNodes()) {
            CompletableFuture<RouteResult> remoteFuture = executeRemoteRouting(
                publisherClientId, topic, payload, qos, retain, remoteNodeId)
                .thenApply(result -> new RouteResult(remoteNodeId, null, result));
            routingFutures.add(remoteFuture);
            clusterMessages.incrementAndGet();
        }
        
        // 等待所有路由完成
        CompletableFuture<Void> allFutures = CompletableFuture.allOf(
            routingFutures.toArray(new CompletableFuture[0]));
        
        return allFutures.thenApply(v -> {
            List<RouteResult> results = routingFutures.stream()
                .map(CompletableFuture::join)
                .collect(Collectors.toList());
            
            long endTime = System.nanoTime();
            double latencyMs = (endTime - startTime) / 1_000_000.0;
            
            return new ClusterRouteResult(topic, results, latencyMs, true, null);
        }).join();
    }
    
    /**
     * 执行本地路由
     */
    private CompletableFuture<MessageRouter.RouteResult> executeLocalRouting(
            String publisherClientId, String topic, byte[] payload, MqttQos qos, boolean retain) {
        
        return localRoutingEngine.route(publisherClientId, topic, payload, qos, retain);
    }
    
    /**
     * 执行远程路由
     */
    private CompletableFuture<RouteMessageResponse> executeRemoteRouting(
            String publisherClientId, String topic, byte[] payload, MqttQos qos, boolean retain, String targetNodeId) {
        
        return CompletableFuture.supplyAsync(() -> {
            try {
                // 构建集群消息
                ClusterMessage clusterMessage = ClusterMessage.newBuilder()
                    .setMessageId(generateMessageId())
                    .setClientId(publisherClientId)
                    .setTopic(topic)
                    .setPayload(com.google.protobuf.ByteString.copyFrom(payload))
                    .setQos(qos.getValue())
                    .setRetain(retain)
                    .setTimestamp(Timestamp.newBuilder().setSeconds(Instant.now().getEpochSecond()).build())
                    .build();
                
                // 构建路由元数据
                RoutingMetadata metadata = RoutingMetadata.newBuilder()
                    .setMessageId(clusterMessage.getMessageId())
                    .setSourceNode(clusterNodeManager.getCurrentNodeId())
                    .addTargetNodes(targetNodeId)
                    .setHopCount(0)
                    .setRoutingTimestamp(clusterMessage.getTimestamp())
                    .build();
                
                // 构建路由请求
                RouteMessageRequest request = RouteMessageRequest.newBuilder()
                    .setSourceNodeId(clusterNodeManager.getCurrentNodeId())
                    .setTargetNodeId(targetNodeId)
                    .setMessage(clusterMessage)
                    .setRoutingMetadata(metadata)
                    .build();
                
                // 这里应该通过gRPC调用目标节点
                // 暂时返回模拟结果，实际实现时需要调用具体的gRPC客户端
                log.debug("Executing remote routing to node {}", targetNodeId);
                
                return RouteMessageResponse.newBuilder()
                    .setSuccess(true)
                    .setRoutingResult(RoutingResult.newBuilder()
                        .setMessageId(clusterMessage.getMessageId())
                        .setDelivered(true)
                        .setSubscriberCount(1)
                        .setRoutingLatencyMs(10.0)
                        .build())
                    .build();
                
            } catch (Exception e) {
                log.error("Failed to execute remote routing to node {}", targetNodeId, e);
                return RouteMessageResponse.newBuilder()
                    .setSuccess(false)
                    .setErrorMessage("Remote routing failed: " + e.getMessage())
                    .build();
            }
        });
    }
    
    /**
     * 广播分发
     */
    private Map<String, Boolean> distributeBroadcast(ClusterMessage message, List<String> targetNodes) {
        Map<String, Boolean> results = new HashMap<>();
        
        for (String nodeId : targetNodes) {
            try {
                // 这里应该实际发送消息到目标节点
                log.debug("Broadcasting message to node {}", nodeId);
                results.put(nodeId, true);
            } catch (Exception e) {
                log.error("Failed to broadcast to node {}", nodeId, e);
                results.put(nodeId, false);
            }
        }
        
        return results;
    }
    
    /**
     * 选择性分发
     */
    private Map<String, Boolean> distributeSelective(ClusterMessage message, List<String> targetNodes) {
        // 选择负载最低的节点
        List<ServiceInfo> availableNodes = getAvailableNodes(targetNodes);
        ServiceInfo selectedNode = nodeLoadBalancer.select(availableNodes, message.getTopic());
        
        Map<String, Boolean> results = new HashMap<>();
        if (selectedNode != null) {
            try {
                log.debug("Selectively distributing message to node {}", selectedNode.getNodeId());
                results.put(selectedNode.getNodeId(), true);
            } catch (Exception e) {
                log.error("Failed to distribute to selected node {}", selectedNode.getNodeId(), e);
                results.put(selectedNode.getNodeId(), false);
            }
        }
        
        return results;
    }
    
    /**
     * 负载均衡分发
     */
    private Map<String, Boolean> distributeLoadBalanced(ClusterMessage message, List<String> targetNodes) {
        // 简化实现：选择负载最低的几个节点
        List<ServiceInfo> availableNodes = getAvailableNodes(targetNodes);
        availableNodes.sort(Comparator.comparingDouble(this::getNodeLoad));
        
        int targetCount = Math.min(2, availableNodes.size()); // 选择负载最低的2个节点
        List<ServiceInfo> selectedNodes = availableNodes.subList(0, targetCount);
        
        Map<String, Boolean> results = new HashMap<>();
        for (ServiceInfo node : selectedNodes) {
            try {
                log.debug("Load-balanced distributing message to node {}", node.getNodeId());
                results.put(node.getNodeId(), true);
            } catch (Exception e) {
                log.error("Failed to distribute to node {}", node.getNodeId(), e);
                results.put(node.getNodeId(), false);
            }
        }
        
        return results;
    }
    
    /**
     * 获取可用节点
     */
    private List<ServiceInfo> getAvailableNodes(List<String> nodeIds) {
        try {
            List<ServiceInfo> allNodes = serviceRegistry.discover("vmqtt-core");
            return allNodes.stream()
                .filter(node -> nodeIds.contains(node.getNodeId()))
                .filter(node -> node.getHealthStatus() == HealthStatus.HEALTHY)
                .collect(Collectors.toList());
        } catch (Exception e) {
            log.error("Failed to get available nodes", e);
            return Collections.emptyList();
        }
    }
    
    /**
     * 获取节点负载
     */
    private double getNodeLoad(String nodeId) {
        // 简化实现，返回随机负载值
        // 实际应该从监控系统获取真实负载
        return Math.random() * 100;
    }
    
    private double getNodeLoad(ServiceInfo node) {
        return getNodeLoad(node.getNodeId());
    }
    
    /**
     * 更新路由统计信息
     */
    private void updateRoutingStats(ClusterRouteResult result) {
        // 更新统计信息
        log.debug("Route result: topic={}, success={}, latency={}ms, results={}", 
            result.getTopic(), result.isSuccess(), result.getLatencyMs(), result.getResults().size());
    }
    
    /**
     * 生成消息ID
     */
    private String generateMessageId() {
        return "msg_" + System.currentTimeMillis() + "_" + UUID.randomUUID().toString().substring(0, 8);
    }
    
    /**
     * 获取集群路由统计信息
     */
    public ClusterRoutingStats getStats() {
        return new ClusterRoutingStats(
            totalMessages.get(),
            localMessages.get(), 
            clusterMessages.get(),
            routingErrors.get(),
            System.currentTimeMillis()
        );
    }
    
    // ========== 数据类定义 ==========
    
    @Data
    public static class ClusterMessageRequest {
        private final String publisherClientId;
        private final String topic;
        private final byte[] payload;
        private final MqttQos qos;
        private final boolean retain;
    }
    
    @Data
    public static class NodeSubscriberDistribution {
        private final String topic;
        private final List<NodeSubscriberInfo> nodeInfos;
        private final long timestamp;
        private final long ttlMs;
        
        public NodeSubscriberDistribution(String topic, List<NodeSubscriberInfo> nodeInfos) {
            this.topic = topic;
            this.nodeInfos = nodeInfos;
            this.timestamp = System.currentTimeMillis();
            this.ttlMs = 30000; // 30秒缓存
        }
        
        public boolean isExpired() {
            return System.currentTimeMillis() - timestamp > ttlMs;
        }
    }
    
    @Data
    public static class RoutingDecision {
        private final String topic;
        private final MqttQos qos;
        private final List<String> localNodes;
        private final List<String> remoteNodes;
        private final long timestamp;
        private final long ttlMs;
        
        public RoutingDecision(String topic, MqttQos qos, List<String> localNodes, List<String> remoteNodes) {
            this.topic = topic;
            this.qos = qos;
            this.localNodes = localNodes;
            this.remoteNodes = remoteNodes;
            this.timestamp = System.currentTimeMillis();
            this.ttlMs = 60000; // 60秒缓存
        }
        
        public boolean isExpired() {
            return System.currentTimeMillis() - timestamp > ttlMs;
        }
    }
    
    @Data
    public static class RouteResult {
        private final String nodeId;
        private final MessageRouter.RouteResult localResult;
        private final RouteMessageResponse remoteResult;
    }
    
    @Data
    public static class ClusterRouteResult {
        private final String topic;
        private final List<RouteResult> results;
        private final double latencyMs;
        private final boolean success;
        private final String errorMessage;
        
        public static ClusterRouteResult error(String errorMessage) {
            return new ClusterRouteResult(null, Collections.emptyList(), 0, false, errorMessage);
        }
    }
    
    @Data
    public static class BatchClusterRouteResult {
        private final List<ClusterRouteResult> results;
        private final boolean success;
        private final String errorMessage;
        
        public BatchClusterRouteResult(List<ClusterRouteResult> results) {
            this.results = results;
            this.success = true;
            this.errorMessage = null;
        }
        
        public static BatchClusterRouteResult error(String errorMessage) {
            return new BatchClusterRouteResult(Collections.emptyList(), false, errorMessage);
        }
        
        private BatchClusterRouteResult(List<ClusterRouteResult> results, boolean success, String errorMessage) {
            this.results = results;
            this.success = success;
            this.errorMessage = errorMessage;
        }
    }
    
    @Data
    public static class ClusterRoutingStats {
        private final long totalMessages;
        private final long localMessages;
        private final long clusterMessages;
        private final long errors;
        private final long timestamp;
    }
}