package com.vmqtt.vmqttcore.cluster;

import com.google.protobuf.Timestamp;
import com.vmqtt.common.grpc.cluster.*;
import lombok.Data;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.time.Instant;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Gossip协议实现订阅信息传播
 * 基于SWIM(Scalable Weakly-consistent Infection-style Process Group Membership)协议
 *
 * @author zhenglin
 * @date 2025/08/10
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class GossipSubscriptionProtocol {

    private final ServiceRegistry serviceRegistry;
    private final ClusterNodeManager clusterNodeManager;
    
    // Gossip配置参数
    private static final int GOSSIP_INTERVAL_SECONDS = 10;
    private static final int GOSSIP_FANOUT = 3; // 每轮传播的目标节点数
    private static final int MAX_GOSSIP_ROUNDS = 5; // 最大传播轮次
    private static final int GOSSIP_MESSAGE_TTL = 300; // 消息生存时间（秒）
    
    // Gossip状态管理
    private final ConcurrentHashMap<String, GossipMessage> gossipMessages = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, NodeGossipState> nodeStates = new ConcurrentHashMap<>();
    private final AtomicLong gossipRound = new AtomicLong(0);
    private final Random random = new Random();
    
    // 执行器
    private final ScheduledExecutorService gossipExecutor = Executors.newScheduledThreadPool(2);
    
    /**
     * 启动Gossip协议
     */
    public void start() {
        log.info("Starting Gossip subscription protocol");
        
        // 定期执行Gossip传播
        gossipExecutor.scheduleWithFixedDelay(
            this::performGossipRound, 
            GOSSIP_INTERVAL_SECONDS, 
            GOSSIP_INTERVAL_SECONDS, 
            TimeUnit.SECONDS
        );
        
        // 定期清理过期消息
        gossipExecutor.scheduleWithFixedDelay(
            this::cleanupExpiredMessages,
            60,
            60,
            TimeUnit.SECONDS
        );
    }
    
    /**
     * 停止Gossip协议
     */
    public void stop() {
        log.info("Stopping Gossip subscription protocol");
        gossipExecutor.shutdown();
        try {
            if (!gossipExecutor.awaitTermination(10, TimeUnit.SECONDS)) {
                gossipExecutor.shutdownNow();
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            gossipExecutor.shutdownNow();
        }
    }
    
    /**
     * 传播订阅变更消息
     *
     * @param subscriptionChange 订阅变更事件
     * @return CompletableFuture<Boolean>
     */
    public CompletableFuture<Boolean> gossipSubscriptionChange(SubscriptionChangeEvent subscriptionChange) {
        return CompletableFuture.supplyAsync(() -> {
            try {
                String messageId = generateMessageId(subscriptionChange);
                
                GossipMessage gossipMessage = new GossipMessage(
                    messageId,
                    clusterNodeManager.getCurrentNodeId(),
                    subscriptionChange,
                    0, // 初始轮次
                    Instant.now(),
                    new HashSet<>()
                );
                
                gossipMessages.put(messageId, gossipMessage);
                
                log.debug("Started gossiping subscription change: messageId={}, subscriptionId={}", 
                    messageId, subscriptionChange.getSubscriptionId());
                    
                return true;
            } catch (Exception e) {
                log.error("Failed to start gossip for subscription change", e);
                return false;
            }
        });
    }
    
    /**
     * 处理来自其他节点的Gossip消息
     *
     * @param request Gossip请求
     * @return Gossip响应
     */
    public GossipSubscriptionInfoResponse handleGossipMessage(GossipSubscriptionInfoRequest request) {
        try {
            String sourceNodeId = request.getNodeId();
            int gossipRoundNumber = request.getGossipRound();
            
            log.debug("Received gossip from node {} in round {}", sourceNodeId, gossipRoundNumber);
            
            List<GossipSubscriptionEntry> responseEntries = new ArrayList<>();
            
            // 处理接收到的Gossip条目
            for (GossipSubscriptionEntry entry : request.getGossipDataList()) {
                processGossipEntry(entry, sourceNodeId);
            }
            
            // 更新节点状态
            updateNodeGossipState(sourceNodeId, gossipRoundNumber);
            
            // 准备响应数据：返回需要传播的消息
            responseEntries.addAll(prepareGossipResponse(sourceNodeId));
            
            return GossipSubscriptionInfoResponse.newBuilder()
                .setSuccess(true)
                .addAllResponseData(responseEntries)
                .build();
                
        } catch (Exception e) {
            log.error("Failed to handle gossip message from node {}", request.getNodeId(), e);
            return GossipSubscriptionInfoResponse.newBuilder()
                .setSuccess(false)
                .setErrorMessage("Failed to handle gossip: " + e.getMessage())
                .build();
        }
    }
    
    /**
     * 执行一轮Gossip传播
     */
    private void performGossipRound() {
        try {
            List<ServiceInfo> availableNodes = getAvailableNodes();
            if (availableNodes.isEmpty()) {
                return;
            }
            
            long currentRound = gossipRound.incrementAndGet();
            log.debug("Performing gossip round {}", currentRound);
            
            // 选择传播目标节点
            List<ServiceInfo> targetNodes = selectGossipTargets(availableNodes);
            
            // 向每个目标节点发送Gossip消息
            for (ServiceInfo targetNode : targetNodes) {
                gossipToNode(targetNode, currentRound);
            }
            
        } catch (Exception e) {
            log.error("Failed to perform gossip round", e);
        }
    }
    
    /**
     * 向指定节点发送Gossip消息
     */
    private void gossipToNode(ServiceInfo targetNode, long currentRound) {
        CompletableFuture.runAsync(() -> {
            try {
                String targetNodeId = targetNode.getNodeId();
                
                // 准备要传播的消息
                List<GossipSubscriptionEntry> gossipEntries = prepareGossipEntriesForNode(targetNodeId);
                
                if (gossipEntries.isEmpty()) {
                    return;
                }
                
                GossipSubscriptionInfoRequest request = GossipSubscriptionInfoRequest.newBuilder()
                    .setNodeId(clusterNodeManager.getCurrentNodeId())
                    .addAllGossipData(gossipEntries)
                    .setGossipRound((int) currentRound)
                    .build();
                
                // 这里应该通过gRPC调用目标节点
                // 暂时记录日志，实际实现时需要调用具体的gRPC客户端
                log.debug("Sending gossip to node {}: {} entries", targetNodeId, gossipEntries.size());
                
                // 模拟处理响应
                simulateGossipResponse(targetNodeId);
                
            } catch (Exception e) {
                log.warn("Failed to gossip to node {}", targetNode.getNodeId(), e);
            }
        });
    }
    
    /**
     * 选择Gossip传播目标节点
     */
    private List<ServiceInfo> selectGossipTargets(List<ServiceInfo> availableNodes) {
        List<ServiceInfo> targets = new ArrayList<>(availableNodes);
        Collections.shuffle(targets, random);
        
        int targetCount = Math.min(GOSSIP_FANOUT, targets.size());
        return targets.subList(0, targetCount);
    }
    
    /**
     * 为特定节点准备Gossip条目
     */
    private List<GossipSubscriptionEntry> prepareGossipEntriesForNode(String targetNodeId) {
        List<GossipSubscriptionEntry> entries = new ArrayList<>();
        
        for (GossipMessage message : gossipMessages.values()) {
            // 检查是否应该向该节点传播此消息
            if (shouldGossipToNode(message, targetNodeId)) {
                GossipSubscriptionEntry entry = createGossipEntry(message);
                entries.add(entry);
                
                // 标记已向此节点传播
                message.getPropagatedNodes().add(targetNodeId);
                message.setCurrentRound(message.getCurrentRound() + 1);
            }
        }
        
        return entries;
    }
    
    /**
     * 检查是否应该向指定节点传播消息
     */
    private boolean shouldGossipToNode(GossipMessage message, String targetNodeId) {
        // 不向消息源节点传播
        if (message.getSourceNodeId().equals(targetNodeId)) {
            return false;
        }
        
        // 检查是否已经传播到该节点
        if (message.getPropagatedNodes().contains(targetNodeId)) {
            return false;
        }
        
        // 检查传播轮次限制
        if (message.getCurrentRound() >= MAX_GOSSIP_ROUNDS) {
            return false;
        }
        
        // 检查消息是否过期
        long messageAge = Instant.now().getEpochSecond() - message.getCreatedAt().getEpochSecond();
        if (messageAge > GOSSIP_MESSAGE_TTL) {
            return false;
        }
        
        return true;
    }
    
    /**
     * 创建Gossip条目
     */
    private GossipSubscriptionEntry createGossipEntry(GossipMessage message) {
        return GossipSubscriptionEntry.newBuilder()
            .setNodeId(message.getSourceNodeId())
            .setSubscriptionData(serializeSubscriptionChange(message.getSubscriptionChange()))
            .setVersion(message.getSubscriptionChange().getSequenceNumber())
            .setTimestamp(Timestamp.newBuilder().setSeconds(message.getCreatedAt().getEpochSecond()).build())
            .build();
    }
    
    /**
     * 处理接收到的Gossip条目
     */
    private void processGossipEntry(GossipSubscriptionEntry entry, String sourceNodeId) {
        try {
            // 解析订阅变更事件
            SubscriptionChangeEvent changeEvent = deserializeSubscriptionChange(entry.getSubscriptionData());
            
            if (changeEvent != null) {
                String messageId = generateMessageId(changeEvent);
                
                // 检查是否已经处理过此消息
                if (!gossipMessages.containsKey(messageId)) {
                    // 创建新的Gossip消息用于继续传播
                    GossipMessage gossipMessage = new GossipMessage(
                        messageId,
                        entry.getNodeId(), // 原始消息源
                        changeEvent,
                        1, // 已经传播了一轮
                        Instant.ofEpochSecond(entry.getTimestamp().getSeconds()),
                        new HashSet<>(Arrays.asList(sourceNodeId)) // 已传播到当前节点的源节点
                    );
                    
                    gossipMessages.put(messageId, gossipMessage);
                    
                    // 处理订阅变更事件
                    handleSubscriptionChangeEvent(changeEvent);
                    
                    log.debug("Processed new gossip entry: messageId={}, from={}", 
                        messageId, sourceNodeId);
                }
            }
        } catch (Exception e) {
            log.error("Failed to process gossip entry from node {}", sourceNodeId, e);
        }
    }
    
    /**
     * 处理订阅变更事件
     */
    private void handleSubscriptionChangeEvent(SubscriptionChangeEvent changeEvent) {
        // 这里应该通知ClusterSubscriptionManager处理订阅变更
        log.debug("Handling subscription change event: type={}, subscriptionId={}, clientId={}", 
            changeEvent.getEventType(), changeEvent.getSubscriptionId(), changeEvent.getClientId());
    }
    
    /**
     * 准备Gossip响应数据
     */
    private List<GossipSubscriptionEntry> prepareGossipResponse(String targetNodeId) {
        // 可以在响应中包含一些额外的传播消息
        return new ArrayList<>();
    }
    
    /**
     * 更新节点Gossip状态
     */
    private void updateNodeGossipState(String nodeId, int gossipRound) {
        NodeGossipState state = nodeStates.computeIfAbsent(nodeId, k -> new NodeGossipState(nodeId));
        state.setLastGossipRound(gossipRound);
        state.setLastContactTime(Instant.now());
        state.incrementMessageCount();
    }
    
    /**
     * 清理过期消息
     */
    private void cleanupExpiredMessages() {
        try {
            long currentTime = Instant.now().getEpochSecond();
            
            List<String> expiredMessages = gossipMessages.entrySet().stream()
                .filter(entry -> {
                    GossipMessage msg = entry.getValue();
                    long messageAge = currentTime - msg.getCreatedAt().getEpochSecond();
                    return messageAge > GOSSIP_MESSAGE_TTL || msg.getCurrentRound() >= MAX_GOSSIP_ROUNDS;
                })
                .map(Map.Entry::getKey)
                .toList();
                
            for (String messageId : expiredMessages) {
                gossipMessages.remove(messageId);
            }
            
            if (!expiredMessages.isEmpty()) {
                log.debug("Cleaned up {} expired gossip messages", expiredMessages.size());
            }
            
        } catch (Exception e) {
            log.error("Failed to cleanup expired gossip messages", e);
        }
    }
    
    /**
     * 获取可用节点列表
     */
    private List<ServiceInfo> getAvailableNodes() {
        try {
            List<ServiceInfo> allNodes = serviceRegistry.discover("vmqtt-core");
            String currentNodeId = clusterNodeManager.getCurrentNodeId();
            
            return allNodes.stream()
                .filter(node -> !currentNodeId.equals(node.getNodeId()))
                .filter(node -> node.getHealthStatus() == HealthStatus.HEALTHY)
                .toList();
        } catch (Exception e) {
            log.error("Failed to get available nodes for gossip", e);
            return Collections.emptyList();
        }
    }
    
    /**
     * 生成消息ID
     */
    private String generateMessageId(SubscriptionChangeEvent changeEvent) {
        return changeEvent.getNodeId() + ":" + changeEvent.getSequenceNumber();
    }
    
    /**
     * 序列化订阅变更事件
     */
    private String serializeSubscriptionChange(SubscriptionChangeEvent changeEvent) {
        // 简化实现，实际应该使用更高效的序列化方式
        return changeEvent.toString();
    }
    
    /**
     * 反序列化订阅变更事件
     */
    private SubscriptionChangeEvent deserializeSubscriptionChange(String data) {
        // 简化实现，实际应该使用相应的反序列化方式
        // 这里返回null，实际实现时应该正确解析
        return null;
    }
    
    /**
     * 模拟Gossip响应处理
     */
    private void simulateGossipResponse(String targetNodeId) {
        // 实际实现时这里应该处理gRPC响应
        log.debug("Simulated gossip response from node {}", targetNodeId);
    }
    
    /**
     * Gossip消息数据结构
     */
    @Data
    private static class GossipMessage {
        private final String messageId;
        private final String sourceNodeId;
        private final SubscriptionChangeEvent subscriptionChange;
        private int currentRound;
        private final Instant createdAt;
        private final Set<String> propagatedNodes;
        
        public GossipMessage(String messageId, String sourceNodeId, 
                           SubscriptionChangeEvent subscriptionChange, 
                           int currentRound, Instant createdAt, 
                           Set<String> propagatedNodes) {
            this.messageId = messageId;
            this.sourceNodeId = sourceNodeId;
            this.subscriptionChange = subscriptionChange;
            this.currentRound = currentRound;
            this.createdAt = createdAt;
            this.propagatedNodes = propagatedNodes;
        }
    }
    
    /**
     * 节点Gossip状态
     */
    @Data
    private static class NodeGossipState {
        private final String nodeId;
        private int lastGossipRound;
        private Instant lastContactTime;
        private final AtomicInteger messageCount = new AtomicInteger(0);
        
        public NodeGossipState(String nodeId) {
            this.nodeId = nodeId;
            this.lastContactTime = Instant.now();
        }
        
        public void incrementMessageCount() {
            messageCount.incrementAndGet();
        }
        
        public int getMessageCount() {
            return messageCount.get();
        }
    }
}