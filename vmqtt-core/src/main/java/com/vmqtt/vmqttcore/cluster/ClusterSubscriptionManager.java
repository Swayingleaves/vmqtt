package com.vmqtt.vmqttcore.cluster;

import com.google.protobuf.Timestamp;
import com.vmqtt.common.grpc.cluster.*;
import com.vmqtt.common.model.ClientSession;
import com.vmqtt.common.model.TopicSubscription;
import com.vmqtt.common.service.SessionManager;
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
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

/**
 * 集群订阅管理器
 * 负责管理集群内所有节点的订阅信息同步
 *
 * @author zhenglin
 * @date 2025/08/10
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class ClusterSubscriptionManager {

    private final SessionManager sessionManager;
    private final ServiceRegistry serviceRegistry;
    private final ClusterNodeManager clusterNodeManager;
    
    // 全局订阅信息缓存: subscriptionId -> ClusterSubscriptionInfo
    private final ConcurrentHashMap<String, ClusterSubscriptionInfo> globalSubscriptions = new ConcurrentHashMap<>();
    
    // 本地订阅版本控制
    private final AtomicLong localVersion = new AtomicLong(0);
    
    // Gossip 协议相关
    private final ScheduledExecutorService gossipExecutor = Executors.newScheduledThreadPool(2);
    private final Random random = new Random();
    
    // 订阅变更事件序列号
    private final AtomicLong sequenceNumber = new AtomicLong(0);
    
    /**
     * 启动集群订阅管理器
     */
    public void start() {
        log.info("Starting cluster subscription manager");
        
        // 启动定期的订阅信息同步
        gossipExecutor.scheduleWithFixedDelay(this::performGossipSync, 10, 30, TimeUnit.SECONDS);
        
        // 启动订阅清理任务
        gossipExecutor.scheduleWithFixedDelay(this::cleanupExpiredSubscriptions, 60, 60, TimeUnit.SECONDS);
    }
    
    /**
     * 停止集群订阅管理器
     */
    public void stop() {
        log.info("Stopping cluster subscription manager");
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
     * 添加本地订阅到集群
     *
     * @param clientId 客户端ID
     * @param subscription 订阅信息
     * @return CompletableFuture<Boolean>
     */
    public CompletableFuture<Boolean> addSubscription(String clientId, TopicSubscription subscription) {
        return CompletableFuture.supplyAsync(() -> {
            try {
                String subscriptionId = generateSubscriptionId(clientId, subscription.getTopicFilter());
                String nodeId = clusterNodeManager.getCurrentNodeId();
                
                ClusterSubscriptionInfo clusterSub = ClusterSubscriptionInfo.newBuilder()
                    .setSubscriptionId(subscriptionId)
                    .setClientId(clientId)
                    .setNodeId(nodeId)
                    .setTopicFilter(subscription.getTopicFilter())
                    .setQos(subscription.getQos().getValue())
                    .setSharedSubscription(subscription.getTopicFilter().startsWith("$share/"))
                    .setShareGroup(extractShareGroup(subscription.getTopicFilter()))
                    .setCreatedAt(Timestamp.newBuilder().setSeconds(Instant.now().getEpochSecond()).build())
                    .setUpdatedAt(Timestamp.newBuilder().setSeconds(Instant.now().getEpochSecond()).build())
                    .setVersion(localVersion.incrementAndGet())
                    .setStatus(SubscriptionStatus.SUBSCRIPTION_ACTIVE)
                    .build();
                
                // 添加到本地缓存
                globalSubscriptions.put(subscriptionId, clusterSub);
                
                // 广播变更事件
                broadcastSubscriptionChange(clusterSub, ChangeEventType.SUBSCRIPTION_ADDED);
                
                log.debug("Added cluster subscription: clientId={}, topic={}, subscriptionId={}", 
                    clientId, subscription.getTopicFilter(), subscriptionId);
                return true;
                
            } catch (Exception e) {
                log.error("Failed to add cluster subscription for client {} and topic {}", 
                    clientId, subscription.getTopicFilter(), e);
                return false;
            }
        });
    }
    
    /**
     * 移除集群订阅
     *
     * @param clientId 客户端ID  
     * @param topicFilter 主题过滤器
     * @return CompletableFuture<Boolean>
     */
    public CompletableFuture<Boolean> removeSubscription(String clientId, String topicFilter) {
        return CompletableFuture.supplyAsync(() -> {
            try {
                String subscriptionId = generateSubscriptionId(clientId, topicFilter);
                ClusterSubscriptionInfo removed = globalSubscriptions.remove(subscriptionId);
                
                if (removed != null) {
                    // 广播变更事件
                    broadcastSubscriptionChange(removed, ChangeEventType.SUBSCRIPTION_REMOVED);
                    
                    log.debug("Removed cluster subscription: clientId={}, topic={}, subscriptionId={}", 
                        clientId, topicFilter, subscriptionId);
                    return true;
                } else {
                    log.debug("Subscription not found for removal: clientId={}, topic={}", clientId, topicFilter);
                    return false;
                }
            } catch (Exception e) {
                log.error("Failed to remove cluster subscription for client {} and topic {}", 
                    clientId, topicFilter, e);
                return false;
            }
        });
    }
    
    /**
     * 查询主题的所有集群订阅者
     *
     * @param topic 发布主题
     * @return 匹配的集群订阅信息列表
     */
    public List<ClusterSubscriptionInfo> getSubscribersForTopic(String topic) {
        return globalSubscriptions.values().stream()
            .filter(sub -> topicMatches(topic, sub.getTopicFilter()))
            .filter(sub -> sub.getStatus() == SubscriptionStatus.SUBSCRIPTION_ACTIVE)
            .collect(Collectors.toList());
    }
    
    /**
     * 查询指定节点的订阅信息
     *
     * @param nodeId 节点ID
     * @return 节点的订阅信息列表
     */
    public List<ClusterSubscriptionInfo> getSubscriptionsForNode(String nodeId) {
        return globalSubscriptions.values().stream()
            .filter(sub -> nodeId.equals(sub.getNodeId()))
            .filter(sub -> sub.getStatus() == SubscriptionStatus.SUBSCRIPTION_ACTIVE)
            .collect(Collectors.toList());
    }
    
    /**
     * 同步订阅信息 (处理来自其他节点的同步请求)
     *
     * @param request 同步请求
     * @return 同步响应
     */
    public SyncSubscriptionResponse syncSubscriptions(SyncSubscriptionRequest request) {
        try {
            List<ClusterSubscriptionInfo> updatedSubs = new ArrayList<>();
            
            for (ClusterSubscriptionInfo remoteSub : request.getSubscriptionsList()) {
                String subId = remoteSub.getSubscriptionId();
                ClusterSubscriptionInfo localSub = globalSubscriptions.get(subId);
                
                if (localSub == null || remoteSub.getVersion() > localSub.getVersion()) {
                    // 远程版本更新或本地不存在，接受远程版本
                    globalSubscriptions.put(subId, remoteSub);
                    updatedSubs.add(remoteSub);
                    log.debug("Updated cluster subscription from node {}: {}", request.getNodeId(), subId);
                } else if (localSub.getVersion() > remoteSub.getVersion()) {
                    // 本地版本更新，将本地版本加入响应
                    updatedSubs.add(localSub);
                }
            }
            
            return SyncSubscriptionResponse.newBuilder()
                .setSuccess(true)
                .addAllUpdatedSubscriptions(updatedSubs)
                .setCurrentVersion(localVersion.get())
                .build();
                
        } catch (Exception e) {
            log.error("Failed to sync subscriptions from node {}", request.getNodeId(), e);
            return SyncSubscriptionResponse.newBuilder()
                .setSuccess(false)
                .setErrorMessage("Sync failed: " + e.getMessage())
                .setCurrentVersion(localVersion.get())
                .build();
        }
    }
    
    /**
     * 处理Gossip订阅信息传播
     *
     * @param request Gossip请求
     * @return Gossip响应
     */
    public GossipSubscriptionInfoResponse handleGossip(GossipSubscriptionInfoRequest request) {
        try {
            List<GossipSubscriptionEntry> responseData = new ArrayList<>();
            
            // 处理接收到的Gossip数据
            for (GossipSubscriptionEntry entry : request.getGossipDataList()) {
                processGossipEntry(entry);
            }
            
            // 准备响应数据：随机选择一些本地订阅信息
            List<ClusterSubscriptionInfo> localSubs = globalSubscriptions.values().stream()
                .filter(sub -> clusterNodeManager.getCurrentNodeId().equals(sub.getNodeId()))
                .limit(10) // 限制传播数量
                .collect(Collectors.toList());
                
            for (ClusterSubscriptionInfo sub : localSubs) {
                GossipSubscriptionEntry entry = GossipSubscriptionEntry.newBuilder()
                    .setNodeId(sub.getNodeId())
                    .setSubscriptionData(sub.toString()) // 简化处理，实际可用更高效的序列化
                    .setVersion(sub.getVersion())
                    .setTimestamp(sub.getUpdatedAt())
                    .build();
                responseData.add(entry);
            }
            
            return GossipSubscriptionInfoResponse.newBuilder()
                .setSuccess(true)
                .addAllResponseData(responseData)
                .build();
                
        } catch (Exception e) {
            log.error("Failed to handle gossip from node {}", request.getNodeId(), e);
            return GossipSubscriptionInfoResponse.newBuilder()
                .setSuccess(false)
                .setErrorMessage("Gossip handling failed: " + e.getMessage())
                .build();
        }
    }
    
    /**
     * 广播订阅变更事件到其他节点
     */
    private void broadcastSubscriptionChange(ClusterSubscriptionInfo subscription, ChangeEventType eventType) {
        try {
            SubscriptionChangeEvent event = SubscriptionChangeEvent.newBuilder()
                .setSubscriptionId(subscription.getSubscriptionId())
                .setClientId(subscription.getClientId())
                .setNodeId(subscription.getNodeId())
                .setTopicFilter(subscription.getTopicFilter())
                .setEventType(eventType)
                .setTimestamp(Timestamp.newBuilder().setSeconds(Instant.now().getEpochSecond()).build())
                .setSequenceNumber(sequenceNumber.incrementAndGet())
                .build();
                
            // 异步广播到其他节点
            CompletableFuture.runAsync(() -> {
                List<ServiceInfo> otherNodes = getOtherClusterNodes();
                for (ServiceInfo node : otherNodes) {
                    try {
                        // 这里应该通过gRPC调用其他节点
                        // 暂时记录日志，实际实现时需要调用具体的gRPC客户端
                        log.debug("Broadcasting subscription change to node {}: {}", 
                            node.getNodeId(), event.getSubscriptionId());
                    } catch (Exception e) {
                        log.warn("Failed to broadcast subscription change to node {}", 
                            node.getNodeId(), e);
                    }
                }
            });
        } catch (Exception e) {
            log.error("Failed to broadcast subscription change", e);
        }
    }
    
    /**
     * 执行Gossip同步
     */
    private void performGossipSync() {
        try {
            List<ServiceInfo> otherNodes = getOtherClusterNodes();
            if (otherNodes.isEmpty()) {
                return;
            }
            
            // 随机选择几个节点进行Gossip
            Collections.shuffle(otherNodes);
            int gossipTargets = Math.min(3, otherNodes.size());
            
            for (int i = 0; i < gossipTargets; i++) {
                ServiceInfo targetNode = otherNodes.get(i);
                performGossipWithNode(targetNode);
            }
        } catch (Exception e) {
            log.error("Failed to perform gossip sync", e);
        }
    }
    
    /**
     * 与指定节点执行Gossip
     */
    private void performGossipWithNode(ServiceInfo targetNode) {
        try {
            // 准备Gossip数据
            List<GossipSubscriptionEntry> gossipData = prepareGossipData();
            
            GossipSubscriptionInfoRequest request = GossipSubscriptionInfoRequest.newBuilder()
                .setNodeId(clusterNodeManager.getCurrentNodeId())
                .addAllGossipData(gossipData)
                .setGossipRound((int) (System.currentTimeMillis() / 1000))
                .build();
                
            // 这里应该通过gRPC调用目标节点
            // 暂时记录日志，实际实现时需要调用具体的gRPC客户端
            log.debug("Performing gossip with node {}", targetNode.getNodeId());
            
        } catch (Exception e) {
            log.warn("Failed to perform gossip with node {}", targetNode.getNodeId(), e);
        }
    }
    
    /**
     * 准备Gossip数据
     */
    private List<GossipSubscriptionEntry> prepareGossipData() {
        return globalSubscriptions.values().stream()
            .filter(sub -> clusterNodeManager.getCurrentNodeId().equals(sub.getNodeId()))
            .limit(20) // 限制每次Gossip的数据量
            .map(sub -> GossipSubscriptionEntry.newBuilder()
                .setNodeId(sub.getNodeId())
                .setSubscriptionData(sub.toString())
                .setVersion(sub.getVersion())
                .setTimestamp(sub.getUpdatedAt())
                .build())
            .collect(Collectors.toList());
    }
    
    /**
     * 处理Gossip条目
     */
    private void processGossipEntry(GossipSubscriptionEntry entry) {
        // 简化实现，实际应该解析subscription_data并更新本地缓存
        log.debug("Processing gossip entry from node {}, version {}", 
            entry.getNodeId(), entry.getVersion());
    }
    
    /**
     * 清理过期的订阅信息
     */
    private void cleanupExpiredSubscriptions() {
        try {
            long now = Instant.now().getEpochSecond();
            long expireThreshold = now - 300; // 5分钟过期
            
            List<String> expiredSubs = globalSubscriptions.entrySet().stream()
                .filter(entry -> {
                    ClusterSubscriptionInfo sub = entry.getValue();
                    return sub.getUpdatedAt().getSeconds() < expireThreshold;
                })
                .map(Map.Entry::getKey)
                .collect(Collectors.toList());
                
            for (String subId : expiredSubs) {
                ClusterSubscriptionInfo removed = globalSubscriptions.remove(subId);
                if (removed != null) {
                    log.debug("Cleaned up expired subscription: {}", subId);
                }
            }
            
            if (!expiredSubs.isEmpty()) {
                log.info("Cleaned up {} expired subscriptions", expiredSubs.size());
            }
        } catch (Exception e) {
            log.error("Failed to cleanup expired subscriptions", e);
        }
    }
    
    /**
     * 获取其他集群节点
     */
    private List<ServiceInfo> getOtherClusterNodes() {
        try {
            List<ServiceInfo> allNodes = serviceRegistry.discover("vmqtt-core");
            String currentNodeId = clusterNodeManager.getCurrentNodeId();
            
            return allNodes.stream()
                .filter(node -> !currentNodeId.equals(node.getNodeId()))
                .collect(Collectors.toList());
        } catch (Exception e) {
            log.error("Failed to get other cluster nodes", e);
            return Collections.emptyList();
        }
    }
    
    /**
     * 生成订阅ID
     */
    private String generateSubscriptionId(String clientId, String topicFilter) {
        return clientId + ":" + topicFilter.hashCode();
    }
    
    /**
     * 提取共享组名
     */
    private String extractShareGroup(String topicFilter) {
        if (topicFilter != null && topicFilter.startsWith("$share/")) {
            String[] parts = topicFilter.split("/", 3);
            return parts.length >= 2 ? parts[1] : "";
        }
        return "";
    }
    
    /**
     * 主题匹配检查
     */
    private boolean topicMatches(String topic, String filter) {
        // 简化实现，实际应该使用完整的MQTT主题匹配算法
        if (filter.equals("#")) return true;
        if (filter.equals(topic)) return true;
        if (filter.endsWith("/#")) {
            String prefix = filter.substring(0, filter.length() - 2);
            return topic.startsWith(prefix + "/");
        }
        return false;
    }
}