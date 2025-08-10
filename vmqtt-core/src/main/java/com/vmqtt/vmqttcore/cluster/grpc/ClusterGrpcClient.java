package com.vmqtt.vmqttcore.cluster.grpc;

import com.vmqtt.common.grpc.cluster.*;
import com.vmqtt.vmqttcore.cluster.ServiceRegistry;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.stub.StreamObserver;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import javax.annotation.PreDestroy;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

/**
 * 集群gRPC客户端
 * 负责与其他集群节点的gRPC通信
 *
 * @author zhenglin
 * @date 2025/08/10
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class ClusterGrpcClient {

    private final ServiceRegistry serviceRegistry;
    
    // 节点连接缓存: nodeId -> ManagedChannel
    private final Map<String, ManagedChannel> channelCache = new ConcurrentHashMap<>();
    
    // gRPC存根缓存
    private final Map<String, ClusterSubscriptionServiceGrpc.ClusterSubscriptionServiceStub> subscriptionStubs = new ConcurrentHashMap<>();
    private final Map<String, ClusterMessageRoutingServiceGrpc.ClusterMessageRoutingServiceStub> routingStubs = new ConcurrentHashMap<>();
    
    /**
     * 同步订阅信息到指定节点
     *
     * @param targetNodeId 目标节点ID
     * @param request 同步请求
     * @return 同步响应
     */
    public CompletableFuture<SyncSubscriptionResponse> syncSubscriptionToNode(
            String targetNodeId, SyncSubscriptionRequest request) {
        
        return CompletableFuture.supplyAsync(() -> {
            try {
                ClusterSubscriptionServiceGrpc.ClusterSubscriptionServiceBlockingStub stub = 
                    getSubscriptionBlockingStub(targetNodeId);
                
                if (stub != null) {
                    log.debug("Syncing subscriptions to node: {}", targetNodeId);
                    SyncSubscriptionResponse response = stub.syncSubscription(request);
                    log.debug("Subscription sync completed to node: {}, success={}", 
                        targetNodeId, response.getSuccess());
                    return response;
                } else {
                    throw new RuntimeException("Failed to create gRPC stub for node: " + targetNodeId);
                }
            } catch (Exception e) {
                log.error("Failed to sync subscriptions to node: {}", targetNodeId, e);
                return SyncSubscriptionResponse.newBuilder()
                    .setSuccess(false)
                    .setErrorMessage("Sync failed: " + e.getMessage())
                    .setCurrentVersion(0)
                    .build();
            }
        });
    }
    
    /**
     * 广播订阅变更到指定节点
     *
     * @param targetNodeId 目标节点ID
     * @param request 广播请求
     * @return 广播响应
     */
    public CompletableFuture<BroadcastSubscriptionChangeResponse> broadcastSubscriptionChangeToNode(
            String targetNodeId, BroadcastSubscriptionChangeRequest request) {
        
        CompletableFuture<BroadcastSubscriptionChangeResponse> future = new CompletableFuture<>();
        
        try {
            ClusterSubscriptionServiceGrpc.ClusterSubscriptionServiceStub stub = 
                getSubscriptionAsyncStub(targetNodeId);
            
            if (stub != null) {
                log.debug("Broadcasting subscription change to node: {}", targetNodeId);
                
                stub.broadcastSubscriptionChange(request, new StreamObserver<BroadcastSubscriptionChangeResponse>() {
                    @Override
                    public void onNext(BroadcastSubscriptionChangeResponse response) {
                        log.debug("Subscription change broadcast completed to node: {}, success={}", 
                            targetNodeId, response.getSuccess());
                        future.complete(response);
                    }
                    
                    @Override
                    public void onError(Throwable t) {
                        log.error("Failed to broadcast subscription change to node: {}", targetNodeId, t);
                        BroadcastSubscriptionChangeResponse errorResponse = BroadcastSubscriptionChangeResponse.newBuilder()
                            .setSuccess(false)
                            .setErrorMessage("Broadcast failed: " + t.getMessage())
                            .build();
                        future.complete(errorResponse);
                    }
                    
                    @Override
                    public void onCompleted() {
                        // Response already handled in onNext
                    }
                });
            } else {
                throw new RuntimeException("Failed to create gRPC stub for node: " + targetNodeId);
            }
        } catch (Exception e) {
            log.error("Failed to broadcast subscription change to node: {}", targetNodeId, e);
            BroadcastSubscriptionChangeResponse errorResponse = BroadcastSubscriptionChangeResponse.newBuilder()
                .setSuccess(false)
                .setErrorMessage("Broadcast setup failed: " + e.getMessage())
                .build();
            future.complete(errorResponse);
        }
        
        return future;
    }
    
    /**
     * 向指定节点发送Gossip消息
     *
     * @param targetNodeId 目标节点ID
     * @param request Gossip请求
     * @return Gossip响应
     */
    public CompletableFuture<GossipSubscriptionInfoResponse> gossipToNode(
            String targetNodeId, GossipSubscriptionInfoRequest request) {
        
        return CompletableFuture.supplyAsync(() -> {
            try {
                ClusterSubscriptionServiceGrpc.ClusterSubscriptionServiceBlockingStub stub = 
                    getSubscriptionBlockingStub(targetNodeId);
                
                if (stub != null) {
                    log.debug("Sending gossip to node: {}", targetNodeId);
                    GossipSubscriptionInfoResponse response = stub.gossipSubscriptionInfo(request);
                    log.debug("Gossip completed to node: {}, success={}", 
                        targetNodeId, response.getSuccess());
                    return response;
                } else {
                    throw new RuntimeException("Failed to create gRPC stub for node: " + targetNodeId);
                }
            } catch (Exception e) {
                log.error("Failed to gossip to node: {}", targetNodeId, e);
                return GossipSubscriptionInfoResponse.newBuilder()
                    .setSuccess(false)
                    .setErrorMessage("Gossip failed: " + e.getMessage())
                    .build();
            }
        });
    }
    
    /**
     * 路由消息到指定节点
     *
     * @param targetNodeId 目标节点ID
     * @param request 路由请求
     * @return 路由响应
     */
    public CompletableFuture<RouteMessageResponse> routeMessageToNode(
            String targetNodeId, RouteMessageRequest request) {
        
        return CompletableFuture.supplyAsync(() -> {
            try {
                ClusterMessageRoutingServiceGrpc.ClusterMessageRoutingServiceBlockingStub stub = 
                    getRoutingBlockingStub(targetNodeId);
                
                if (stub != null) {
                    log.debug("Routing message to node: {}", targetNodeId);
                    RouteMessageResponse response = stub.routeMessage(request);
                    log.debug("Message routing completed to node: {}, success={}", 
                        targetNodeId, response.getSuccess());
                    return response;
                } else {
                    throw new RuntimeException("Failed to create gRPC stub for node: " + targetNodeId);
                }
            } catch (Exception e) {
                log.error("Failed to route message to node: {}", targetNodeId, e);
                return RouteMessageResponse.newBuilder()
                    .setSuccess(false)
                    .setErrorMessage("Routing failed: " + e.getMessage())
                    .build();
            }
        });
    }
    
    /**
     * 批量路由消息到指定节点
     *
     * @param targetNodeId 目标节点ID
     * @param request 批量路由请求
     * @return 批量路由响应
     */
    public CompletableFuture<BatchRouteMessageResponse> batchRouteMessageToNode(
            String targetNodeId, BatchRouteMessageRequest request) {
        
        return CompletableFuture.supplyAsync(() -> {
            try {
                ClusterMessageRoutingServiceGrpc.ClusterMessageRoutingServiceBlockingStub stub = 
                    getRoutingBlockingStub(targetNodeId);
                
                if (stub != null) {
                    log.debug("Batch routing messages to node: {}, count={}", 
                        targetNodeId, request.getMessagesCount());
                    BatchRouteMessageResponse response = stub.batchRouteMessage(request);
                    log.debug("Batch message routing completed to node: {}, success={}", 
                        targetNodeId, response.getSuccess());
                    return response;
                } else {
                    throw new RuntimeException("Failed to create gRPC stub for node: " + targetNodeId);
                }
            } catch (Exception e) {
                log.error("Failed to batch route messages to node: {}", targetNodeId, e);
                return BatchRouteMessageResponse.newBuilder()
                    .setSuccess(false)
                    .setErrorMessage("Batch routing failed: " + e.getMessage())
                    .build();
            }
        });
    }
    
    /**
     * 查询指定节点的订阅者分布
     *
     * @param targetNodeId 目标节点ID
     * @param request 查询请求
     * @return 查询响应
     */
    public CompletableFuture<QuerySubscriberDistributionResponse> querySubscriberDistributionFromNode(
            String targetNodeId, QuerySubscriberDistributionRequest request) {
        
        return CompletableFuture.supplyAsync(() -> {
            try {
                ClusterMessageRoutingServiceGrpc.ClusterMessageRoutingServiceBlockingStub stub = 
                    getRoutingBlockingStub(targetNodeId);
                
                if (stub != null) {
                    log.debug("Querying subscriber distribution from node: {}", targetNodeId);
                    QuerySubscriberDistributionResponse response = stub.querySubscriberDistribution(request);
                    log.debug("Subscriber distribution query completed from node: {}, success={}", 
                        targetNodeId, response.getSuccess());
                    return response;
                } else {
                    throw new RuntimeException("Failed to create gRPC stub for node: " + targetNodeId);
                }
            } catch (Exception e) {
                log.error("Failed to query subscriber distribution from node: {}", targetNodeId, e);
                return QuerySubscriberDistributionResponse.newBuilder()
                    .setSuccess(false)
                    .setErrorMessage("Query failed: " + e.getMessage())
                    .build();
            }
        });
    }
    
    /**
     * 分发消息到指定节点
     *
     * @param targetNodeId 目标节点ID
     * @param request 分发请求
     * @return 分发响应
     */
    public CompletableFuture<DistributeMessageResponse> distributeMessageToNode(
            String targetNodeId, DistributeMessageRequest request) {
        
        return CompletableFuture.supplyAsync(() -> {
            try {
                ClusterMessageRoutingServiceGrpc.ClusterMessageRoutingServiceBlockingStub stub = 
                    getRoutingBlockingStub(targetNodeId);
                
                if (stub != null) {
                    log.debug("Distributing message to node: {}", targetNodeId);
                    DistributeMessageResponse response = stub.distributeMessage(request);
                    log.debug("Message distribution completed to node: {}, success={}", 
                        targetNodeId, response.getSuccess());
                    return response;
                } else {
                    throw new RuntimeException("Failed to create gRPC stub for node: " + targetNodeId);
                }
            } catch (Exception e) {
                log.error("Failed to distribute message to node: {}", targetNodeId, e);
                return DistributeMessageResponse.newBuilder()
                    .setSuccess(false)
                    .setErrorMessage("Distribution failed: " + e.getMessage())
                    .build();
            }
        });
    }
    
    /**
     * 获取订阅服务的同步存根
     */
    private ClusterSubscriptionServiceGrpc.ClusterSubscriptionServiceBlockingStub getSubscriptionBlockingStub(String nodeId) {
        try {
            ManagedChannel channel = getOrCreateChannel(nodeId);
            return channel != null ? ClusterSubscriptionServiceGrpc.newBlockingStub(channel) : null;
        } catch (Exception e) {
            log.error("Failed to create subscription blocking stub for node: {}", nodeId, e);
            return null;
        }
    }
    
    /**
     * 获取订阅服务的异步存根
     */
    private ClusterSubscriptionServiceGrpc.ClusterSubscriptionServiceStub getSubscriptionAsyncStub(String nodeId) {
        try {
            ManagedChannel channel = getOrCreateChannel(nodeId);
            return channel != null ? ClusterSubscriptionServiceGrpc.newStub(channel) : null;
        } catch (Exception e) {
            log.error("Failed to create subscription async stub for node: {}", nodeId, e);
            return null;
        }
    }
    
    /**
     * 获取路由服务的同步存根
     */
    private ClusterMessageRoutingServiceGrpc.ClusterMessageRoutingServiceBlockingStub getRoutingBlockingStub(String nodeId) {
        try {
            ManagedChannel channel = getOrCreateChannel(nodeId);
            return channel != null ? ClusterMessageRoutingServiceGrpc.newBlockingStub(channel) : null;
        } catch (Exception e) {
            log.error("Failed to create routing blocking stub for node: {}", nodeId, e);
            return null;
        }
    }
    
    /**
     * 获取或创建gRPC通道
     */
    private ManagedChannel getOrCreateChannel(String nodeId) {
        return channelCache.computeIfAbsent(nodeId, this::createChannel);
    }
    
    /**
     * 创建gRPC通道
     */
    private ManagedChannel createChannel(String nodeId) {
        try {
            // 从服务注册表获取节点信息
            List<ServiceInfo> services = serviceRegistry.discover("vmqtt-core");
            ServiceInfo targetService = services.stream()
                .filter(service -> nodeId.equals(service.getNodeId()))
                .findFirst()
                .orElse(null);
            
            if (targetService == null) {
                log.warn("Node not found in service registry: {}", nodeId);
                return null;
            }
            
            String address = targetService.getAddress();
            int port = targetService.getPort();
            
            log.debug("Creating gRPC channel to node: {} at {}:{}", nodeId, address, port);
            
            ManagedChannel channel = ManagedChannelBuilder.forAddress(address, port)
                .usePlaintext() // 在生产环境中应该使用TLS
                .keepAliveTime(30, TimeUnit.SECONDS)
                .keepAliveTimeout(5, TimeUnit.SECONDS)
                .keepAliveWithoutCalls(true)
                .maxInboundMessageSize(4 * 1024 * 1024) // 4MB
                .build();
            
            log.info("gRPC channel created to node: {} at {}:{}", nodeId, address, port);
            return channel;
            
        } catch (Exception e) {
            log.error("Failed to create gRPC channel to node: {}", nodeId, e);
            return null;
        }
    }
    
    /**
     * 关闭指定节点的连接
     */
    public void closeConnection(String nodeId) {
        ManagedChannel channel = channelCache.remove(nodeId);
        if (channel != null && !channel.isShutdown()) {
            try {
                log.debug("Closing gRPC channel to node: {}", nodeId);
                channel.shutdown().awaitTermination(5, TimeUnit.SECONDS);
                log.info("gRPC channel closed to node: {}", nodeId);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                channel.shutdownNow();
                log.warn("Force closed gRPC channel to node: {}", nodeId);
            }
        }
        
        // 清理存根缓存
        subscriptionStubs.remove(nodeId);
        routingStubs.remove(nodeId);
    }
    
    /**
     * 关闭所有连接
     */
    @PreDestroy
    public void closeAllConnections() {
        log.info("Closing all gRPC connections");
        
        for (String nodeId : channelCache.keySet()) {
            closeConnection(nodeId);
        }
        
        channelCache.clear();
        subscriptionStubs.clear();
        routingStubs.clear();
        
        log.info("All gRPC connections closed");
    }
    
    /**
     * 获取连接状态信息
     */
    public Map<String, String> getConnectionStatus() {
        Map<String, String> status = new ConcurrentHashMap<>();
        
        for (Map.Entry<String, ManagedChannel> entry : channelCache.entrySet()) {
            String nodeId = entry.getKey();
            ManagedChannel channel = entry.getValue();
            
            String state = channel.getState(false).toString();
            status.put(nodeId, state);
        }
        
        return status;
    }
}