package com.vmqtt.vmqttcore.cluster.grpc;

import com.vmqtt.common.grpc.cluster.*;
import com.vmqtt.vmqttcore.cluster.ClusterSubscriptionManager;
import com.vmqtt.vmqttcore.cluster.GossipSubscriptionProtocol;
import com.vmqtt.vmqttcore.cluster.SubscriptionConflictResolver;
import io.grpc.stub.StreamObserver;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import net.devh.boot.grpc.server.service.GrpcService;

/**
 * 集群订阅管理gRPC服务实现
 *
 * @author zhenglin
 * @date 2025/08/10
 */
@Slf4j
@GrpcService
@RequiredArgsConstructor
public class ClusterSubscriptionServiceImpl extends ClusterSubscriptionServiceGrpc.ClusterSubscriptionServiceImplBase {

    private final ClusterSubscriptionManager subscriptionManager;
    private final GossipSubscriptionProtocol gossipProtocol;
    private final SubscriptionConflictResolver conflictResolver;

    @Override
    public void syncSubscription(SyncSubscriptionRequest request, 
                               StreamObserver<SyncSubscriptionResponse> responseObserver) {
        try {
            log.debug("Received sync subscription request from node: {}", request.getNodeId());
            
            // 调用订阅管理器处理同步请求
            SyncSubscriptionResponse response = subscriptionManager.syncSubscriptions(request);
            
            responseObserver.onNext(response);
            responseObserver.onCompleted();
            
            log.debug("Sync subscription response sent to node: {}", request.getNodeId());
            
        } catch (Exception e) {
            log.error("Failed to sync subscriptions from node: {}", request.getNodeId(), e);
            
            SyncSubscriptionResponse errorResponse = SyncSubscriptionResponse.newBuilder()
                .setSuccess(false)
                .setErrorMessage("Sync failed: " + e.getMessage())
                .setCurrentVersion(0)
                .build();
            
            responseObserver.onNext(errorResponse);
            responseObserver.onCompleted();
        }
    }

    @Override
    public void broadcastSubscriptionChange(BroadcastSubscriptionChangeRequest request,
                                          StreamObserver<BroadcastSubscriptionChangeResponse> responseObserver) {
        try {
            log.debug("Received subscription change broadcast from node: {} for subscription: {}", 
                request.getNodeId(), request.getEvent().getSubscriptionId());
            
            // 通过Gossip协议传播订阅变更
            gossipProtocol.gossipSubscriptionChange(request.getEvent())
                .thenAccept(success -> {
                    BroadcastSubscriptionChangeResponse response = BroadcastSubscriptionChangeResponse.newBuilder()
                        .setSuccess(success)
                        .build();
                    responseObserver.onNext(response);
                    responseObserver.onCompleted();
                })
                .exceptionally(throwable -> {
                    log.error("Failed to broadcast subscription change", throwable);
                    BroadcastSubscriptionChangeResponse errorResponse = BroadcastSubscriptionChangeResponse.newBuilder()
                        .setSuccess(false)
                        .setErrorMessage("Broadcast failed: " + throwable.getMessage())
                        .build();
                    responseObserver.onNext(errorResponse);
                    responseObserver.onCompleted();
                    return null;
                });
            
        } catch (Exception e) {
            log.error("Failed to handle subscription change broadcast", e);
            
            BroadcastSubscriptionChangeResponse errorResponse = BroadcastSubscriptionChangeResponse.newBuilder()
                .setSuccess(false)
                .setErrorMessage("Broadcast handling failed: " + e.getMessage())
                .build();
            
            responseObserver.onNext(errorResponse);
            responseObserver.onCompleted();
        }
    }

    @Override
    public void querySubscriptions(QuerySubscriptionsRequest request,
                                 StreamObserver<QuerySubscriptionsResponse> responseObserver) {
        try {
            log.debug("Received subscription query from node: {} for topic: {}", 
                request.getNodeId(), request.getTopicFilter());
            
            // 查询匹配的订阅信息
            var subscriptions = subscriptionManager.getSubscribersForTopic(request.getTopicFilter());
            
            QuerySubscriptionsResponse response = QuerySubscriptionsResponse.newBuilder()
                .setSuccess(true)
                .addAllSubscriptions(subscriptions)
                .build();
            
            responseObserver.onNext(response);
            responseObserver.onCompleted();
            
            log.debug("Subscription query response sent: {} subscriptions found", subscriptions.size());
            
        } catch (Exception e) {
            log.error("Failed to query subscriptions", e);
            
            QuerySubscriptionsResponse errorResponse = QuerySubscriptionsResponse.newBuilder()
                .setSuccess(false)
                .setErrorMessage("Query failed: " + e.getMessage())
                .build();
            
            responseObserver.onNext(errorResponse);
            responseObserver.onCompleted();
        }
    }

    @Override
    public void gossipSubscriptionInfo(GossipSubscriptionInfoRequest request,
                                     StreamObserver<GossipSubscriptionInfoResponse> responseObserver) {
        try {
            log.debug("Received gossip subscription info from node: {} in round: {}", 
                request.getNodeId(), request.getGossipRound());
            
            // 处理Gossip消息
            GossipSubscriptionInfoResponse response = gossipProtocol.handleGossipMessage(request);
            
            responseObserver.onNext(response);
            responseObserver.onCompleted();
            
            log.debug("Gossip subscription response sent to node: {}", request.getNodeId());
            
        } catch (Exception e) {
            log.error("Failed to handle gossip subscription info", e);
            
            GossipSubscriptionInfoResponse errorResponse = GossipSubscriptionInfoResponse.newBuilder()
                .setSuccess(false)
                .setErrorMessage("Gossip handling failed: " + e.getMessage())
                .build();
            
            responseObserver.onNext(errorResponse);
            responseObserver.onCompleted();
        }
    }

    @Override
    public void resolveSubscriptionConflict(ResolveSubscriptionConflictRequest request,
                                          StreamObserver<ResolveSubscriptionConflictResponse> responseObserver) {
        try {
            log.debug("Received subscription conflict resolution request from node: {} with {} conflicts", 
                request.getNodeId(), request.getConflictsCount());
            
            // 解决订阅冲突
            ResolveSubscriptionConflictResponse response = conflictResolver.resolveConflicts(request);
            
            responseObserver.onNext(response);
            responseObserver.onCompleted();
            
            log.debug("Conflict resolution response sent: {} subscriptions resolved", 
                response.getResolvedSubscriptionsCount());
            
        } catch (Exception e) {
            log.error("Failed to resolve subscription conflicts", e);
            
            ResolveSubscriptionConflictResponse errorResponse = ResolveSubscriptionConflictResponse.newBuilder()
                .setSuccess(false)
                .setErrorMessage("Conflict resolution failed: " + e.getMessage())
                .build();
            
            responseObserver.onNext(errorResponse);
            responseObserver.onCompleted();
        }
    }
}