package com.vmqtt.vmqttcore.cluster.grpc;

import com.vmqtt.common.grpc.cluster.*;
import com.vmqtt.vmqttcore.cluster.ClusterMessageDistributor;
import com.vmqtt.vmqttcore.cluster.ClusterMessageRoutingEngine;
import com.vmqtt.vmqttcore.cluster.ClusterSubscriptionManager;
import io.grpc.stub.StreamObserver;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import net.devh.boot.grpc.server.service.GrpcService;

import java.util.List;
import java.util.stream.Collectors;

/**
 * 集群消息路由gRPC服务实现
 *
 * @author zhenglin
 * @date 2025/08/10
 */
@Slf4j
@GrpcService
@RequiredArgsConstructor
public class ClusterMessageRoutingServiceImpl extends ClusterMessageRoutingServiceGrpc.ClusterMessageRoutingServiceImplBase {

    private final ClusterMessageRoutingEngine routingEngine;
    private final ClusterMessageDistributor messageDistributor;
    private final ClusterSubscriptionManager subscriptionManager;

    @Override
    public void routeMessage(RouteMessageRequest request,
                           StreamObserver<RouteMessageResponse> responseObserver) {
        try {
            log.debug("Received route message request from node: {} to node: {}", 
                request.getSourceNodeId(), request.getTargetNodeId());
            
            // 处理路由消息请求
            RouteMessageResponse response = routingEngine.handleRemoteRouteMessage(request);
            
            responseObserver.onNext(response);
            responseObserver.onCompleted();
            
            log.debug("Route message response sent: success={}", response.getSuccess());
            
        } catch (Exception e) {
            log.error("Failed to route message from node: {}", request.getSourceNodeId(), e);
            
            RouteMessageResponse errorResponse = RouteMessageResponse.newBuilder()
                .setSuccess(false)
                .setErrorMessage("Route message failed: " + e.getMessage())
                .build();
            
            responseObserver.onNext(errorResponse);
            responseObserver.onCompleted();
        }
    }

    @Override
    public void batchRouteMessage(BatchRouteMessageRequest request,
                                StreamObserver<BatchRouteMessageResponse> responseObserver) {
        try {
            log.debug("Received batch route message request from node: {} with {} messages", 
                request.getSourceNodeId(), request.getMessagesCount());
            
            // 处理每个消息的路由请求
            List<RouteMessageResponse> responses = request.getMessagesList().stream()
                .map(routingEngine::handleRemoteRouteMessage)
                .collect(Collectors.toList());
            
            // 检查是否所有消息都路由成功
            boolean allSuccess = responses.stream().allMatch(RouteMessageResponse::getSuccess);
            String errorMessage = allSuccess ? "" : "Some messages failed to route";
            
            BatchRouteMessageResponse response = BatchRouteMessageResponse.newBuilder()
                .setSuccess(allSuccess)
                .setErrorMessage(errorMessage)
                .addAllResults(responses)
                .build();
            
            responseObserver.onNext(response);
            responseObserver.onCompleted();
            
            log.debug("Batch route message response sent: {} messages processed, success={}", 
                responses.size(), allSuccess);
            
        } catch (Exception e) {
            log.error("Failed to process batch route message from node: {}", request.getSourceNodeId(), e);
            
            BatchRouteMessageResponse errorResponse = BatchRouteMessageResponse.newBuilder()
                .setSuccess(false)
                .setErrorMessage("Batch route failed: " + e.getMessage())
                .build();
            
            responseObserver.onNext(errorResponse);
            responseObserver.onCompleted();
        }
    }

    @Override
    public void querySubscriberDistribution(QuerySubscriberDistributionRequest request,
                                          StreamObserver<QuerySubscriberDistributionResponse> responseObserver) {
        try {
            log.debug("Received subscriber distribution query for topic: {}", request.getTopic());
            
            // 查询订阅者分布
            List<ClusterSubscriptionInfo> subscribers = subscriptionManager.getSubscribersForTopic(request.getTopic());
            
            // 按节点分组订阅者
            var nodeGroups = subscribers.stream()
                .collect(Collectors.groupingBy(ClusterSubscriptionInfo::getNodeId));
            
            // 构建节点订阅者信息
            List<NodeSubscriberInfo> nodeSubscriberInfos = nodeGroups.entrySet().stream()
                .map(entry -> {
                    String nodeId = entry.getKey();
                    List<ClusterSubscriptionInfo> nodeSubs = entry.getValue();
                    
                    return NodeSubscriberInfo.newBuilder()
                        .setNodeId(nodeId)
                        .setSubscriberCount(nodeSubs.size())
                        .addAllClientIds(nodeSubs.stream()
                            .map(ClusterSubscriptionInfo::getClientId)
                            .collect(Collectors.toList()))
                        .setNodeLoad(getNodeLoad(nodeId)) // 简化实现
                        .build();
                })
                .collect(Collectors.toList());
            
            QuerySubscriberDistributionResponse response = QuerySubscriberDistributionResponse.newBuilder()
                .setSuccess(true)
                .addAllNodeSubscribers(nodeSubscriberInfos)
                .build();
            
            responseObserver.onNext(response);
            responseObserver.onCompleted();
            
            log.debug("Subscriber distribution response sent: {} nodes with subscribers", 
                nodeSubscriberInfos.size());
            
        } catch (Exception e) {
            log.error("Failed to query subscriber distribution for topic: {}", request.getTopic(), e);
            
            QuerySubscriberDistributionResponse errorResponse = QuerySubscriberDistributionResponse.newBuilder()
                .setSuccess(false)
                .setErrorMessage("Distribution query failed: " + e.getMessage())
                .build();
            
            responseObserver.onNext(errorResponse);
            responseObserver.onCompleted();
        }
    }

    @Override
    public void distributeMessage(DistributeMessageRequest request,
                                StreamObserver<DistributeMessageResponse> responseObserver) {
        try {
            log.debug("Received distribute message request from node: {} to {} target nodes", 
                request.getSourceNodeId(), request.getTargetNodesCount());
            
            // 处理消息分发请求
            DistributeMessageResponse response = messageDistributor.handleDistributeMessage(request);
            
            responseObserver.onNext(response);
            responseObserver.onCompleted();
            
            log.debug("Distribute message response sent: success={}", response.getSuccess());
            
        } catch (Exception e) {
            log.error("Failed to distribute message from node: {}", request.getSourceNodeId(), e);
            
            DistributeMessageResponse errorResponse = DistributeMessageResponse.newBuilder()
                .setSuccess(false)
                .setErrorMessage("Message distribution failed: " + e.getMessage())
                .build();
            
            responseObserver.onNext(errorResponse);
            responseObserver.onCompleted();
        }
    }
    
    /**
     * 获取节点负载（简化实现）
     */
    private double getNodeLoad(String nodeId) {
        // 实际应该从监控系统获取真实负载
        return Math.random() * 100;
    }
}