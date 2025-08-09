/**
 * 存储服务gRPC实现
 *
 * @author zhenglin
 * @date 2025/08/09
 */
package com.vmqtt.backend.grpc;

import com.vmqtt.backend.service.*;
import com.vmqtt.backend.util.ProtoConverter;
import com.vmqtt.common.grpc.storage.*;
import com.vmqtt.common.model.QueuedMessage;
import com.vmqtt.common.model.TopicSubscription;
import com.vmqtt.common.model.ClientSession;
import io.grpc.stub.StreamObserver;
import lombok.extern.slf4j.Slf4j;
import net.devh.boot.grpc.server.service.GrpcService;
import org.springframework.beans.factory.annotation.Autowired;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/**
 * 存储服务gRPC实现
 * 提供统一的存储接口，整合各种持久化服务
 */
@Slf4j
@GrpcService
public class StorageServiceImpl extends StorageServiceGrpc.StorageServiceImplBase {
    
    @Autowired
    private MessagePersistenceService messagePersistenceService;
    
    @Autowired
    private SessionPersistenceService sessionPersistenceService;
    
    @Autowired
    private SubscriptionPersistenceService subscriptionPersistenceService;
    
    @Autowired
    private RetainedMessageService retainedMessageService;
    
    @Autowired
    private WillMessageService willMessageService;
    
    @Autowired
    private QoSMessageManager qosMessageManager;
    
    @Override
    public void storeMessage(StoreMessageRequest request, StreamObserver<StoreMessageResponse> responseObserver) {
        log.debug("Received store message request for messageId: {}", 
                 request.getMessage().getMessageId());
        
        try {
            // 转换proto消息到Java对象
            QueuedMessage message = ProtoConverter.toQueuedMessage(request.getMessage());
            
            // 存储消息
            boolean success = messagePersistenceService.storeMessage(message);
            
            StoreMessageResponse.Builder responseBuilder = StoreMessageResponse.newBuilder()
                    .setSuccess(success);
                    
            if (success) {
                responseBuilder.setStorageKey(message.getMessageId());
                log.debug("Message stored successfully: messageId={}", message.getMessageId());
            } else {
                responseBuilder.setErrorMessage("Failed to store message");
                log.error("Failed to store message: messageId={}", message.getMessageId());
            }
            
            responseObserver.onNext(responseBuilder.build());
            responseObserver.onCompleted();
            
        } catch (Exception e) {
            log.error("Error storing message", e);
            StoreMessageResponse response = StoreMessageResponse.newBuilder()
                    .setSuccess(false)
                    .setErrorMessage("Internal error: " + e.getMessage())
                    .build();
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        }
    }

    @Override
    public void batchStoreMessage(BatchStoreMessageRequest request, 
                                StreamObserver<BatchStoreMessageResponse> responseObserver) {
        log.debug("Received batch store message request with {} messages", 
                 request.getMessagesCount());
        
        try {
            List<QueuedMessage> messages = new ArrayList<>();
            
            // 转换所有proto消息
            for (StoredMessage storedMessage : request.getMessagesList()) {
                messages.add(ProtoConverter.toQueuedMessage(storedMessage));
            }
            
            // 批量存储消息
            int successfulCount = messagePersistenceService.batchStoreMessages(messages);
            int failedCount = messages.size() - successfulCount;
            
            BatchStoreMessageResponse.Builder responseBuilder = BatchStoreMessageResponse.newBuilder()
                    .setSuccess(successfulCount > 0)
                    .setSuccessfulCount(successfulCount)
                    .setFailedCount(failedCount);
            
            // 添加成功存储的消息键
            for (int i = 0; i < successfulCount; i++) {
                responseBuilder.addStorageKeys(messages.get(i).getMessageId());
            }
            
            if (failedCount > 0) {
                responseBuilder.setErrorMessage(
                    String.format("Failed to store %d out of %d messages", failedCount, messages.size()));
            }
            
            log.info("Batch store completed: successful={}, failed={}", successfulCount, failedCount);
            
            responseObserver.onNext(responseBuilder.build());
            responseObserver.onCompleted();
            
        } catch (Exception e) {
            log.error("Error in batch store message", e);
            BatchStoreMessageResponse response = BatchStoreMessageResponse.newBuilder()
                    .setSuccess(false)
                    .setErrorMessage("Internal error: " + e.getMessage())
                    .setFailedCount(request.getMessagesCount())
                    .build();
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        }
    }

    @Override
    public void getMessage(GetMessageRequest request, StreamObserver<GetMessageResponse> responseObserver) {
        log.debug("Received get message request: messageId={}", request.getMessageId());
        
        try {
            String messageId = !request.getMessageId().isEmpty() ? 
                             request.getMessageId() : request.getStorageKey();
            
            Optional<QueuedMessage> messageOpt = messagePersistenceService.getMessage(messageId);
            
            GetMessageResponse.Builder responseBuilder = GetMessageResponse.newBuilder();
            
            if (messageOpt.isPresent()) {
                QueuedMessage message = messageOpt.get();
                StoredMessage storedMessage = ProtoConverter.toStoredMessage(message);
                
                responseBuilder.setSuccess(true)
                             .setMessage(storedMessage);
                             
                log.debug("Message retrieved successfully: messageId={}", messageId);
            } else {
                responseBuilder.setSuccess(false)
                             .setErrorMessage("Message not found");
                             
                log.debug("Message not found: messageId={}", messageId);
            }
            
            responseObserver.onNext(responseBuilder.build());
            responseObserver.onCompleted();
            
        } catch (Exception e) {
            log.error("Error getting message", e);
            GetMessageResponse response = GetMessageResponse.newBuilder()
                    .setSuccess(false)
                    .setErrorMessage("Internal error: " + e.getMessage())
                    .build();
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        }
    }

    @Override
    public void deleteMessage(DeleteMessageRequest request, StreamObserver<DeleteMessageResponse> responseObserver) {
        log.debug("Received delete message request: messageId={}", request.getMessageId());
        
        try {
            String messageId = !request.getMessageId().isEmpty() ? 
                             request.getMessageId() : request.getStorageKey();
            
            boolean success = messagePersistenceService.deleteMessage(messageId);
            
            DeleteMessageResponse.Builder responseBuilder = DeleteMessageResponse.newBuilder()
                    .setSuccess(success);
                    
            if (!success) {
                responseBuilder.setErrorMessage("Failed to delete message");
            }
            
            log.debug("Message deletion result: messageId={}, success={}", messageId, success);
            
            responseObserver.onNext(responseBuilder.build());
            responseObserver.onCompleted();
            
        } catch (Exception e) {
            log.error("Error deleting message", e);
            DeleteMessageResponse response = DeleteMessageResponse.newBuilder()
                    .setSuccess(false)
                    .setErrorMessage("Internal error: " + e.getMessage())
                    .build();
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        }
    }

    @Override
    public void queryMessages(QueryMessagesRequest request, StreamObserver<QueryMessagesResponse> responseObserver) {
        log.debug("Received query messages request: clientId={}, topicFilter={}", 
                 request.getQuery().getClientId(), request.getQuery().getTopicFilter());
        
        try {
            QueryMessagesResponse.Builder responseBuilder = QueryMessagesResponse.newBuilder();
            
            MessageQuery query = request.getQuery();
            int limit = request.getLimit() > 0 ? request.getLimit() : 100;
            
            List<QueuedMessage> messages;
            
            if (!query.getClientId().isEmpty()) {
                // 查询特定客户端的消息
                messages = messagePersistenceService.getQueuedMessages(query.getClientId(), limit);
            } else {
                // 这里可以扩展更复杂的查询逻辑
                messages = new ArrayList<>();
                log.warn("Complex message queries not yet implemented");
            }
            
            // 转换为proto消息
            for (QueuedMessage message : messages) {
                StoredMessage storedMessage = ProtoConverter.toStoredMessage(message);
                responseBuilder.addMessages(storedMessage);
            }
            
            responseBuilder.setSuccess(true)
                          .setTotalCount(messages.size());
                          
            log.debug("Query messages completed: found {} messages", messages.size());
            
            responseObserver.onNext(responseBuilder.build());
            responseObserver.onCompleted();
            
        } catch (Exception e) {
            log.error("Error querying messages", e);
            QueryMessagesResponse response = QueryMessagesResponse.newBuilder()
                    .setSuccess(false)
                    .setErrorMessage("Internal error: " + e.getMessage())
                    .build();
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        }
    }

    @Override
    public void storeSessionState(StoreSessionStateRequest request, 
                                StreamObserver<StoreSessionStateResponse> responseObserver) {
        log.debug("Received store session state request: sessionId={}", 
                 request.getSessionState().getSessionId());
        
        try {
            // 转换proto会话状态到Java对象
            ClientSession session = ProtoConverter.toClientSession(request.getSessionState());
            
            // 存储会话状态
            boolean success = sessionPersistenceService.saveSession(session);
            
            StoreSessionStateResponse.Builder responseBuilder = StoreSessionStateResponse.newBuilder()
                    .setSuccess(success);
                    
            if (!success) {
                responseBuilder.setErrorMessage("Failed to store session state");
            }
            
            log.debug("Session state storage result: sessionId={}, success={}", 
                     session.getSessionId(), success);
            
            responseObserver.onNext(responseBuilder.build());
            responseObserver.onCompleted();
            
        } catch (Exception e) {
            log.error("Error storing session state", e);
            StoreSessionStateResponse response = StoreSessionStateResponse.newBuilder()
                    .setSuccess(false)
                    .setErrorMessage("Internal error: " + e.getMessage())
                    .build();
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        }
    }

    @Override
    public void getSessionState(GetSessionStateRequest request, 
                              StreamObserver<GetSessionStateResponse> responseObserver) {
        log.debug("Received get session state request: sessionId={}, clientId={}", 
                 request.getSessionId(), request.getClientId());
        
        try {
            String identifier = !request.getSessionId().isEmpty() ? 
                              request.getSessionId() : request.getClientId();
            
            Optional<ClientSession> sessionOpt = sessionPersistenceService.loadSession(identifier);
            
            GetSessionStateResponse.Builder responseBuilder = GetSessionStateResponse.newBuilder();
            
            if (sessionOpt.isPresent()) {
                ClientSession session = sessionOpt.get();
                SessionState sessionState = ProtoConverter.toSessionState(session);
                
                responseBuilder.setSuccess(true)
                             .setSessionState(sessionState);
                             
                log.debug("Session state retrieved successfully: sessionId={}", identifier);
            } else {
                responseBuilder.setSuccess(false)
                             .setErrorMessage("Session not found");
                             
                log.debug("Session not found: sessionId={}", identifier);
            }
            
            responseObserver.onNext(responseBuilder.build());
            responseObserver.onCompleted();
            
        } catch (Exception e) {
            log.error("Error getting session state", e);
            GetSessionStateResponse response = GetSessionStateResponse.newBuilder()
                    .setSuccess(false)
                    .setErrorMessage("Internal error: " + e.getMessage())
                    .build();
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        }
    }

    @Override
    public void deleteSessionState(DeleteSessionStateRequest request, 
                                 StreamObserver<DeleteSessionStateResponse> responseObserver) {
        log.debug("Received delete session state request: sessionId={}, clientId={}", 
                 request.getSessionId(), request.getClientId());
        
        try {
            String identifier = !request.getSessionId().isEmpty() ? 
                              request.getSessionId() : request.getClientId();
            
            boolean success = sessionPersistenceService.deleteSession(identifier);
            
            DeleteSessionStateResponse.Builder responseBuilder = DeleteSessionStateResponse.newBuilder()
                    .setSuccess(success);
                    
            if (!success) {
                responseBuilder.setErrorMessage("Failed to delete session state or session not found");
            }
            
            log.debug("Session state deletion result: sessionId={}, success={}", identifier, success);
            
            responseObserver.onNext(responseBuilder.build());
            responseObserver.onCompleted();
            
        } catch (Exception e) {
            log.error("Error deleting session state", e);
            DeleteSessionStateResponse response = DeleteSessionStateResponse.newBuilder()
                    .setSuccess(false)
                    .setErrorMessage("Internal error: " + e.getMessage())
                    .build();
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        }
    }

    @Override
    public void storeSubscription(StoreSubscriptionRequest request, 
                                StreamObserver<StoreSubscriptionResponse> responseObserver) {
        log.debug("Received store subscription request: clientId={}, topicFilter={}", 
                 request.getSubscription().getClientId(), request.getSubscription().getTopicFilter());
        
        try {
            // 转换proto订阅信息到Java对象
            TopicSubscription subscription = ProtoConverter.toTopicSubscription(request.getSubscription());
            
            // 存储订阅信息
            boolean success = subscriptionPersistenceService.addSubscription(subscription);
            
            StoreSubscriptionResponse.Builder responseBuilder = StoreSubscriptionResponse.newBuilder()
                    .setSuccess(success);
                    
            if (!success) {
                responseBuilder.setErrorMessage("Failed to store subscription");
            }
            
            log.debug("Subscription storage result: clientId={}, topicFilter={}, success={}", 
                     subscription.getClientId(), subscription.getTopicFilter(), success);
            
            responseObserver.onNext(responseBuilder.build());
            responseObserver.onCompleted();
            
        } catch (Exception e) {
            log.error("Error storing subscription", e);
            StoreSubscriptionResponse response = StoreSubscriptionResponse.newBuilder()
                    .setSuccess(false)
                    .setErrorMessage("Internal error: " + e.getMessage())
                    .build();
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        }
    }

    @Override
    public void getSubscriptions(GetSubscriptionsRequest request, 
                               StreamObserver<GetSubscriptionsResponse> responseObserver) {
        log.debug("Received get subscriptions request: clientId={}, topicFilter={}", 
                 request.getClientId(), request.getTopicFilter());
        
        try {
            GetSubscriptionsResponse.Builder responseBuilder = GetSubscriptionsResponse.newBuilder();
            
            List<TopicSubscription> subscriptions;
            
            if (!request.getClientId().isEmpty() && !request.getTopicFilter().isEmpty()) {
                // 获取特定客户端的特定主题订阅
                Optional<TopicSubscription> subscriptionOpt = 
                    subscriptionPersistenceService.getSubscription(request.getClientId(), request.getTopicFilter());
                subscriptions = subscriptionOpt.map(List::of).orElse(new ArrayList<>());
            } else if (!request.getClientId().isEmpty()) {
                // 获取客户端的所有订阅
                subscriptions = subscriptionPersistenceService.getClientSubscriptions(request.getClientId(), 1000);
            } else if (!request.getTopicFilter().isEmpty()) {
                // 获取主题的所有订阅者
                subscriptions = subscriptionPersistenceService.getTopicSubscribers(request.getTopicFilter(), 1000);
            } else {
                subscriptions = new ArrayList<>();
                log.warn("Invalid subscription query - both clientId and topicFilter are empty");
            }
            
            // 转换为proto订阅信息
            for (TopicSubscription subscription : subscriptions) {
                SubscriptionInfo subscriptionInfo = ProtoConverter.toSubscriptionInfo(subscription);
                responseBuilder.addSubscriptions(subscriptionInfo);
            }
            
            responseBuilder.setSuccess(true);
            
            log.debug("Get subscriptions completed: found {} subscriptions", subscriptions.size());
            
            responseObserver.onNext(responseBuilder.build());
            responseObserver.onCompleted();
            
        } catch (Exception e) {
            log.error("Error getting subscriptions", e);
            GetSubscriptionsResponse response = GetSubscriptionsResponse.newBuilder()
                    .setSuccess(false)
                    .setErrorMessage("Internal error: " + e.getMessage())
                    .build();
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        }
    }

    @Override
    public void deleteSubscription(DeleteSubscriptionRequest request, 
                                 StreamObserver<DeleteSubscriptionResponse> responseObserver) {
        log.debug("Received delete subscription request: clientId={}, topic={}", 
                 request.getClientId(), request.getTopic());
        
        try {
            // 删除订阅信息
            Optional<TopicSubscription> deletedSubscription = 
                subscriptionPersistenceService.removeSubscription(request.getClientId(), request.getTopic());
            
            boolean success = deletedSubscription.isPresent();
            
            DeleteSubscriptionResponse.Builder responseBuilder = DeleteSubscriptionResponse.newBuilder()
                    .setSuccess(success);
                    
            if (!success) {
                responseBuilder.setErrorMessage("Subscription not found or failed to delete");
            }
            
            log.debug("Subscription deletion result: clientId={}, topic={}, success={}", 
                     request.getClientId(), request.getTopic(), success);
            
            responseObserver.onNext(responseBuilder.build());
            responseObserver.onCompleted();
            
        } catch (Exception e) {
            log.error("Error deleting subscription", e);
            DeleteSubscriptionResponse response = DeleteSubscriptionResponse.newBuilder()
                    .setSuccess(false)
                    .setErrorMessage("Internal error: " + e.getMessage())
                    .build();
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        }
    }

    @Override
    public void createSnapshot(CreateSnapshotRequest request, 
                             StreamObserver<CreateSnapshotResponse> responseObserver) {
        log.info("Received create snapshot request: snapshotId={}, type={}", 
                request.getSnapshotId(), request.getSnapshotType());
        
        try {
            // 快照创建逻辑 - 这里暂时返回未实现的响应
            CreateSnapshotResponse response = CreateSnapshotResponse.newBuilder()
                    .setSuccess(false)
                    .setErrorMessage("Snapshot creation not yet implemented")
                    .build();
            
            responseObserver.onNext(response);
            responseObserver.onCompleted();
            
        } catch (Exception e) {
            log.error("Error creating snapshot", e);
            CreateSnapshotResponse response = CreateSnapshotResponse.newBuilder()
                    .setSuccess(false)
                    .setErrorMessage("Internal error: " + e.getMessage())
                    .build();
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        }
    }

    @Override
    public void restoreSnapshot(RestoreSnapshotRequest request, 
                              StreamObserver<RestoreSnapshotResponse> responseObserver) {
        log.info("Received restore snapshot request: snapshotId={}, snapshotPath={}", 
                request.getSnapshotId(), request.getSnapshotPath());
        
        try {
            // 快照恢复逻辑 - 这里暂时返回未实现的响应
            RestoreSnapshotResponse response = RestoreSnapshotResponse.newBuilder()
                    .setSuccess(false)
                    .setErrorMessage("Snapshot restoration not yet implemented")
                    .build();
            
            responseObserver.onNext(response);
            responseObserver.onCompleted();
            
        } catch (Exception e) {
            log.error("Error restoring snapshot", e);
            RestoreSnapshotResponse response = RestoreSnapshotResponse.newBuilder()
                    .setSuccess(false)
                    .setErrorMessage("Internal error: " + e.getMessage())
                    .build();
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        }
    }
}