/**
 * Protobuf消息转换工具类
 *
 * @author zhenglin
 * @date 2025/08/09
 */
package com.vmqtt.backend.util;

import com.google.protobuf.Timestamp;
import com.vmqtt.common.grpc.storage.*;
import com.vmqtt.common.model.ClientSession;
import com.vmqtt.common.model.QueuedMessage;
import com.vmqtt.common.model.TopicSubscription;
import com.vmqtt.common.protocol.MqttQos;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.HashMap;
import java.util.Map;

/**
 * Protobuf消息与Java对象之间的转换工具类
 * 提供双向转换功能，支持各种存储相关的数据模型
 */
public class ProtoConverter {
    
    /**
     * 将proto StoredMessage转换为Java QueuedMessage
     */
    public static QueuedMessage toQueuedMessage(StoredMessage protoMessage) {
        QueuedMessage.QueuedMessageBuilder builder = QueuedMessage.builder()
                .messageId(protoMessage.getMessageId())
                .clientId(protoMessage.getClientId())
                .topic(protoMessage.getTopic())
                .payload(protoMessage.getPayload().toByteArray())
                .qos(toMqttQos(protoMessage.getQos()))
                .retain(protoMessage.getRetain());
                
        // 转换时间戳
        if (protoMessage.hasCreatedAt()) {
            builder.createdAt(toLocalDateTime(protoMessage.getCreatedAt()));
        }
        
        if (protoMessage.hasExpiresAt()) {
            builder.expiresAt(toLocalDateTime(protoMessage.getExpiresAt()));
        }
        
        // 转换属性
        Map<String, Object> properties = new HashMap<>();
        for (Map.Entry<String, String> entry : protoMessage.getPropertiesMap().entrySet()) {
            properties.put(entry.getKey(), entry.getValue());
        }
        builder.properties(properties);
        
        return builder.build();
    }
    
    /**
     * 将Java QueuedMessage转换为proto StoredMessage
     */
    public static StoredMessage toStoredMessage(QueuedMessage message) {
        StoredMessage.Builder builder = StoredMessage.newBuilder()
                .setStorageKey(message.getMessageId())
                .setMessageId(message.getMessageId())
                .setTopic(message.getTopic())
                .setPayload(com.google.protobuf.ByteString.copyFrom(message.getPayload()))
                .setQos(fromMqttQos(message.getQos()))
                .setRetain(message.isRetain())
                .setClientId(message.getClientId() != null ? message.getClientId() : "");
                
        // 转换时间戳
        if (message.getCreatedAt() != null) {
            builder.setCreatedAt(toTimestamp(message.getCreatedAt()));
        }
        
        if (message.getExpiresAt() != null) {
            builder.setExpiresAt(toTimestamp(message.getExpiresAt()));
        }
        
        // 转换属性
        if (message.getProperties() != null) {
            Map<String, String> protoProperties = new HashMap<>();
            for (Map.Entry<String, Object> entry : message.getProperties().entrySet()) {
                protoProperties.put(entry.getKey(), String.valueOf(entry.getValue()));
            }
            builder.putAllProperties(protoProperties);
        }
        
        return builder.build();
    }
    
    /**
     * 将proto SessionState转换为Java ClientSession
     */
    public static ClientSession toClientSession(SessionState protoState) {
        ClientSession session = new ClientSession();
        session.setSessionId(protoState.getSessionId());
        session.setClientId(protoState.getClientId());
        session.setCleanSession(protoState.getCleanSession());
        
        // 转换时间戳
        if (protoState.hasCreatedAt()) {
            session.setCreatedAt(toLocalDateTime(protoState.getCreatedAt()));
        }
        
        if (protoState.hasLastActivity()) {
            session.setLastActivity(toLocalDateTime(protoState.getLastActivity()));
        }
        
        // 转换属性
        Map<String, Object> attributes = new HashMap<>();
        for (Map.Entry<String, String> entry : protoState.getAttributesMap().entrySet()) {
            attributes.put(entry.getKey(), entry.getValue());
        }
        session.setAttributes(attributes);
        
        return session;
    }
    
    /**
     * 将Java ClientSession转换为proto SessionState
     */
    public static SessionState toSessionState(ClientSession session) {
        SessionState.Builder builder = SessionState.newBuilder()
                .setSessionId(session.getSessionId())
                .setClientId(session.getClientId())
                .setCleanSession(session.isCleanSession())
                .setKeepAlive(60); // 默认keepAlive值
                
        // 转换时间戳
        if (session.getCreatedAt() != null) {
            builder.setCreatedAt(toTimestamp(session.getCreatedAt()));
        }
        
        if (session.getLastActivity() != null) {
            builder.setLastActivity(toTimestamp(session.getLastActivity()));
        }
        
        // 转换连接状态 - 根据会话状态判断
        ConnectionState connectionState = (session.getState() == ClientSession.SessionState.ACTIVE) ? 
            ConnectionState.CONNECTED : ConnectionState.DISCONNECTED;
        builder.setConnectionState(connectionState);
        
        // 转换属性
        if (session.getAttributes() != null) {
            Map<String, String> protoAttributes = new HashMap<>();
            for (Map.Entry<String, Object> entry : session.getAttributes().entrySet()) {
                protoAttributes.put(entry.getKey(), String.valueOf(entry.getValue()));
            }
            builder.putAllAttributes(protoAttributes);
        }
        
        return builder.build();
    }
    
    /**
     * 将proto SubscriptionInfo转换为Java TopicSubscription
     */
    public static TopicSubscription toTopicSubscription(SubscriptionInfo protoSub) {
        TopicSubscription subscription = new TopicSubscription();
        subscription.setClientId(protoSub.getClientId());
        subscription.setTopicFilter(protoSub.getTopicFilter());
        subscription.setQos(toMqttQos(protoSub.getQos()));
        
        // 转换时间戳
        if (protoSub.hasCreatedAt()) {
            subscription.setSubscribedAt(toLocalDateTime(protoSub.getCreatedAt()));
        }
        
        // 转换订阅选项
        if (protoSub.hasOptions()) {
            SubscriptionOptions protoOptions = protoSub.getOptions();
            TopicSubscription.SubscriptionOptions options = subscription.getOptions();
            if (options == null) {
                options = new TopicSubscription.SubscriptionOptions();
                subscription.setOptions(options);
            }
            
            options.setNoLocal(protoOptions.getNoLocal());
            options.setRetainAsPublished(protoOptions.getRetainAsPublished());
            
            // 转换保留消息处理方式
            switch (protoOptions.getRetainHandling()) {
                case SEND_ON_SUBSCRIBE:
                    options.setRetainHandling(TopicSubscription.RetainHandling.SEND_ON_SUBSCRIBE);
                    break;
                case SEND_ON_SUBSCRIBE_NEW:
                    options.setRetainHandling(TopicSubscription.RetainHandling.SEND_ON_SUBSCRIBE_NEW);
                    break;
                case DO_NOT_SEND:
                    options.setRetainHandling(TopicSubscription.RetainHandling.DO_NOT_SEND);
                    break;
            }
        }
        
        return subscription;
    }
    
    /**
     * 将Java TopicSubscription转换为proto SubscriptionInfo
     */
    public static SubscriptionInfo toSubscriptionInfo(TopicSubscription subscription) {
        SubscriptionInfo.Builder builder = SubscriptionInfo.newBuilder()
                .setClientId(subscription.getClientId())
                .setTopicFilter(subscription.getTopicFilter())
                .setQos(fromMqttQos(subscription.getQos()));
                
        // 转换时间戳
        if (subscription.getSubscribedAt() != null) {
            builder.setCreatedAt(toTimestamp(subscription.getSubscribedAt()));
        }
        
        // 转换订阅选项
        SubscriptionOptions.Builder optionsBuilder = SubscriptionOptions.newBuilder();
        
        TopicSubscription.SubscriptionOptions options = subscription.getOptions();
        if (options != null) {
            optionsBuilder.setNoLocal(options.isNoLocal())
                          .setRetainAsPublished(options.isRetainAsPublished());
                          
            // 转换保留消息处理方式
            if (options.getRetainHandling() != null) {
                switch (options.getRetainHandling()) {
                    case SEND_ON_SUBSCRIBE:
                        optionsBuilder.setRetainHandling(RetainHandling.SEND_ON_SUBSCRIBE);
                        break;
                    case SEND_ON_SUBSCRIBE_NEW:
                        optionsBuilder.setRetainHandling(RetainHandling.SEND_ON_SUBSCRIBE_NEW);
                        break;
                    case DO_NOT_SEND:
                        optionsBuilder.setRetainHandling(RetainHandling.DO_NOT_SEND);
                        break;
                }
            }
        } else {
            // 默认选项
            optionsBuilder.setNoLocal(false)
                          .setRetainAsPublished(false)
                          .setRetainHandling(RetainHandling.SEND_ON_SUBSCRIBE);
        }
        
        builder.setOptions(optionsBuilder.build());
        
        return builder.build();
    }
    
    /**
     * 将proto QoS值转换为Java MqttQos枚举
     */
    public static MqttQos toMqttQos(int qosValue) {
        switch (qosValue) {
            case 0:
                return MqttQos.AT_MOST_ONCE;
            case 1:
                return MqttQos.AT_LEAST_ONCE;
            case 2:
                return MqttQos.EXACTLY_ONCE;
            default:
                throw new IllegalArgumentException("Invalid QoS value: " + qosValue);
        }
    }
    
    /**
     * 将Java MqttQos枚举转换为proto QoS值
     */
    public static int fromMqttQos(MqttQos qos) {
        switch (qos) {
            case AT_MOST_ONCE:
                return 0;
            case AT_LEAST_ONCE:
                return 1;
            case EXACTLY_ONCE:
                return 2;
            default:
                throw new IllegalArgumentException("Invalid QoS: " + qos);
        }
    }
    
    /**
     * 将proto Timestamp转换为Java LocalDateTime
     */
    public static LocalDateTime toLocalDateTime(Timestamp timestamp) {
        Instant instant = Instant.ofEpochSecond(timestamp.getSeconds(), timestamp.getNanos());
        return LocalDateTime.ofInstant(instant, ZoneId.systemDefault());
    }
    
    /**
     * 将Java LocalDateTime转换为proto Timestamp
     */
    public static Timestamp toTimestamp(LocalDateTime localDateTime) {
        Instant instant = localDateTime.atZone(ZoneId.systemDefault()).toInstant();
        return Timestamp.newBuilder()
                .setSeconds(instant.getEpochSecond())
                .setNanos(instant.getNano())
                .build();
    }
    
    /**
     * 将proto QueuedMessageState转换为Java MessageState
     */
    public static QueuedMessage.MessageState toMessageState(QueuedMessageState protoState) {
        switch (protoState) {
            case PENDING:
                return QueuedMessage.MessageState.QUEUED;
            case DELIVERED:
                return QueuedMessage.MessageState.SENT_WAITING_PUBACK;
            case ACKNOWLEDGED:
                return QueuedMessage.MessageState.ACKNOWLEDGED;
            case EXPIRED:
                return QueuedMessage.MessageState.EXPIRED;
            case FAILED:
                return QueuedMessage.MessageState.FAILED;
            default:
                return QueuedMessage.MessageState.QUEUED;
        }
    }
    
    /**
     * 将Java MessageState转换为proto QueuedMessageState
     */
    public static QueuedMessageState fromMessageState(QueuedMessage.MessageState state) {
        switch (state) {
            case QUEUED:
                return QueuedMessageState.PENDING;
            case SENDING:
            case SENT_WAITING_PUBACK:
            case SENT_WAITING_PUBREC:
            case RECEIVED_PUBREC_WAITING_PUBCOMP:
                return QueuedMessageState.DELIVERED;
            case ACKNOWLEDGED:
                return QueuedMessageState.ACKNOWLEDGED;
            case EXPIRED:
                return QueuedMessageState.EXPIRED;
            case FAILED:
                return QueuedMessageState.FAILED;
            default:
                return QueuedMessageState.PENDING;
        }
    }
}