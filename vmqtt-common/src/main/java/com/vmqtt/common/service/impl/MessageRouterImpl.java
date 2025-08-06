/**
 * 消息路由器实现类
 *
 * @author zhenglin
 * @date 2025/08/06
 */
package com.vmqtt.common.service.impl;

import com.vmqtt.common.model.ClientSession;
import com.vmqtt.common.model.QueuedMessage;
import com.vmqtt.common.protocol.MqttQos;
import com.vmqtt.common.service.MessageRouter;
import com.vmqtt.common.service.SessionManager;
import io.netty.buffer.ByteBuf;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;
import java.util.stream.Collectors;

/**
 * 消息路由器默认实现
 * 基于会话管理器的高性能消息路由和分发
 */
@Slf4j
@Service
public class MessageRouterImpl implements MessageRouter {

    /**
     * 会话管理器依赖
     */
    @Autowired
    private SessionManager sessionManager;

    /**
     * 路由事件监听器集合
     */
    private final ConcurrentLinkedQueue<RouteEventListener> listeners = new ConcurrentLinkedQueue<>();

    /**
     * 路由统计信息
     */
    private final RouteStatsImpl stats = new RouteStatsImpl();

    @Override
    public CompletableFuture<RouteResult> routeMessage(String publisherClientId, String topic, 
                                                      ByteBuf payload, MqttQos qos, boolean retain) {
        return CompletableFuture.supplyAsync(() -> {
            long startTime = System.currentTimeMillis();
            String messageId = generateMessageId();
            
            try {
                log.debug("开始路由消息: publisherClientId={}, topic={}, qos={}, retain={}, messageId={}", 
                    publisherClientId, topic, qos, retain, messageId);

                // 创建排队消息
                ByteBuf payloadCopy = payload.copy();
                byte[] payloadBytes = new byte[payloadCopy.readableBytes()];
                payloadCopy.readBytes(payloadBytes);
                payloadCopy.release(); // 释放复制的ByteBuf
                
                QueuedMessage message = QueuedMessage.builder()
                        .messageId(messageId)
                        .topic(topic)
                        .payload(payloadBytes)
                        .qos(qos)
                        .retain(retain)
                        .createdAt(LocalDateTime.now())
                        .build();
                        
                // 在消息属性中记录发布者客户端ID
                message.getProperties().put("publisherClientId", publisherClientId);

                // 获取匹配的订阅者
                List<ClientSession> subscribers = getMatchingSubscribers(topic);
                
                // 触发路由开始事件
                triggerRoutingStarted(topic, messageId, subscribers.size());

                // 执行消息路由
                RouteResult result = routeMessageInternal(message, subscribers, startTime);

                // 更新统计信息
                stats.totalRoutedMessages.increment();
                if (result.isAllSuccessful()) {
                    stats.successfulRoutes.increment();
                } else {
                    stats.failedRoutes.increment();
                }

                // 触发路由完成事件
                triggerRoutingCompleted(topic, messageId, result);

                log.debug("消息路由完成: topic={}, messageId={}, subscribers={}, successful={}, failed={}", 
                    topic, messageId, result.getMatchedSubscribers(), 
                    result.getSuccessfulDeliveries(), result.getFailedDeliveries());

                return result;

            } catch (Exception e) {
                log.error("消息路由失败: publisherClientId={}, topic={}, messageId={}", 
                    publisherClientId, topic, messageId, e);
                
                stats.failedRoutes.increment();
                long routingTime = System.currentTimeMillis() - startTime;
                return new RouteResultImpl(0, 0, 1, 0, 0, routingTime, false);
                
            } finally {
                // 释放ByteBuf资源
                if (payload.refCnt() > 0) {
                    payload.release();
                }
            }
        });
    }

    @Override
    public CompletableFuture<RouteResult> routeMessage(QueuedMessage message) {
        return CompletableFuture.supplyAsync(() -> {
            long startTime = System.currentTimeMillis();
            
            try {
                log.debug("路由排队消息: messageId={}, topic={}, qos={}", 
                    message.getMessageId(), message.getTopic(), message.getQos());

                // 获取匹配的订阅者
                List<ClientSession> subscribers = getMatchingSubscribers(message.getTopic());
                
                // 触发路由开始事件
                triggerRoutingStarted(message.getTopic(), message.getMessageId(), subscribers.size());

                // 执行消息路由
                RouteResult result = routeMessageInternal(message, subscribers, startTime);

                // 更新统计信息
                stats.totalRoutedMessages.increment();
                if (result.isAllSuccessful()) {
                    stats.successfulRoutes.increment();
                } else {
                    stats.failedRoutes.increment();
                }

                // 触发路由完成事件
                triggerRoutingCompleted(message.getTopic(), message.getMessageId(), result);

                return result;

            } catch (Exception e) {
                log.error("路由排队消息失败: messageId={}, topic={}", 
                    message.getMessageId(), message.getTopic(), e);
                
                stats.failedRoutes.increment();
                long routingTime = System.currentTimeMillis() - startTime;
                return new RouteResultImpl(0, 0, 1, 0, 0, routingTime, false);
            }
        });
    }

    @Override
    public CompletableFuture<Boolean> sendMessageToClient(String clientId, QueuedMessage message) {
        return CompletableFuture.supplyAsync(() -> {
            try {
                log.debug("发送消息到客户端: clientId={}, messageId={}", clientId, message.getMessageId());

                // 检查客户端会话是否存在
                Optional<ClientSession> sessionOpt = sessionManager.getSession(clientId);
                if (!sessionOpt.isPresent()) {
                    log.warn("客户端会话不存在: clientId={}, messageId={}", clientId, message.getMessageId());
                    triggerMessageDeliveryFailed(clientId, message, "Session not found");
                    return false;
                }

                ClientSession session = sessionOpt.get();
                
                // 检查会话状态
                if (session.getState() != ClientSession.SessionState.ACTIVE) {
                    // 会话不活跃，将消息添加到队列
                    log.debug("会话不活跃，消息加入队列: clientId={}, messageId={}", clientId, message.getMessageId());
                    sessionManager.addQueuedMessage(clientId, message).join();
                    triggerMessageQueued(clientId, message);
                    return true;
                }

                // TODO: 这里需要实现实际的消息发送逻辑
                // 现在先模拟发送成功
                boolean sent = simulateMessageSending(session, message);
                
                if (sent) {
                    // 更新会话统计
                    session.incrementMessagesReceived();
                    sessionManager.updateSessionActivity(clientId);
                    
                    triggerMessageDelivered(clientId, message);
                    log.debug("消息发送成功: clientId={}, messageId={}", clientId, message.getMessageId());
                } else {
                    // 发送失败，尝试加入队列
                    sessionManager.addQueuedMessage(clientId, message).join();
                    triggerMessageQueued(clientId, message);
                    log.warn("消息发送失败，已加入队列: clientId={}, messageId={}", clientId, message.getMessageId());
                }

                return sent;

            } catch (Exception e) {
                log.error("发送消息到客户端失败: clientId={}, messageId={}", 
                    clientId, message.getMessageId(), e);
                triggerMessageDeliveryFailed(clientId, message, e.getMessage());
                return false;
            }
        });
    }

    @Override
    public CompletableFuture<BatchSendResult> sendMessagesToClients(List<ClientMessage> messages) {
        return CompletableFuture.supplyAsync(() -> {
            long startTime = System.currentTimeMillis();
            int totalSent = messages.size();
            int successfulSent = 0;
            int failedSent = 0;

            try {
                log.debug("批量发送消息: 总数={}", totalSent);

                // 并发发送消息
                List<CompletableFuture<Boolean>> sendTasks = messages.stream()
                        .map(clientMessage -> sendMessageToClient(clientMessage.getClientId(), clientMessage.getMessage()))
                        .collect(Collectors.toList());

                // 等待所有发送任务完成
                CompletableFuture.allOf(sendTasks.toArray(new CompletableFuture[0])).join();

                // 统计结果
                for (CompletableFuture<Boolean> task : sendTasks) {
                    try {
                        if (task.join()) {
                            successfulSent++;
                        } else {
                            failedSent++;
                        }
                    } catch (Exception e) {
                        failedSent++;
                    }
                }

                long sendTime = System.currentTimeMillis() - startTime;
                log.debug("批量发送完成: 总数={}, 成功={}, 失败={}, 耗时={}ms", 
                    totalSent, successfulSent, failedSent, sendTime);

                return new BatchSendResultImpl(totalSent, successfulSent, failedSent, sendTime);

            } catch (Exception e) {
                log.error("批量发送消息失败", e);
                long sendTime = System.currentTimeMillis() - startTime;
                return new BatchSendResultImpl(totalSent, 0, totalSent, sendTime);
            }
        });
    }

    @Override
    public List<ClientSession> getMatchingSubscribers(String topic) {
        return sessionManager.getSubscribedSessions(topic);
    }

    @Override
    public int getSubscriberCount(String topic) {
        return getMatchingSubscribers(topic).size();
    }

    @Override
    public boolean hasSubscribers(String topic) {
        return getSubscriberCount(topic) > 0;
    }

    @Override
    public RouteStats getRouteStats() {
        return stats;
    }

    @Override
    public void addRouteListener(RouteEventListener listener) {
        listeners.add(listener);
        log.debug("添加路由事件监听器: {}", listener.getClass().getSimpleName());
    }

    @Override
    public void removeRouteListener(RouteEventListener listener) {
        listeners.remove(listener);
        log.debug("移除路由事件监听器: {}", listener.getClass().getSimpleName());
    }

    /**
     * 内部消息路由逻辑
     *
     * @param message 消息
     * @param subscribers 订阅者列表
     * @param startTime 开始时间
     * @return 路由结果
     */
    private RouteResult routeMessageInternal(QueuedMessage message, List<ClientSession> subscribers, long startTime) {
        int matchedSubscribers = subscribers.size();
        int successfulDeliveries = 0;
        int failedDeliveries = 0;
        int queuedDeliveries = 0;
        int droppedDeliveries = 0;

        if (matchedSubscribers == 0) {
            log.debug("没有匹配的订阅者: topic={}, messageId={}", message.getTopic(), message.getMessageId());
            long routingTime = System.currentTimeMillis() - startTime;
            return new RouteResultImpl(0, 0, 0, 0, 0, routingTime, true);
        }

        // 创建发送任务列表
        List<ClientMessage> clientMessages = subscribers.stream()
                .map(session -> new ClientMessageImpl(session.getClientId(), message))
                .collect(Collectors.toList());

        // 批量发送消息
        BatchSendResult sendResult = sendMessagesToClients(clientMessages).join();
        successfulDeliveries = sendResult.getSuccessfulSent();
        failedDeliveries = sendResult.getFailedSent();

        // TODO: 这里需要根据实际发送结果更新queuedDeliveries和droppedDeliveries

        long routingTime = System.currentTimeMillis() - startTime;
        boolean allSuccessful = failedDeliveries == 0;

        return new RouteResultImpl(matchedSubscribers, successfulDeliveries, failedDeliveries, 
                                  queuedDeliveries, droppedDeliveries, routingTime, allSuccessful);
    }

    /**
     * 模拟消息发送
     * TODO: 这里需要替换为实际的网络发送逻辑
     *
     * @param session 会话
     * @param message 消息
     * @return 发送结果
     */
    private boolean simulateMessageSending(ClientSession session, QueuedMessage message) {
        // 模拟发送成功率95%
        return Math.random() > 0.05;
    }

    /**
     * 生成唯一消息ID
     *
     * @return 消息ID
     */
    private String generateMessageId() {
        return "msg_" + UUID.randomUUID().toString().replace("-", "");
    }

    // 事件触发方法
    private void triggerRoutingStarted(String topic, String messageId, int subscriberCount) {
        listeners.forEach(listener -> {
            try {
                listener.onRoutingStarted(topic, messageId, subscriberCount);
            } catch (Exception e) {
                log.warn("路由事件监听器异常", e);
            }
        });
    }

    private void triggerRoutingCompleted(String topic, String messageId, RouteResult result) {
        listeners.forEach(listener -> {
            try {
                listener.onRoutingCompleted(topic, messageId, result);
            } catch (Exception e) {
                log.warn("路由事件监听器异常", e);
            }
        });
    }

    private void triggerMessageDelivered(String clientId, QueuedMessage message) {
        listeners.forEach(listener -> {
            try {
                listener.onMessageDelivered(clientId, message);
            } catch (Exception e) {
                log.warn("路由事件监听器异常", e);
            }
        });
    }

    private void triggerMessageDeliveryFailed(String clientId, QueuedMessage message, String reason) {
        listeners.forEach(listener -> {
            try {
                listener.onMessageDeliveryFailed(clientId, message, reason);
            } catch (Exception e) {
                log.warn("路由事件监听器异常", e);
            }
        });
    }

    private void triggerMessageDropped(String clientId, QueuedMessage message, String reason) {
        listeners.forEach(listener -> {
            try {
                listener.onMessageDropped(clientId, message, reason);
            } catch (Exception e) {
                log.warn("路由事件监听器异常", e);
            }
        });
    }

    private void triggerMessageQueued(String clientId, QueuedMessage message) {
        listeners.forEach(listener -> {
            try {
                listener.onMessageQueued(clientId, message);
            } catch (Exception e) {
                log.warn("路由事件监听器异常", e);
            }
        });
    }

    /**
     * 客户端消息实现
     */
    private static class ClientMessageImpl implements ClientMessage {
        private final String clientId;
        private final QueuedMessage message;

        public ClientMessageImpl(String clientId, QueuedMessage message) {
            this.clientId = clientId;
            this.message = message;
        }

        @Override
        public String getClientId() {
            return clientId;
        }

        @Override
        public QueuedMessage getMessage() {
            return message;
        }
    }

    /**
     * 路由结果实现
     */
    private static class RouteResultImpl implements RouteResult {
        private final int matchedSubscribers;
        private final int successfulDeliveries;
        private final int failedDeliveries;
        private final int queuedDeliveries;
        private final int droppedDeliveries;
        private final long routingTimeMs;
        private final boolean allSuccessful;

        public RouteResultImpl(int matchedSubscribers, int successfulDeliveries, int failedDeliveries,
                              int queuedDeliveries, int droppedDeliveries, long routingTimeMs, boolean allSuccessful) {
            this.matchedSubscribers = matchedSubscribers;
            this.successfulDeliveries = successfulDeliveries;
            this.failedDeliveries = failedDeliveries;
            this.queuedDeliveries = queuedDeliveries;
            this.droppedDeliveries = droppedDeliveries;
            this.routingTimeMs = routingTimeMs;
            this.allSuccessful = allSuccessful;
        }

        @Override
        public int getMatchedSubscribers() {
            return matchedSubscribers;
        }

        @Override
        public int getSuccessfulDeliveries() {
            return successfulDeliveries;
        }

        @Override
        public int getFailedDeliveries() {
            return failedDeliveries;
        }

        @Override
        public int getQueuedDeliveries() {
            return queuedDeliveries;
        }

        @Override
        public int getDroppedDeliveries() {
            return droppedDeliveries;
        }

        @Override
        public long getRoutingTimeMs() {
            return routingTimeMs;
        }

        @Override
        public boolean isAllSuccessful() {
            return allSuccessful;
        }
    }

    /**
     * 批量发送结果实现
     */
    private static class BatchSendResultImpl implements BatchSendResult {
        private final int totalSent;
        private final int successfulSent;
        private final int failedSent;
        private final long sendTimeMs;

        public BatchSendResultImpl(int totalSent, int successfulSent, int failedSent, long sendTimeMs) {
            this.totalSent = totalSent;
            this.successfulSent = successfulSent;
            this.failedSent = failedSent;
            this.sendTimeMs = sendTimeMs;
        }

        @Override
        public int getTotalSent() {
            return totalSent;
        }

        @Override
        public int getSuccessfulSent() {
            return successfulSent;
        }

        @Override
        public int getFailedSent() {
            return failedSent;
        }

        @Override
        public long getSendTimeMs() {
            return sendTimeMs;
        }
    }

    /**
     * 路由统计信息实现
     */
    private static class RouteStatsImpl implements RouteStats {
        private final LongAdder totalRoutedMessages = new LongAdder();
        private final LongAdder successfulRoutes = new LongAdder();
        private final LongAdder failedRoutes = new LongAdder();
        private final LongAdder droppedMessages = new LongAdder();
        private final AtomicLong startTime = new AtomicLong(System.currentTimeMillis());

        @Override
        public long getTotalRoutedMessages() {
            return totalRoutedMessages.sum();
        }

        @Override
        public long getSuccessfulRoutes() {
            return successfulRoutes.sum();
        }

        @Override
        public long getFailedRoutes() {
            return failedRoutes.sum();
        }

        @Override
        public long getDroppedMessages() {
            return droppedMessages.sum();
        }

        @Override
        public double getAverageRoutingLatency() {
            // 简单实现，返回估计值
            return 5.0; // 5ms平均延迟
        }

        @Override
        public double getRoutingRate() {
            long uptime = System.currentTimeMillis() - startTime.get();
            return uptime > 0 ? (getTotalRoutedMessages() * 1000.0 / uptime) : 0.0;
        }

        @Override
        public QosRouteStats getQosStats() {
            return new QosRouteStatsImpl();
        }
    }

    /**
     * QoS路由统计信息实现
     */
    private static class QosRouteStatsImpl implements QosRouteStats {
        // TODO: 实现QoS级别的统计分类
        @Override
        public long getQos0Routes() {
            return 0;
        }

        @Override
        public long getQos1Routes() {
            return 0;
        }

        @Override
        public long getQos2Routes() {
            return 0;
        }
    }
}