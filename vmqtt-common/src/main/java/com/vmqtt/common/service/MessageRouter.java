/**
 * 消息路由器接口
 *
 * @author zhenglin
 * @date 2025/08/06
 */
package com.vmqtt.common.service;

import com.vmqtt.common.model.ClientSession;
import com.vmqtt.common.model.QueuedMessage;
import com.vmqtt.common.protocol.MqttQos;
import io.netty.buffer.ByteBuf;

import java.util.List;
import java.util.concurrent.CompletableFuture;

/**
 * 消息路由器接口
 * 负责MQTT消息的路由、分发和传递
 */
public interface MessageRouter {
    
    /**
     * 路由消息到匹配的订阅者
     *
     * @param publisherClientId 发布者客户端ID
     * @param topic 主题
     * @param payload 消息负载
     * @param qos QoS等级
     * @param retain 保留消息标志
     * @return 异步操作结果，包含路由统计信息
     */
    CompletableFuture<RouteResult> routeMessage(String publisherClientId, String topic, 
                                               ByteBuf payload, MqttQos qos, boolean retain);
    
    /**
     * 路由消息到匹配的订阅者
     *
     * @param message 排队消息
     * @return 异步操作结果，包含路由统计信息
     */
    CompletableFuture<RouteResult> routeMessage(QueuedMessage message);
    
    /**
     * 直接发送消息到指定客户端
     *
     * @param clientId 目标客户端ID
     * @param message 消息
     * @return 异步操作结果
     */
    CompletableFuture<Boolean> sendMessageToClient(String clientId, QueuedMessage message);
    
    /**
     * 批量发送消息到多个客户端
     *
     * @param messages 消息和目标客户端的映射
     * @return 异步操作结果，包含发送统计信息
     */
    CompletableFuture<BatchSendResult> sendMessagesToClients(List<ClientMessage> messages);
    
    /**
     * 获取匹配指定主题的所有订阅者
     *
     * @param topic 主题
     * @return 匹配的订阅者会话列表
     */
    List<ClientSession> getMatchingSubscribers(String topic);
    
    /**
     * 获取匹配指定主题的订阅者数量
     *
     * @param topic 主题
     * @return 订阅者数量
     */
    int getSubscriberCount(String topic);
    
    /**
     * 检查主题是否有订阅者
     *
     * @param topic 主题
     * @return 如果有订阅者返回true
     */
    boolean hasSubscribers(String topic);
    
    /**
     * 获取路由统计信息
     *
     * @return 路由统计信息
     */
    RouteStats getRouteStats();
    
    /**
     * 注册消息路由事件监听器
     *
     * @param listener 路由事件监听器
     */
    void addRouteListener(RouteEventListener listener);
    
    /**
     * 移除消息路由事件监听器
     *
     * @param listener 路由事件监听器
     */
    void removeRouteListener(RouteEventListener listener);
    
    /**
     * 客户端消息对
     */
    interface ClientMessage {
        /**
         * 获取目标客户端ID
         *
         * @return 客户端ID
         */
        String getClientId();
        
        /**
         * 获取消息
         *
         * @return 消息
         */
        QueuedMessage getMessage();
    }
    
    /**
     * 路由结果
     */
    interface RouteResult {
        /**
         * 获取匹配的订阅者数量
         *
         * @return 订阅者数量
         */
        int getMatchedSubscribers();
        
        /**
         * 获取成功发送数量
         *
         * @return 成功发送数量
         */
        int getSuccessfulDeliveries();
        
        /**
         * 获取失败发送数量
         *
         * @return 失败发送数量
         */
        int getFailedDeliveries();
        
        /**
         * 获取排队发送数量
         *
         * @return 排队发送数量
         */
        int getQueuedDeliveries();
        
        /**
         * 获取丢弃数量
         *
         * @return 丢弃数量
         */
        int getDroppedDeliveries();
        
        /**
         * 获取路由耗时（毫秒）
         *
         * @return 路由耗时
         */
        long getRoutingTimeMs();
        
        /**
         * 是否所有发送都成功
         *
         * @return 如果所有发送都成功返回true
         */
        boolean isAllSuccessful();
    }
    
    /**
     * 批量发送结果
     */
    interface BatchSendResult {
        /**
         * 获取总发送数量
         *
         * @return 总发送数量
         */
        int getTotalSent();
        
        /**
         * 获取成功发送数量
         *
         * @return 成功发送数量
         */
        int getSuccessfulSent();
        
        /**
         * 获取失败发送数量
         *
         * @return 失败发送数量
         */
        int getFailedSent();
        
        /**
         * 获取发送耗时（毫秒）
         *
         * @return 发送耗时
         */
        long getSendTimeMs();
    }
    
    /**
     * 路由统计信息
     */
    interface RouteStats {
        /**
         * 获取总路由消息数
         *
         * @return 总路由消息数
         */
        long getTotalRoutedMessages();
        
        /**
         * 获取成功路由数量
         *
         * @return 成功路由数量
         */
        long getSuccessfulRoutes();
        
        /**
         * 获取失败路由数量
         *
         * @return 失败路由数量
         */
        long getFailedRoutes();
        
        /**
         * 获取丢弃消息数量
         *
         * @return 丢弃消息数量
         */
        long getDroppedMessages();
        
        /**
         * 获取平均路由延迟
         *
         * @return 平均路由延迟（毫秒）
         */
        double getAverageRoutingLatency();
        
        /**
         * 获取路由速率（每秒）
         *
         * @return 路由速率
         */
        double getRoutingRate();
        
        /**
         * 获取各QoS级别的路由统计
         *
         * @return QoS路由统计
         */
        QosRouteStats getQosStats();
    }
    
    /**
     * QoS路由统计信息
     */
    interface QosRouteStats {
        /**
         * 获取QoS0路由数量
         *
         * @return QoS0路由数量
         */
        long getQos0Routes();
        
        /**
         * 获取QoS1路由数量
         *
         * @return QoS1路由数量
         */
        long getQos1Routes();
        
        /**
         * 获取QoS2路由数量
         *
         * @return QoS2路由数量
         */
        long getQos2Routes();
    }
    
    /**
     * 路由事件监听器
     */
    interface RouteEventListener {
        /**
         * 消息路由开始事件
         *
         * @param topic 主题
         * @param messageId 消息ID
         * @param subscriberCount 订阅者数量
         */
        void onRoutingStarted(String topic, String messageId, int subscriberCount);
        
        /**
         * 消息路由完成事件
         *
         * @param topic 主题
         * @param messageId 消息ID
         * @param result 路由结果
         */
        void onRoutingCompleted(String topic, String messageId, RouteResult result);
        
        /**
         * 消息发送成功事件
         *
         * @param clientId 客户端ID
         * @param message 消息
         */
        void onMessageDelivered(String clientId, QueuedMessage message);
        
        /**
         * 消息发送失败事件
         *
         * @param clientId 客户端ID
         * @param message 消息
         * @param reason 失败原因
         */
        void onMessageDeliveryFailed(String clientId, QueuedMessage message, String reason);
        
        /**
         * 消息丢弃事件
         *
         * @param clientId 客户端ID
         * @param message 消息
         * @param reason 丢弃原因
         */
        void onMessageDropped(String clientId, QueuedMessage message, String reason);
        
        /**
         * 消息排队事件
         *
         * @param clientId 客户端ID
         * @param message 消息
         */
        void onMessageQueued(String clientId, QueuedMessage message);
    }
}