/**
 * 服务配置类
 *
 * @author zhenglin
 * @date 2025/08/06
 */
package com.vmqtt.vmqttcore.config;

import com.vmqtt.common.service.AuthenticationService;
import com.vmqtt.common.service.ConnectionManager;
import com.vmqtt.common.service.MessageRouter;
import com.vmqtt.common.service.SessionManager;
import com.vmqtt.common.service.impl.AuthenticationServiceImpl;
import com.vmqtt.common.service.impl.ConnectionManagerImpl;
import com.vmqtt.common.service.impl.MessageRouterImpl;
import com.vmqtt.common.service.impl.SessionManagerImpl;
import com.vmqtt.vmqttcore.cluster.InMemoryServiceRegistry;
import com.vmqtt.vmqttcore.cluster.ServiceRegistry;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;

import javax.annotation.PostConstruct;

/**
 * V-MQTT核心服务配置类
 * 配置和注册所有基础服务的Spring Bean
 */
@Slf4j
@Configuration
public class ServiceConfiguration {

    /**
     * 连接管理器Bean
     * 
     * @return ConnectionManager实例
     */
    @Bean
    @Primary
    public ConnectionManager connectionManager() {
        log.info("创建连接管理器Bean");
        ConnectionManagerImpl connectionManager = new ConnectionManagerImpl();
        
        // 添加连接事件监听器进行日志记录
        connectionManager.addConnectionListener(new ConnectionEventLogger());
        
        return connectionManager;
    }

    /**
     * 会话管理器Bean
     * 
     * @return SessionManager实例
     */
    @Bean
    @Primary
    public SessionManager sessionManager() {
        log.info("创建会话管理器Bean");
        SessionManagerImpl sessionManager = new SessionManagerImpl();
        
        // 添加会话事件监听器进行日志记录
        sessionManager.addSessionListener(new SessionEventLogger());
        
        return sessionManager;
    }

    /**
     * 消息路由器Bean
     * 
     * @return MessageRouter实例
     */
    @Bean
    @Primary
    public MessageRouter messageRouter() {
        log.info("创建消息路由器Bean");
        MessageRouterImpl messageRouter = new MessageRouterImpl();
        
        // 添加路由事件监听器进行日志记录
        messageRouter.addRouteListener(new RouteEventLogger());
        
        return messageRouter;
    }

    /**
     * 轻量服务注册发现Bean（无外部中间件）
     */
    @Bean
    @Primary
    public ServiceRegistry serviceRegistry() {
        log.info("创建内存服务注册表Bean");
        return new InMemoryServiceRegistry();
    }

    /**
     * 认证服务Bean
     * 
     * @return AuthenticationService实例
     */
    @Bean
    @Primary
    public AuthenticationService authenticationService() {
        log.info("创建认证服务Bean");
        AuthenticationServiceImpl authService = new AuthenticationServiceImpl();
        
        // 初始化认证服务（创建默认管理员账户等）
        authService.init();
        
        // 添加认证事件监听器进行日志记录
        authService.addAuthenticationListener(new AuthenticationEventLogger());
        
        return authService;
    }

    /**
     * 初始化回调方法
     */
    @PostConstruct
    public void init() {
        log.info("V-MQTT核心服务配置初始化完成");
        log.info("已注册的服务：ConnectionManager, SessionManager, MessageRouter, AuthenticationService");
    }

    /**
     * 连接事件日志记录器
     */
    private static class ConnectionEventLogger implements ConnectionManager.ConnectionEventListener {
        
        @Override
        public void onConnectionEstablished(com.vmqtt.common.model.ClientConnection connection) {
            log.debug("连接建立事件: connectionId={}, clientId={}, remoteAddress={}", 
                connection.getConnectionId(), connection.getClientId(), connection.getRemoteAddress());
        }

        @Override
        public void onConnectionClosed(com.vmqtt.common.model.ClientConnection connection, String reason) {
            log.debug("连接关闭事件: connectionId={}, clientId={}, reason={}", 
                connection.getConnectionId(), connection.getClientId(), reason);
        }

        @Override
        public void onConnectionFailed(String connectionId, String reason) {
            log.warn("连接失败事件: connectionId={}, reason={}", connectionId, reason);
        }

        @Override
        public void onConnectionTimeout(com.vmqtt.common.model.ClientConnection connection) {
            log.warn("连接超时事件: connectionId={}, clientId={}", 
                connection.getConnectionId(), connection.getClientId());
        }
    }

    /**
     * 会话事件日志记录器
     */
    private static class SessionEventLogger implements SessionManager.SessionEventListener {
        
        @Override
        public void onSessionCreated(com.vmqtt.common.model.ClientSession session) {
            log.debug("会话创建事件: sessionId={}, clientId={}", 
                session.getSessionId(), session.getClientId());
        }

        @Override
        public void onSessionDestroyed(com.vmqtt.common.model.ClientSession session) {
            log.debug("会话销毁事件: sessionId={}, clientId={}", 
                session.getSessionId(), session.getClientId());
        }

        @Override
        public void onSessionStateChanged(com.vmqtt.common.model.ClientSession session, 
                                        com.vmqtt.common.model.ClientSession.SessionState oldState, 
                                        com.vmqtt.common.model.ClientSession.SessionState newState) {
            log.debug("会话状态变化事件: sessionId={}, clientId={}, oldState={}, newState={}", 
                session.getSessionId(), session.getClientId(), oldState, newState);
        }

        @Override
        public void onSubscriptionAdded(com.vmqtt.common.model.ClientSession session, 
                                       com.vmqtt.common.model.TopicSubscription subscription) {
            log.debug("订阅添加事件: sessionId={}, clientId={}, topicFilter={}, qos={}", 
                session.getSessionId(), session.getClientId(), 
                subscription.getTopicFilter(), subscription.getQos());
        }

        @Override
        public void onSubscriptionRemoved(com.vmqtt.common.model.ClientSession session, 
                                         com.vmqtt.common.model.TopicSubscription subscription) {
            log.debug("订阅移除事件: sessionId={}, clientId={}, topicFilter={}", 
                session.getSessionId(), session.getClientId(), subscription.getTopicFilter());
        }

        @Override
        public void onMessageQueued(com.vmqtt.common.model.ClientSession session, 
                                   com.vmqtt.common.model.QueuedMessage message) {
            log.debug("消息入队事件: sessionId={}, clientId={}, messageId={}, topic={}", 
                session.getSessionId(), session.getClientId(), 
                message.getMessageId(), message.getTopic());
        }

        @Override
        public void onMessageDequeued(com.vmqtt.common.model.ClientSession session, 
                                     com.vmqtt.common.model.QueuedMessage message) {
            log.debug("消息出队事件: sessionId={}, clientId={}, messageId={}, topic={}", 
                session.getSessionId(), session.getClientId(), 
                message.getMessageId(), message.getTopic());
        }
    }

    /**
     * 路由事件日志记录器
     */
    private static class RouteEventLogger implements MessageRouter.RouteEventListener {
        
        @Override
        public void onRoutingStarted(String topic, String messageId, int subscriberCount) {
            log.debug("消息路由开始事件: topic={}, messageId={}, subscriberCount={}", 
                topic, messageId, subscriberCount);
        }

        @Override
        public void onRoutingCompleted(String topic, String messageId, MessageRouter.RouteResult result) {
            log.debug("消息路由完成事件: topic={}, messageId={}, matched={}, successful={}, failed={}, time={}ms", 
                topic, messageId, result.getMatchedSubscribers(), 
                result.getSuccessfulDeliveries(), result.getFailedDeliveries(), result.getRoutingTimeMs());
        }

        @Override
        public void onMessageDelivered(String clientId, com.vmqtt.common.model.QueuedMessage message) {
            log.debug("消息投递成功事件: clientId={}, messageId={}, topic={}", 
                clientId, message.getMessageId(), message.getTopic());
        }

        @Override
        public void onMessageDeliveryFailed(String clientId, com.vmqtt.common.model.QueuedMessage message, String reason) {
            log.warn("消息投递失败事件: clientId={}, messageId={}, topic={}, reason={}", 
                clientId, message.getMessageId(), message.getTopic(), reason);
        }

        @Override
        public void onMessageDropped(String clientId, com.vmqtt.common.model.QueuedMessage message, String reason) {
            log.warn("消息丢弃事件: clientId={}, messageId={}, topic={}, reason={}", 
                clientId, message.getMessageId(), message.getTopic(), reason);
        }

        @Override
        public void onMessageQueued(String clientId, com.vmqtt.common.model.QueuedMessage message) {
            log.debug("消息排队事件: clientId={}, messageId={}, topic={}", 
                clientId, message.getMessageId(), message.getTopic());
        }
    }

    /**
     * 认证事件日志记录器
     */
    private static class AuthenticationEventLogger implements AuthenticationService.AuthenticationEventListener {
        
        @Override
        public void onAuthenticationSuccess(String clientId, AuthenticationService.AuthenticationResult result) {
            log.info("认证成功事件: clientId={}, returnCode={}", clientId, result.getReturnCode());
        }

        @Override
        public void onAuthenticationFailure(String clientId, AuthenticationService.AuthenticationResult result) {
            log.warn("认证失败事件: clientId={}, returnCode={}, reason={}", 
                clientId, result.getReturnCode(), result.getFailureReason());
        }

        @Override
        public void onPermissionDenied(String clientId, String action, String resource) {
            log.warn("权限拒绝事件: clientId={}, action={}, resource={}", clientId, action, resource);
        }

        @Override
        public void onClientBlocked(String clientId, String reason) {
            log.warn("客户端禁用事件: clientId={}, reason={}", clientId, reason);
        }

        @Override
        public void onClientUnblocked(String clientId) {
            log.info("客户端解禁事件: clientId={}", clientId);
        }

        @Override
        public void onCredentialsExpired(String clientId, AuthenticationService.AuthenticationCredentials credentials) {
            log.warn("凭据过期事件: clientId={}, username={}, expiresAt={}", 
                clientId, credentials.getUsername(), credentials.getExpiresAt());
        }
    }
}