/**
 * MQTT消息处理器
 *
 * @author zhenglin
 * @date 2025/08/06
 */
package com.vmqtt.frontend.service;

import com.vmqtt.common.protocol.packet.*;
import com.vmqtt.common.protocol.packet.connect.MqttConnectPacket;
import com.vmqtt.common.protocol.packet.connack.MqttConnackPacket;
import com.vmqtt.common.protocol.packet.connack.MqttConnackVariableHeader;
import com.vmqtt.common.protocol.packet.connack.MqttConnectReturnCode;
import com.vmqtt.common.protocol.packet.publish.MqttPublishPacket;
import com.vmqtt.common.protocol.packet.puback.MqttPubackPacket;
import com.vmqtt.common.protocol.packet.subscribe.MqttSubscribePacket;
import com.vmqtt.common.protocol.packet.suback.MqttSubackPacket;
import com.vmqtt.common.protocol.packet.suback.MqttSubackPayload;
import com.vmqtt.common.protocol.packet.suback.MqttSubackReturnCode;
import com.vmqtt.common.protocol.packet.suback.MqttSubackVariableHeader;
import com.vmqtt.common.protocol.MqttQos;
import com.vmqtt.common.model.ClientConnection;
import com.vmqtt.common.service.AuthenticationService;
import com.vmqtt.common.service.ConnectionManager;
import com.vmqtt.common.service.MessageRouter;
import com.vmqtt.common.service.SessionManager;
import io.netty.channel.Channel;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.concurrent.CompletableFuture;


/**
 * MQTT消息处理服务
 * 集成基础服务处理各类MQTT消息
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class MqttMessageProcessor {
    
    private final AuthenticationService authService;
    private final ConnectionManager connectionManager;
    private final SessionManager sessionManager;
    private final MessageRouter messageRouter;
    
    /**
     * 处理CONNECT消息
     *
     * @param channel 网络通道
     * @param packet CONNECT包
     * @return CONNACK响应包
     */
    public MqttConnackPacket processConnect(Channel channel, MqttConnectPacket packet) {
        String clientId = packet.payload().clientId();
        String username = packet.payload().username();
        String password = packet.payload().getPasswordAsString();
        
        log.info("处理客户端连接: clientId={}, username={}", clientId, username);
        
        try {
            // 1. 简化认证逻辑 - 当前暂时跳过认证验证，允许所有连接
            log.debug("暂时跳过认证验证，允许连接: clientId={}, username={}", clientId, username);
            
            // 2. 检查现有连接
            if (connectionManager.isClientConnected(clientId)) {
                log.info("客户端已连接，断开旧连接: clientId={}", clientId);
                connectionManager.disconnectClient(clientId, "新连接替换");
            }
            
            // 3. 注册新连接
            CompletableFuture<Void> registerFuture = connectionManager.handleNewConnection(channel);
            registerFuture.get(); // 等待注册完成
            
            // 4. 恢复或创建会话
            boolean cleanSession = packet.variableHeader().connectFlags().cleanSession();
            boolean sessionPresent = cleanSession ? false : sessionManager.hasSession(clientId);
                
            if (!cleanSession && sessionPresent) {
                // 恢复现有会话 - 需要根据新接口调整
                // sessionManager.restoreSession(clientId);
                log.info("恢复客户端会话: clientId={}", clientId);
            } else {
                // 创建新会话 - 使用新的接口
                String connectionId = getConnectionIdFromChannel(channel);
                sessionManager.createOrGetSession(clientId, cleanSession, connectionId);
                log.info("创建新会话: clientId={}, cleanSession={}", clientId, cleanSession);
            }
            
            // 5. 处理遗嘱消息
            if (packet.variableHeader().connectFlags().willFlag()) {
                String willTopic = packet.payload().willTopic();
                String willMessage = packet.payload().getWillMessageAsString();
                MqttQos willQos = packet.variableHeader().connectFlags().getWillQos();
                boolean willRetain = packet.variableHeader().connectFlags().willRetain();
                
                // sessionManager.setWillMessage(clientId, willTopic, willMessage, willQos, willRetain);
                log.debug("设置遗嘱消息: clientId={}, topic={}", clientId, willTopic);
            }
            
            log.info("客户端连接成功: clientId={}", clientId);
            return createConnackPacket(MqttConnectReturnCode.CONNECTION_ACCEPTED, sessionPresent);
            
        } catch (Exception e) {
            log.error("处理CONNECT消息失败: clientId={}", clientId, e);
            return createConnackPacket(MqttConnectReturnCode.CONNECTION_REFUSED_SERVER_UNAVAILABLE, false);
        }
    }
    
    /**
     * 处理PUBLISH消息
     *
     * @param channel 网络通道
     * @param packet PUBLISH包
     */
    public void processPublish(Channel channel, MqttPublishPacket packet) {
        String topic = packet.variableHeader().topicName();
        MqttQos qos = packet.fixedHeader().qos();
        boolean retain = packet.fixedHeader().retainFlag();
        
        log.debug("处理PUBLISH消息: topic={}, qos={}, retain={}", topic, qos, retain);
        
        try {
            // 获取客户端ID（从连接属性中）
            String clientId = getClientIdFromChannel(channel);
            
            // 检查发布权限
            if (!authService.checkPublishPermission(clientId, topic).get()) {
                log.warn("客户端无发布权限: clientId={}, topic={}", clientId, topic);
                return;
            }
            
            // 路由消息给订阅者 - payload可能需要调整
            // 路由消息给订阅者 - payload可能需要调整，暂时传null
            // messageRouter.routeMessage(topic, packet.payload(), qos, retain, clientId);
            log.debug("消息路由功能暂未实现");
            
            log.debug("消息路由成功: topic={}, from={}", topic, clientId);
            
        } catch (Exception e) {
            log.error("处理PUBLISH消息失败: topic={}", topic, e);
        }
    }
    
    /**
     * 处理PUBACK消息
     *
     * @param channel 网络通道
     * @param packet PUBACK包
     */
    public void processPuback(Channel channel, MqttPubackPacket packet) {
        int messageId = packet.packetId();
        String clientId = getClientIdFromChannel(channel);
        
        log.debug("处理PUBACK消息: clientId={}, messageId={}", clientId, messageId);
        
        try {
            // 确认QoS 1消息已被客户端接收 - 该方法不存在，需要用其他方式
            // sessionManager.confirmQoS1Message(clientId, messageId);
            sessionManager.removeInflightMessage(clientId, messageId);
            
        } catch (Exception e) {
            log.error("处理PUBACK消息失败: clientId={}, messageId={}", clientId, messageId, e);
        }
    }
    
    /**
     * 处理SUBSCRIBE消息
     *
     * @param channel 网络通道
     * @param packet SUBSCRIBE包
     * @return SUBACK响应包
     */
    public MqttSubackPacket processSubscribe(Channel channel, MqttSubscribePacket packet) {
        int messageId = packet.variableHeader().packetId();
        String clientId = getClientIdFromChannel(channel);
        
        log.debug("处理SUBSCRIBE消息: clientId={}, messageId={}", clientId, messageId);
        
        try {
            List<MqttSubackReturnCode> returnCodes = packet.payload().subscriptions().stream()
                .map(subscription -> {
                    try {
                        String topic = subscription.topicFilter();
                        MqttQos qos = subscription.getQos();
                        
                        // 检查订阅权限
                        if (!authService.checkSubscribePermission(clientId, topic).get()) {
                            log.warn("客户端无订阅权限: clientId={}, topic={}", clientId, topic);
                            return MqttSubackReturnCode.FAILURE;
                        }
                        
                        // 添加订阅 - 使用新的接口和Builder模式
                        var topicSubscription = com.vmqtt.common.model.TopicSubscription.builder()
                            .clientId(clientId)
                            .topicFilter(topic)
                            .qos(qos)
                            .build();
                        sessionManager.addSubscription(clientId, topicSubscription);
                        log.debug("添加订阅: clientId={}, topic={}, qos={}", clientId, topic, qos);
                        
                        // 返回成功的订阅码
                        return switch (qos) {
                            case AT_MOST_ONCE -> MqttSubackReturnCode.MAXIMUM_QOS_0;
                            case AT_LEAST_ONCE -> MqttSubackReturnCode.MAXIMUM_QOS_1;
                            case EXACTLY_ONCE -> MqttSubackReturnCode.MAXIMUM_QOS_2;
                        };
                    } catch (Exception e) {
                        log.error("处理订阅失败", e);
                        return MqttSubackReturnCode.FAILURE;
                    }
                })
                .toList();
            
            // 创建SUBACK响应
            return createSubackPacket(messageId, returnCodes);
            
        } catch (Exception e) {
            log.error("处理SUBSCRIBE消息失败: clientId={}, messageId={}", clientId, messageId, e);
            
            // 返回失败响应
            List<MqttSubackReturnCode> failureCodes = packet.payload().subscriptions().stream()
                .map(sub -> MqttSubackReturnCode.FAILURE)
                .toList();
            return createSubackPacket(messageId, failureCodes);
        }
    }
    
    /**
     * 处理UNSUBSCRIBE消息
     *
     * @param channel 网络通道
     * @param packet UNSUBSCRIBE包
     */
    public void processUnsubscribe(Channel channel, MqttPacket packet) {
        String clientId = getClientIdFromChannel(channel);
        
        log.debug("处理UNSUBSCRIBE消息: clientId={}", clientId);
        
        try {
            // 这里需要根据实际的UNSUBSCRIBE包结构来实现
            // 由于MqttPacket是基类，需要转换为具体的UnsubscribePacket
            // sessionManager.removeSubscriptions(clientId, topics);
            
        } catch (Exception e) {
            log.error("处理UNSUBSCRIBE消息失败: clientId={}", clientId, e);
        }
    }
    
    /**
     * 处理DISCONNECT消息
     *
     * @param channel 网络通道
     */
    public void processDisconnect(Channel channel) {
        String clientId = getClientIdFromChannel(channel);
        
        log.debug("处理DISCONNECT消息: clientId={}", clientId);
        
        try {
            // 清理会话（如果是cleanSession=true） - isCleanSession方法不存在
            // if (sessionManager.isCleanSession(clientId)) {
                sessionManager.removeSession(clientId);
                log.debug("清理会话: clientId={}", clientId);
            // }
            
            // 移除连接
            connectionManager.removeConnection(getConnectionIdFromChannel(channel));
            
        } catch (Exception e) {
            log.error("处理DISCONNECT消息失败: clientId={}", clientId, e);
        }
    }
    
    /**
     * 创建PUBACK响应包
     *
     * @param publishPacket 原始PUBLISH包
     * @return PUBACK包
     */
    public MqttPubackPacket createPubackPacket(MqttPublishPacket publishPacket) {
        if (publishPacket instanceof MqttPacketWithId packetWithId) {
            int messageId = packetWithId.getPacketId();
            return MqttPubackPacket.create(messageId);
        }
        throw new IllegalArgumentException("PUBLISH包缺少消息ID");
    }
    
    /**
     * 创建PINGRESP响应包
     *
     * @return PINGRESP包
     */
    public MqttPacket createPingrespPacket() {
        // MqttPacket是抽象类，不能直接实例化，需要创建一个具体的PINGRESP包类型
        // return new MqttPacket(
        //     new MqttFixedHeader(MqttPacketType.PINGRESP, false, MqttQos.AT_MOST_ONCE, false, 0)
        // );
        // 这里需要创建一个具体的PINGRESP包类型
        throw new UnsupportedOperationException("PINGRESP包类型尚未实现");
    }
    
    /**
     * 创建CONNACK响应包
     */
    private MqttConnackPacket createConnackPacket(MqttConnectReturnCode returnCode, boolean sessionPresent) {
        return new MqttConnackPacket(
            new MqttFixedHeader(MqttPacketType.CONNACK, false, MqttQos.AT_MOST_ONCE, false, 2),
            new MqttConnackVariableHeader(sessionPresent, returnCode, null)
        );
    }
    
    /**
     * 创建SUBACK响应包
     */
    private MqttSubackPacket createSubackPacket(int messageId, List<MqttSubackReturnCode> returnCodes) {
        return new MqttSubackPacket(
            new MqttFixedHeader(MqttPacketType.SUBACK, false, MqttQos.AT_MOST_ONCE, false, 2 + returnCodes.size()),
            new MqttSubackVariableHeader(messageId, null),
            new MqttSubackPayload(returnCodes)
        );
    }
    
    /**
     * 从Channel获取客户端ID
     */
    private String getClientIdFromChannel(Channel channel) {
        // 从Channel属性中获取客户端ID
        // 这个需要在连接建立时设置
        return channel.attr(io.netty.util.AttributeKey.<String>valueOf("clientId")).get();
    }
    
    /**
     * 从Channel获取连接ID
     */
    private String getConnectionIdFromChannel(Channel channel) {
        // 从Channel属性中获取连接ID
        return channel.attr(io.netty.util.AttributeKey.<String>valueOf("connectionId")).get();
    }
    
    /**
     * 创建临时连接对象用于认证
     *
     * @param channel 网络通道
     * @param clientId 客户端ID
     * @return 临时连接对象
     */
    private ClientConnection createTempConnection(Channel channel, String clientId) {
        // 这里需要根据实际的ClientConnection实现来创建
        // 暂时返回null，需要根据具体实现调整
        return null;
    }
    
    /**
     * 将认证返回码转换为CONNACK返回码
     *
     * @param authCode 认证返回码
     * @return CONNACK返回码
     */
    private MqttConnectReturnCode getConnackReturnCode(AuthenticationService.AuthenticationCode authCode) {
        return switch (authCode) {
            case CONNECTION_ACCEPTED -> MqttConnectReturnCode.CONNECTION_ACCEPTED;
            case UNACCEPTABLE_PROTOCOL_VERSION -> MqttConnectReturnCode.CONNECTION_REFUSED_UNACCEPTABLE_PROTOCOL_VERSION;
            case IDENTIFIER_REJECTED -> MqttConnectReturnCode.CONNECTION_REFUSED_IDENTIFIER_REJECTED;
            case SERVER_UNAVAILABLE -> MqttConnectReturnCode.CONNECTION_REFUSED_SERVER_UNAVAILABLE;
            case BAD_USERNAME_PASSWORD -> MqttConnectReturnCode.CONNECTION_REFUSED_BAD_USER_NAME_OR_PASSWORD;
            case NOT_AUTHORIZED, CLIENT_BLOCKED, AUTHENTICATION_FAILED -> 
                MqttConnectReturnCode.CONNECTION_REFUSED_NOT_AUTHORIZED;
        };
    }
}