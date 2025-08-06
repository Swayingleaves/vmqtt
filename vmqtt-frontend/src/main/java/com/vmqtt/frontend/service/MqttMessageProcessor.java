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
        String clientId = packet.getPayload().getClientId();
        String username = packet.getPayload().getUsername();
        String password = packet.getPayload().getPassword();
        
        log.info("处理客户端连接: clientId={}, username={}", clientId, username);
        
        try {
            // 1. 客户端认证
            boolean authenticated = authService.authenticate(clientId, username, password);
            if (!authenticated) {
                log.warn("客户端认证失败: clientId={}", clientId);
                return createConnackPacket(MqttConnectReturnCode.CONNECTION_REFUSED_BAD_USERNAME_OR_PASSWORD, false);
            }
            
            // 2. 检查客户端授权
            boolean authorized = authService.authorize(clientId, "CONNECT", null);
            if (!authorized) {
                log.warn("客户端授权失败: clientId={}", clientId);
                return createConnackPacket(MqttConnectReturnCode.CONNECTION_REFUSED_NOT_AUTHORIZED, false);
            }
            
            // 3. 检查现有连接
            if (connectionManager.isClientConnected(clientId)) {
                log.info("客户端已连接，断开旧连接: clientId={}", clientId);
                connectionManager.disconnectClient(clientId, "新连接替换");
            }
            
            // 4. 注册新连接
            CompletableFuture<Void> registerFuture = connectionManager.handleNewConnection(channel);
            registerFuture.get(); // 等待注册完成
            
            // 5. 恢复或创建会话
            boolean sessionPresent = packet.getVariableHeader().hasCleanSession() ? false : 
                sessionManager.hasSession(clientId);
                
            if (!packet.getVariableHeader().hasCleanSession() && sessionPresent) {
                // 恢复现有会话
                sessionManager.restoreSession(clientId);
                log.info("恢复客户端会话: clientId={}", clientId);
            } else {
                // 创建新会话
                sessionManager.createSession(clientId, packet.getVariableHeader().hasCleanSession());
                log.info("创建新会话: clientId={}, cleanSession={}", 
                    clientId, packet.getVariableHeader().hasCleanSession());
            }
            
            // 6. 处理遗嘱消息
            if (packet.getVariableHeader().hasWillFlag()) {
                String willTopic = packet.getPayload().getWillTopic();
                String willMessage = packet.getPayload().getWillMessage();
                MqttQos willQos = MqttQos.valueOf(packet.getVariableHeader().getWillQos());
                boolean willRetain = packet.getVariableHeader().hasWillRetain();
                
                sessionManager.setWillMessage(clientId, willTopic, willMessage, willQos, willRetain);
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
        String topic = packet.getVariableHeader().getTopicName();
        MqttQos qos = packet.getFixedHeader().getQos();
        boolean retain = packet.getFixedHeader().isRetain();
        
        log.debug("处理PUBLISH消息: topic={}, qos={}, retain={}", topic, qos, retain);
        
        try {
            // 获取客户端ID（从连接属性中）
            String clientId = getClientIdFromChannel(channel);
            
            // 检查发布权限
            if (!authService.authorize(clientId, "PUBLISH", topic)) {
                log.warn("客户端无发布权限: clientId={}, topic={}", clientId, topic);
                return;
            }
            
            // 路由消息给订阅者
            messageRouter.routeMessage(topic, packet.getPayload(), qos, retain, clientId);
            
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
        int messageId = packet.getVariableHeader().getMessageId();
        String clientId = getClientIdFromChannel(channel);
        
        log.debug("处理PUBACK消息: clientId={}, messageId={}", clientId, messageId);
        
        try {
            // 确认QoS 1消息已被客户端接收
            sessionManager.confirmQoS1Message(clientId, messageId);
            
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
        int messageId = packet.getVariableHeader().getMessageId();
        String clientId = getClientIdFromChannel(channel);
        
        log.debug("处理SUBSCRIBE消息: clientId={}, messageId={}", clientId, messageId);
        
        try {
            List<MqttSubackReturnCode> returnCodes = packet.getPayload().getTopicSubscriptions().stream()
                .map(subscription -> {
                    String topic = subscription.getTopicFilter();
                    MqttQos qos = subscription.getQos();
                    
                    // 检查订阅权限
                    if (!authService.authorize(clientId, "SUBSCRIBE", topic)) {
                        log.warn("客户端无订阅权限: clientId={}, topic={}", clientId, topic);
                        return MqttSubackReturnCode.FAILURE;
                    }
                    
                    // 添加订阅
                    sessionManager.addSubscription(clientId, topic, qos);
                    log.debug("添加订阅: clientId={}, topic={}, qos={}", clientId, topic, qos);
                    
                    return MqttSubackReturnCode.valueOf(qos.ordinal());
                })
                .toList();
            
            // 创建SUBACK响应
            return createSubackPacket(messageId, returnCodes);
            
        } catch (Exception e) {
            log.error("处理SUBSCRIBE消息失败: clientId={}, messageId={}", clientId, messageId, e);
            
            // 返回失败响应
            List<MqttSubackReturnCode> failureCodes = packet.getPayload().getTopicSubscriptions().stream()
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
            // 清理会话（如果是cleanSession=true）
            if (sessionManager.isCleanSession(clientId)) {
                sessionManager.removeSession(clientId);
                log.debug("清理会话: clientId={}", clientId);
            }
            
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
            int messageId = packetWithId.getVariableHeader().getMessageId();
            return new MqttPubackPacket(
                new MqttFixedHeader(MqttPacketType.PUBACK, false, MqttQos.AT_MOST_ONCE, false, 2),
                new com.vmqtt.common.protocol.packet.puback.MqttPubackVariableHeader(messageId)
            );
        }
        throw new IllegalArgumentException("PUBLISH包缺少消息ID");
    }
    
    /**
     * 创建PINGRESP响应包
     *
     * @return PINGRESP包
     */
    public MqttPacket createPingrespPacket() {
        return new MqttPacket(
            new MqttFixedHeader(MqttPacketType.PINGRESP, false, MqttQos.AT_MOST_ONCE, false, 0)
        );
    }
    
    /**
     * 创建CONNACK响应包
     */
    private MqttConnackPacket createConnackPacket(MqttConnectReturnCode returnCode, boolean sessionPresent) {
        return new MqttConnackPacket(
            new MqttFixedHeader(MqttPacketType.CONNACK, false, MqttQos.AT_MOST_ONCE, false, 2),
            new MqttConnackVariableHeader(returnCode, sessionPresent)
        );
    }
    
    /**
     * 创建SUBACK响应包
     */
    private MqttSubackPacket createSubackPacket(int messageId, List<MqttSubackReturnCode> returnCodes) {
        return new MqttSubackPacket(
            new MqttFixedHeader(MqttPacketType.SUBACK, false, MqttQos.AT_MOST_ONCE, false, 2 + returnCodes.size()),
            new MqttSubackVariableHeader(messageId),
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
}