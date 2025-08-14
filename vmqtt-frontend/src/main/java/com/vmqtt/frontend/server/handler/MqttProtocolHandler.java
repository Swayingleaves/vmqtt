/**
 * MQTT协议处理器
 *
 * @author zhenglin
 * @date 2025/08/06
 */
package com.vmqtt.frontend.server.handler;

import com.vmqtt.common.protocol.packet.*;
import com.vmqtt.common.protocol.packet.connect.MqttConnectPacket;
import com.vmqtt.common.protocol.packet.connack.MqttConnackPacket;
import com.vmqtt.common.protocol.packet.publish.MqttPublishPacket;
import com.vmqtt.common.protocol.packet.puback.MqttPubackPacket;
import com.vmqtt.common.protocol.packet.subscribe.MqttSubscribePacket;
import com.vmqtt.common.protocol.packet.suback.MqttSubackPacket;
import com.vmqtt.frontend.server.VirtualThreadManager;
import com.vmqtt.frontend.service.ConnectionLifecycleManager;
import com.vmqtt.frontend.service.MqttMessageProcessor;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.timeout.IdleStateEvent;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

/**
 * MQTT协议处理器
 * 处理所有MQTT协议消息类型
 */
@Slf4j
@Component
@ChannelHandler.Sharable
@RequiredArgsConstructor
public class MqttProtocolHandler extends SimpleChannelInboundHandler<MqttPacket> {
    
    private final VirtualThreadManager threadManager;
    private final ConnectionLifecycleManager connectionManager;
    private final MqttMessageProcessor messageProcessor;
    
    @Override
    protected void channelRead0(ChannelHandlerContext ctx, MqttPacket packet) throws Exception {
        // 更新连接活动时间
        connectionManager.updateLastActivity(ctx.channel());
        
        // 根据消息类型分发处理
        MqttPacketType packetType = packet.getFixedHeader().packetType();
        
        log.debug("接收到MQTT消息: {} from {}", packetType, ctx.channel().remoteAddress());
        
        // 使用虚拟线程异步处理消息
        threadManager.executeMqttTask(() -> {
            try {
                switch (packetType) {
                    case CONNECT:
                        handleConnect(ctx, (MqttConnectPacket) packet);
                        break;
                    case PUBLISH:
                        handlePublish(ctx, (MqttPublishPacket) packet);
                        break;
                    case PUBACK:
                        handlePuback(ctx, (MqttPubackPacket) packet);
                        break;
                    case SUBSCRIBE:
                        handleSubscribe(ctx, (MqttSubscribePacket) packet);
                        break;
                    case UNSUBSCRIBE:
                        handleUnsubscribe(ctx, packet);
                        break;
                    case PINGREQ:
                        handlePingRequest(ctx);
                        break;
                    case DISCONNECT:
                        handleDisconnect(ctx);
                        break;
                    default:
                        log.warn("未处理的MQTT消息类型: {} from {}", packetType, ctx.channel().remoteAddress());
                        break;
                }
            } catch (Exception e) {
                log.error("处理MQTT消息时发生错误: {} from {}", packetType, ctx.channel().remoteAddress(), e);
                handleProtocolError(ctx, e);
            }
        });
    }
    
    /**
     * 处理CONNECT消息
     */
    private void handleConnect(ChannelHandlerContext ctx, MqttConnectPacket packet) {
        log.debug("处理CONNECT消息: clientId={}", packet.payload().clientId());
        
        threadManager.executeConnectionTask(() -> {
            try {
                MqttConnackPacket connackPacket = messageProcessor.processConnect(ctx.channel(), packet);
                ctx.writeAndFlush(connackPacket);
            } catch (Exception e) {
                log.error("处理CONNECT消息失败", e);
                ctx.close();
            }
        });
    }
    
    /**
     * 处理PUBLISH消息
     */
    private void handlePublish(ChannelHandlerContext ctx, MqttPublishPacket packet) {
        log.debug("处理PUBLISH消息: topic={}, qos={}", 
            packet.variableHeader().topicName(), 
            packet.fixedHeader().qos());
        
        threadManager.executeMessageTask(() -> {
            try {
                messageProcessor.processPublish(ctx.channel(), packet);
                
                // 根据QoS等级发送确认
                if (packet.fixedHeader().qos().ordinal() >= 1) {
                    MqttPubackPacket pubackPacket = messageProcessor.createPubackPacket(packet);
                    ctx.writeAndFlush(pubackPacket);
                }
            } catch (Exception e) {
                log.error("处理PUBLISH消息失败", e);
            }
        });
    }
    
    /**
     * 处理PUBACK消息
     */
    private void handlePuback(ChannelHandlerContext ctx, MqttPubackPacket packet) {
        log.debug("处理PUBACK消息: messageId={}", packet.packetId());
        
        threadManager.executeMessageTask(() -> {
            try {
                messageProcessor.processPuback(ctx.channel(), packet);
            } catch (Exception e) {
                log.error("处理PUBACK消息失败", e);
            }
        });
    }
    
    /**
     * 处理SUBSCRIBE消息
     */
    private void handleSubscribe(ChannelHandlerContext ctx, MqttSubscribePacket packet) {
        log.debug("处理SUBSCRIBE消息: messageId={}", packet.variableHeader().packetId());
        
        threadManager.executeMessageTask(() -> {
            try {
                MqttSubackPacket subackPacket = messageProcessor.processSubscribe(ctx.channel(), packet);
                ctx.writeAndFlush(subackPacket);
            } catch (Exception e) {
                log.error("处理SUBSCRIBE消息失败", e);
            }
        });
    }
    
    /**
     * 处理UNSUBSCRIBE消息
     */
    private void handleUnsubscribe(ChannelHandlerContext ctx, MqttPacket packet) {
        log.debug("处理UNSUBSCRIBE消息");
        
        threadManager.executeMessageTask(() -> {
            try {
                messageProcessor.processUnsubscribe(ctx.channel(), packet);
                // 发送UNSUBACK响应
                // MqttUnsubackPacket unsubackPacket = messageProcessor.createUnsubackPacket(packet);
                // ctx.writeAndFlush(unsubackPacket);
            } catch (Exception e) {
                log.error("处理UNSUBSCRIBE消息失败", e);
            }
        });
    }
    
    /**
     * 处理PINGREQ消息
     */
    private void handlePingRequest(ChannelHandlerContext ctx) {
        log.debug("处理PINGREQ消息");
        
        threadManager.executeHeartbeatTask(() -> {
            try {
                // 发送PINGRESP响应
                MqttPacket pingrespPacket = messageProcessor.createPingrespPacket();
                ctx.writeAndFlush(pingrespPacket);
            } catch (Exception e) {
                log.error("处理PINGREQ消息失败", e);
            }
        });
    }
    
    /**
     * 处理DISCONNECT消息
     */
    private void handleDisconnect(ChannelHandlerContext ctx) {
        log.debug("处理DISCONNECT消息");
        
        threadManager.executeConnectionTask(() -> {
            try {
                messageProcessor.processDisconnect(ctx.channel());
                ctx.close();
            } catch (Exception e) {
                log.error("处理DISCONNECT消息失败", e);
                ctx.close();
            }
        });
    }
    
    /**
     * 处理空闲状态事件
     */
    @Override
    public void userEventTriggered(ChannelHandlerContext ctx, Object evt) throws Exception {
        if (evt instanceof IdleStateEvent) {
            IdleStateEvent idleEvent = (IdleStateEvent) evt;
            log.debug("连接空闲事件: {} from {}", idleEvent.state(), ctx.channel().remoteAddress());
            
            threadManager.executeHeartbeatTask(() -> {
                connectionManager.handleIdleTimeout(ctx.channel(), idleEvent.state());
            });
        } else {
            super.userEventTriggered(ctx, evt);
        }
    }
    
    /**
     * 处理协议错误
     */
    private void handleProtocolError(ChannelHandlerContext ctx, Exception e) {
        log.error("MQTT协议错误: {}", ctx.channel().remoteAddress(), e);
        
        // 发送适当的错误响应或关闭连接
        threadManager.executeConnectionTask(() -> {
            try {
                connectionManager.handleProtocolError(ctx.channel(), e);
            } catch (Exception ex) {
                log.error("处理协议错误时发生异常", ex);
                ctx.close();
            }
        });
    }
    
    /**
     * 连接异常处理
     */
    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
        log.error("MQTT协议处理器异常: {}", ctx.channel().remoteAddress(), cause);
        
        threadManager.executeConnectionTask(() -> {
            try {
                connectionManager.handleConnectionException(ctx.channel(), cause);
            } catch (Exception e) {
                log.error("处理连接异常时发生错误", e);
                ctx.close();
            }
        });
    }
    
    /**
     * 连接激活
     */
    @Override
    public void channelActive(ChannelHandlerContext ctx) throws Exception {
        log.debug("MQTT连接激活: {}", ctx.channel().remoteAddress());
        
        threadManager.executeConnectionTask(() -> {
            connectionManager.handleChannelActive(ctx.channel());
        });
        
        super.channelActive(ctx);
    }
    
    /**
     * 连接关闭
     */
    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
        log.debug("MQTT连接关闭: {}", ctx.channel().remoteAddress());
        
        threadManager.executeConnectionTask(() -> {
            connectionManager.handleChannelInactive(ctx.channel());
        });
        
        super.channelInactive(ctx);
    }
}