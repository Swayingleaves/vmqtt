/**
 * MQTT 3.x协议编码器
 *
 * @author zhenglin
 * @date 2025/08/06
 */
package com.vmqtt.common.protocol.codec.v3;

import com.vmqtt.common.protocol.MqttVersion;
import com.vmqtt.common.protocol.codec.MqttCodecException;
import com.vmqtt.common.protocol.codec.MqttCodecUtil;
import com.vmqtt.common.protocol.packet.*;
import com.vmqtt.common.protocol.packet.connack.*;
import com.vmqtt.common.protocol.packet.connect.*;
import com.vmqtt.common.protocol.packet.publish.*;
import com.vmqtt.common.protocol.packet.puback.*;
import com.vmqtt.common.protocol.packet.pubrec.*;
import com.vmqtt.common.protocol.packet.pubrel.*;
import com.vmqtt.common.protocol.packet.pubcomp.*;
import com.vmqtt.common.protocol.packet.subscribe.*;
import com.vmqtt.common.protocol.packet.suback.*;
import io.netty.buffer.ByteBuf;

/**
 * MQTT 3.x协议编码器
 * 
 * 支持MQTT 3.1和3.1.1协议版本
 */
public class MqttV3Encoder {
    
    /**
     * 支持的协议版本
     */
    private final MqttVersion protocolVersion;
    
    /**
     * 获取协议版本
     *
     * @return 协议版本
     */
    public MqttVersion getProtocolVersion() {
        return protocolVersion;
    }
    
    /**
     * 构造函数
     *
     * @param protocolVersion 协议版本
     */
    public MqttV3Encoder(MqttVersion protocolVersion) {
        if (protocolVersion.isMqtt5()) {
            throw new IllegalArgumentException("MQTT 3.x encoder does not support MQTT 5.0");
        }
        this.protocolVersion = protocolVersion;
    }
    
    /**
     * 编码MQTT数据包
     *
     * @param packet MQTT数据包
     * @param buffer 字节缓冲区
     */
    public void encode(MqttPacket packet, ByteBuf buffer) {
        // 验证协议版本兼容性
        MqttCodecUtil.validateVersionCompatibility(protocolVersion, packet.getPacketType());
        
        // 编码固定头部
        MqttCodecUtil.encodeFixedHeader(buffer, packet.getFixedHeader());
        
        // 根据包类型编码可变头部和负载
        switch (packet.getPacketType()) {
            case CONNECT -> encodeConnect((MqttConnectPacket) packet, buffer);
            case CONNACK -> encodeConnack((MqttConnackPacket) packet, buffer);
            case PUBLISH -> encodePublish((MqttPublishPacket) packet, buffer);
            case PUBACK -> encodePuback((MqttPubackPacket) packet, buffer);
            case PUBREC -> encodePubrec((MqttPubrecPacket) packet, buffer);
            case PUBREL -> encodePubrel((MqttPubrelPacket) packet, buffer);
            case PUBCOMP -> encodePubcomp((MqttPubcompPacket) packet, buffer);
            case SUBSCRIBE -> encodeSubscribe((MqttSubscribePacket) packet, buffer);
            case SUBACK -> encodeSuback((MqttSubackPacket) packet, buffer);
            case PINGREQ, PINGRESP, DISCONNECT -> {
                // 这些包没有可变头部和负载
            }
            default -> throw new MqttCodecException("Unsupported packet type: " + packet.getPacketType());
        }
    }
    
    /**
     * 编码CONNECT包
     */
    private void encodeConnect(MqttConnectPacket packet, ByteBuf buffer) {
        MqttConnectVariableHeader variableHeader = packet.variableHeader();
        MqttConnectPayload payload = packet.payload();
        
        // 编码协议名
        MqttCodecUtil.encodeString(buffer, variableHeader.getProtocolName());
        
        // 编码协议级别
        buffer.writeByte(variableHeader.getProtocolLevel());
        
        // 编码连接标志
        buffer.writeByte(variableHeader.connectFlags().toByte());
        
        // 编码保活时间
        buffer.writeShort(variableHeader.keepAlive());
        
        // 编码负载
        encodeConnectPayload(payload, buffer);
    }
    
    /**
     * 编码CONNECT包负载
     */
    private void encodeConnectPayload(MqttConnectPayload payload, ByteBuf buffer) {
        // 客户端ID
        MqttCodecUtil.encodeString(buffer, payload.clientId());
        
        // 遗嘱主题和消息
        if (payload.willTopic() != null) {
            MqttCodecUtil.encodeString(buffer, payload.willTopic());
            MqttCodecUtil.encodeBinaryData(buffer, payload.willMessage());
        }
        
        // 用户名
        if (payload.username() != null) {
            MqttCodecUtil.encodeString(buffer, payload.username());
        }
        
        // 密码
        if (payload.password() != null) {
            MqttCodecUtil.encodeBinaryData(buffer, payload.password());
        }
    }
    
    /**
     * 编码CONNACK包
     */
    private void encodeConnack(MqttConnackPacket packet, ByteBuf buffer) {
        MqttConnackVariableHeader variableHeader = packet.variableHeader();
        
        // 编码连接确认标志
        buffer.writeByte(variableHeader.getAcknowledgeFlags());
        
        // 编码连接返回码
        buffer.writeByte(variableHeader.returnCode().getValue());
    }
    
    /**
     * 编码PUBLISH包
     */
    private void encodePublish(MqttPublishPacket packet, ByteBuf buffer) {
        MqttPublishVariableHeader variableHeader = packet.variableHeader();
        
        // 编码主题名
        MqttCodecUtil.encodeString(buffer, variableHeader.topicName());
        
        // QoS > 0时编码包标识符
        if (packet.getQos().getValue() > 0) {
            MqttCodecUtil.encodePacketId(buffer, variableHeader.packetId());
        }
        
        // 编码负载
        if (packet.payload() != null) {
            buffer.writeBytes(packet.payload());
        }
    }
    
    /**
     * 编码PUBACK包
     */
    private void encodePuback(MqttPubackPacket packet, ByteBuf buffer) {
        MqttCodecUtil.encodePacketId(buffer, packet.getPacketId());
    }
    
    /**
     * 编码PUBREC包
     */
    private void encodePubrec(MqttPubrecPacket packet, ByteBuf buffer) {
        MqttCodecUtil.encodePacketId(buffer, packet.getPacketId());
    }
    
    /**
     * 编码PUBREL包
     */
    private void encodePubrel(MqttPubrelPacket packet, ByteBuf buffer) {
        MqttCodecUtil.encodePacketId(buffer, packet.getPacketId());
    }
    
    /**
     * 编码PUBCOMP包
     */
    private void encodePubcomp(MqttPubcompPacket packet, ByteBuf buffer) {
        MqttCodecUtil.encodePacketId(buffer, packet.getPacketId());
    }
    
    /**
     * 编码SUBSCRIBE包
     */
    private void encodeSubscribe(MqttSubscribePacket packet, ByteBuf buffer) {
        // 编码包标识符
        MqttCodecUtil.encodePacketId(buffer, packet.getPacketId());
        
        // 编码订阅列表
        for (MqttTopicSubscription subscription : packet.getSubscriptions()) {
            MqttCodecUtil.encodeString(buffer, subscription.topicFilter());
            buffer.writeByte(subscription.qos());
        }
    }
    
    /**
     * 编码SUBACK包
     */
    private void encodeSuback(MqttSubackPacket packet, ByteBuf buffer) {
        // 编码包标识符
        MqttCodecUtil.encodePacketId(buffer, packet.getPacketId());
        
        // 编码返回码列表
        for (MqttSubackReturnCode returnCode : packet.getReturnCodes()) {
            buffer.writeByte(returnCode.getValue());
        }
    }
    
    /**
     * 计算CONNECT包的剩余长度
     */
    public static int calculateConnectRemainingLength(MqttConnectPacket packet) {
        MqttConnectVariableHeader variableHeader = packet.variableHeader();
        MqttConnectPayload payload = packet.payload();
        
        int length = 0;
        
        // 协议名长度
        length += MqttCodecUtil.getStringEncodedLength(variableHeader.getProtocolName());
        
        // 协议级别 + 连接标志 + 保活时间
        length += 1 + 1 + 2;
        
        // 客户端ID
        length += MqttCodecUtil.getStringEncodedLength(payload.clientId());
        
        // 遗嘱主题和消息
        if (payload.willTopic() != null) {
            length += MqttCodecUtil.getStringEncodedLength(payload.willTopic());
            length += MqttCodecUtil.getBinaryDataEncodedLength(payload.willMessage());
        }
        
        // 用户名
        if (payload.username() != null) {
            length += MqttCodecUtil.getStringEncodedLength(payload.username());
        }
        
        // 密码
        if (payload.password() != null) {
            length += MqttCodecUtil.getBinaryDataEncodedLength(payload.password());
        }
        
        return length;
    }
    
    /**
     * 计算PUBLISH包的剩余长度
     */
    public static int calculatePublishRemainingLength(MqttPublishPacket packet) {
        int length = 0;
        
        // 主题名
        length += MqttCodecUtil.getStringEncodedLength(packet.getTopicName());
        
        // 包标识符（QoS > 0时）
        if (packet.getQos().getValue() > 0) {
            length += 2;
        }
        
        // 负载
        if (packet.payload() != null) {
            length += packet.payload().length;
        }
        
        return length;
    }
    
    /**
     * 计算SUBSCRIBE包的剩余长度
     */
    public static int calculateSubscribeRemainingLength(MqttSubscribePacket packet) {
        int length = 2; // 包标识符
        
        // 订阅列表
        for (MqttTopicSubscription subscription : packet.getSubscriptions()) {
            length += MqttCodecUtil.getStringEncodedLength(subscription.topicFilter());
            length += 1; // QoS字节
        }
        
        return length;
    }
    
    /**
     * 计算SUBACK包的剩余长度
     */
    public static int calculateSubackRemainingLength(MqttSubackPacket packet) {
        return 2 + packet.getReturnCodes().size(); // 包标识符 + 返回码列表
    }
}