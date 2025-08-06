/**
 * MQTT 3.x协议解码器
 *
 * @author zhenglin
 * @date 2025/08/06
 */
package com.vmqtt.common.protocol.codec.v3;

import com.vmqtt.common.protocol.MqttQos;
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

import java.util.ArrayList;
import java.util.List;

/**
 * MQTT 3.x协议解码器
 * 
 * 支持MQTT 3.1和3.1.1协议版本
 */
public class MqttV3Decoder {
    
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
    public MqttV3Decoder(MqttVersion protocolVersion) {
        if (protocolVersion.isMqtt5()) {
            throw new IllegalArgumentException("MQTT 3.x decoder does not support MQTT 5.0");
        }
        this.protocolVersion = protocolVersion;
    }
    
    /**
     * 解码MQTT数据包
     *
     * @param buffer 字节缓冲区
     * @return MQTT数据包，如果数据不足返回null
     */
    public MqttPacket decode(ByteBuf buffer) {
        // 解码固定头部
        MqttFixedHeader fixedHeader = MqttCodecUtil.decodeFixedHeader(buffer);
        if (fixedHeader == null) {
            return null; // 数据不足
        }
        
        // 检查是否有足够的剩余数据
        if (!MqttCodecUtil.hasEnoughBytes(buffer, fixedHeader.remainingLength())) {
            // 重置读取位置
            buffer.resetReaderIndex();
            return null;
        }
        
        // 根据包类型解码
        return switch (fixedHeader.packetType()) {
            case CONNECT -> decodeConnect(fixedHeader, buffer);
            case CONNACK -> decodeConnack(fixedHeader, buffer);
            case PUBLISH -> decodePublish(fixedHeader, buffer);
            case PUBACK -> decodePuback(fixedHeader, buffer);
            case PUBREC -> decodePubrec(fixedHeader, buffer);
            case PUBREL -> decodePubrel(fixedHeader, buffer);
            case PUBCOMP -> decodePubcomp(fixedHeader, buffer);
            case SUBSCRIBE -> decodeSubscribe(fixedHeader, buffer);
            case SUBACK -> decodeSuback(fixedHeader, buffer);
            case UNSUBSCRIBE -> decodeUnsubscribe(fixedHeader, buffer);
            case UNSUBACK -> decodeUnsuback(fixedHeader, buffer);
            case PINGREQ -> decodePingreq(fixedHeader, buffer);
            case PINGRESP -> decodePingresp(fixedHeader, buffer);
            case DISCONNECT -> decodeDisconnect(fixedHeader, buffer);
            default -> throw new MqttCodecException("Unsupported packet type: " + fixedHeader.packetType());
        };
    }
    
    /**
     * 解码CONNECT包
     */
    private MqttConnectPacket decodeConnect(MqttFixedHeader fixedHeader, ByteBuf buffer) {
        // 解码协议名
        String protocolName = MqttCodecUtil.decodeString(buffer);
        if (protocolName == null) {
            throw new MqttCodecException("Failed to decode protocol name");
        }
        
        // 解码协议级别
        if (!buffer.isReadable()) {
            throw new MqttCodecException("Missing protocol level");
        }
        byte protocolLevel = buffer.readByte();
        
        // 验证协议版本
        MqttVersion version = MqttVersion.fromProtocolNameAndLevel(protocolName, protocolLevel);
        if (version != this.protocolVersion) {
            throw new MqttCodecException("Protocol version mismatch");
        }
        
        // 解码连接标志
        if (!buffer.isReadable()) {
            throw new MqttCodecException("Missing connect flags");
        }
        MqttConnectFlags connectFlags = MqttConnectFlags.fromByte(buffer.readByte());
        
        // 解码保活时间
        if (buffer.readableBytes() < 2) {
            throw new MqttCodecException("Missing keep alive");
        }
        int keepAlive = buffer.readUnsignedShort();
        
        // 创建可变头部
        MqttConnectVariableHeader variableHeader = MqttConnectVariableHeader.create3x(
            version, connectFlags, keepAlive
        );
        
        // 解码负载
        MqttConnectPayload payload = decodeConnectPayload(buffer, connectFlags);
        
        return new MqttConnectPacket(fixedHeader, variableHeader, payload);
    }
    
    /**
     * 解码CONNECT包负载
     */
    private MqttConnectPayload decodeConnectPayload(ByteBuf buffer, MqttConnectFlags connectFlags) {
        // 客户端ID（必需）
        String clientId = MqttCodecUtil.decodeString(buffer);
        if (clientId == null) {
            throw new MqttCodecException("Failed to decode client ID");
        }
        
        String willTopic = null;
        byte[] willMessage = null;
        String username = null;
        byte[] password = null;
        
        // 遗嘱主题和消息
        if (connectFlags.willFlag()) {
            willTopic = MqttCodecUtil.decodeString(buffer);
            if (willTopic == null) {
                throw new MqttCodecException("Failed to decode will topic");
            }
            
            willMessage = MqttCodecUtil.decodeBinaryData(buffer);
            if (willMessage == null) {
                throw new MqttCodecException("Failed to decode will message");
            }
        }
        
        // 用户名
        if (connectFlags.usernameFlag()) {
            username = MqttCodecUtil.decodeString(buffer);
            if (username == null) {
                throw new MqttCodecException("Failed to decode username");
            }
        }
        
        // 密码
        if (connectFlags.passwordFlag()) {
            password = MqttCodecUtil.decodeBinaryData(buffer);
            if (password == null) {
                throw new MqttCodecException("Failed to decode password");
            }
        }
        
        return new MqttConnectPayload(clientId, willTopic, willMessage, username, password);
    }
    
    /**
     * 解码CONNACK包
     */
    private MqttConnackPacket decodeConnack(MqttFixedHeader fixedHeader, ByteBuf buffer) {
        if (buffer.readableBytes() < 2) {
            throw new MqttCodecException("CONNACK packet too short");
        }
        
        // 连接确认标志
        boolean sessionPresent = MqttConnackVariableHeader.parseAcknowledgeFlags(buffer.readByte());
        
        // 连接返回码
        MqttConnectReturnCode returnCode = MqttConnectReturnCode.fromValue(buffer.readByte());
        
        MqttConnackVariableHeader variableHeader = MqttConnackVariableHeader.create3x(sessionPresent, returnCode);
        
        return new MqttConnackPacket(fixedHeader, variableHeader);
    }
    
    /**
     * 解码PUBLISH包
     */
    private MqttPublishPacket decodePublish(MqttFixedHeader fixedHeader, ByteBuf buffer) {
        // 主题名
        String topicName = MqttCodecUtil.decodeString(buffer);
        if (topicName == null) {
            throw new MqttCodecException("Failed to decode topic name");
        }
        
        int packetId = 0;
        // QoS > 0时有包标识符
        if (fixedHeader.qos().getValue() > 0) {
            packetId = MqttCodecUtil.decodePacketId(buffer);
            if (packetId == -1) {
                throw new MqttCodecException("Failed to decode packet ID");
            }
        }
        
        // 负载（剩余的所有数据）
        byte[] payload = null;
        if (buffer.isReadable()) {
            payload = new byte[buffer.readableBytes()];
            buffer.readBytes(payload);
        }
        
        MqttPublishVariableHeader variableHeader = new MqttPublishVariableHeader(topicName, packetId, null);
        
        return new MqttPublishPacket(fixedHeader, variableHeader, payload);
    }
    
    /**
     * 解码PUBACK包
     */
    private MqttPubackPacket decodePuback(MqttFixedHeader fixedHeader, ByteBuf buffer) {
        int packetId = MqttCodecUtil.decodePacketId(buffer);
        if (packetId == -1) {
            throw new MqttCodecException("Failed to decode packet ID");
        }
        
        return MqttPubackPacket.create(packetId);
    }
    
    /**
     * 解码PUBREC包
     */
    private MqttPubrecPacket decodePubrec(MqttFixedHeader fixedHeader, ByteBuf buffer) {
        int packetId = MqttCodecUtil.decodePacketId(buffer);
        if (packetId == -1) {
            throw new MqttCodecException("Failed to decode packet ID");
        }
        
        return MqttPubrecPacket.create(packetId);
    }
    
    /**
     * 解码PUBREL包
     */
    private MqttPubrelPacket decodePubrel(MqttFixedHeader fixedHeader, ByteBuf buffer) {
        int packetId = MqttCodecUtil.decodePacketId(buffer);
        if (packetId == -1) {
            throw new MqttCodecException("Failed to decode packet ID");
        }
        
        return MqttPubrelPacket.create(packetId);
    }
    
    /**
     * 解码PUBCOMP包
     */
    private MqttPubcompPacket decodePubcomp(MqttFixedHeader fixedHeader, ByteBuf buffer) {
        int packetId = MqttCodecUtil.decodePacketId(buffer);
        if (packetId == -1) {
            throw new MqttCodecException("Failed to decode packet ID");
        }
        
        return MqttPubcompPacket.create(packetId);
    }
    
    /**
     * 解码SUBSCRIBE包
     */
    private MqttSubscribePacket decodeSubscribe(MqttFixedHeader fixedHeader, ByteBuf buffer) {
        // 包标识符
        int packetId = MqttCodecUtil.decodePacketId(buffer);
        if (packetId == -1) {
            throw new MqttCodecException("Failed to decode packet ID");
        }
        
        // 订阅列表
        List<MqttTopicSubscription> subscriptions = new ArrayList<>();
        
        while (buffer.isReadable()) {
            String topicFilter = MqttCodecUtil.decodeString(buffer);
            if (topicFilter == null) {
                throw new MqttCodecException("Failed to decode topic filter");
            }
            
            if (!buffer.isReadable()) {
                throw new MqttCodecException("Missing QoS for topic filter");
            }
            
            int qos = buffer.readUnsignedByte();
            if (qos > 2) {
                throw new MqttCodecException("Invalid QoS value: " + qos);
            }
            
            subscriptions.add(new MqttTopicSubscription(topicFilter, qos));
        }
        
        if (subscriptions.isEmpty()) {
            throw new MqttCodecException("SUBSCRIBE packet must contain at least one subscription");
        }
        
        MqttSubscribeVariableHeader variableHeader = MqttSubscribeVariableHeader.create3x(packetId);
        MqttSubscribePayload payload = new MqttSubscribePayload(subscriptions);
        
        return new MqttSubscribePacket(fixedHeader, variableHeader, payload);
    }
    
    /**
     * 解码SUBACK包
     */
    private MqttSubackPacket decodeSuback(MqttFixedHeader fixedHeader, ByteBuf buffer) {
        // 包标识符
        int packetId = MqttCodecUtil.decodePacketId(buffer);
        if (packetId == -1) {
            throw new MqttCodecException("Failed to decode packet ID");
        }
        
        // 返回码列表
        List<MqttSubackReturnCode> returnCodes = new ArrayList<>();
        
        while (buffer.isReadable()) {
            byte returnCodeByte = buffer.readByte();
            MqttSubackReturnCode returnCode = MqttSubackReturnCode.fromValue(returnCodeByte);
            returnCodes.add(returnCode);
        }
        
        if (returnCodes.isEmpty()) {
            throw new MqttCodecException("SUBACK packet must contain at least one return code");
        }
        
        MqttSubackVariableHeader variableHeader = MqttSubackVariableHeader.create3x(packetId);
        MqttSubackPayload payload = new MqttSubackPayload(returnCodes);
        
        return new MqttSubackPacket(fixedHeader, variableHeader, payload);
    }
    
    /**
     * 解码UNSUBSCRIBE包（简化实现）
     */
    private MqttPacket decodeUnsubscribe(MqttFixedHeader fixedHeader, ByteBuf buffer) {
        // 这里需要实现UNSUBSCRIBE包解码，暂时抛出异常
        throw new MqttCodecException("UNSUBSCRIBE packet decoding not implemented yet");
    }
    
    /**
     * 解码UNSUBACK包（简化实现）
     */
    private MqttPacket decodeUnsuback(MqttFixedHeader fixedHeader, ByteBuf buffer) {
        // 这里需要实现UNSUBACK包解码，暂时抛出异常
        throw new MqttCodecException("UNSUBACK packet decoding not implemented yet");
    }
    
    /**
     * 解码PINGREQ包
     */
    private MqttPacket decodePingreq(MqttFixedHeader fixedHeader, ByteBuf buffer) {
        // PINGREQ包没有可变头部和负载
        return new SimpleMqttPacket(fixedHeader);
    }
    
    /**
     * 解码PINGRESP包
     */
    private MqttPacket decodePingresp(MqttFixedHeader fixedHeader, ByteBuf buffer) {
        // PINGRESP包没有可变头部和负载
        return new SimpleMqttPacket(fixedHeader);
    }
    
    /**
     * 解码DISCONNECT包
     */
    private MqttPacket decodeDisconnect(MqttFixedHeader fixedHeader, ByteBuf buffer) {
        // DISCONNECT包没有可变头部和负载
        return new SimpleMqttPacket(fixedHeader);
    }
    
    /**
     * 简单的MQTT包实现（用于PINGREQ、PINGRESP、DISCONNECT等无负载包）
     */
    private static record SimpleMqttPacket(MqttFixedHeader fixedHeader) implements MqttPacket {
        @Override
        public MqttFixedHeader getFixedHeader() {
            return fixedHeader;
        }
    }
}