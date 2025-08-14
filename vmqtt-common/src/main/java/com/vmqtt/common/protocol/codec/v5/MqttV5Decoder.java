/**
 * MQTT 5.0协议解码器
 *
 * @author zhenglin
 * @date 2025/08/14
 */
package com.vmqtt.common.protocol.codec.v5;

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
import com.vmqtt.common.protocol.property.MqttProperty;
import io.netty.buffer.ByteBuf;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * MQTT 5.0协议解码器
 * 
 * 支持MQTT 5.0协议版本，包括属性系统和扩展功能
 */
public class MqttV5Decoder {
    
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
    public MqttV5Decoder(MqttVersion protocolVersion) {
        if (!protocolVersion.isMqtt5()) {
            throw new IllegalArgumentException("MQTT 5.0 decoder only supports MQTT 5.0 protocol version");
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
            case AUTH -> decodeAuth(fixedHeader, buffer);
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
        
        // 解码属性（MQTT 5.0新增）
        Map<MqttProperty, Object> properties = MqttV5PropertyCodec.decodeProperties(buffer);
        if (properties == null) {
            // 数据不足，重置读取位置
            buffer.resetReaderIndex();
            return null;
        }
        
        // 创建可变头部
        MqttConnectVariableHeader variableHeader = MqttConnectVariableHeader.create5(
            connectFlags, keepAlive, properties
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
        
        // 遗嘱属性（MQTT 5.0新增，如果有遗嘱消息）
        Map<MqttProperty, Object> willProperties = null;
        if (connectFlags.willFlag()) {
            willProperties = MqttV5PropertyCodec.decodeProperties(buffer);
            if (willProperties == null) {
                throw new MqttCodecException("Failed to decode will properties");
            }
        }
        
        // 遗嘱主题
        String willTopic = null;
        if (connectFlags.willFlag()) {
            willTopic = MqttCodecUtil.decodeString(buffer);
            if (willTopic == null) {
                throw new MqttCodecException("Failed to decode will topic");
            }
        }
        
        // 遗嘱消息
        byte[] willMessage = null;
        if (connectFlags.willFlag()) {
            willMessage = MqttCodecUtil.decodeBinaryData(buffer);
            if (willMessage == null) {
                throw new MqttCodecException("Failed to decode will message");
            }
        }
        
        // 用户名
        String username = null;
        if (connectFlags.usernameFlag()) {
            username = MqttCodecUtil.decodeString(buffer);
            if (username == null) {
                throw new MqttCodecException("Failed to decode username");
            }
        }
        
        // 密码
        byte[] password = null;
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
        
        // 会话存在标志
        boolean sessionPresent = (buffer.readByte() & 0x01) != 0;
        
        // 连接返回码（MQTT 5.0中称为原因码）
        byte reasonCode = buffer.readByte();
        
        // 解码属性（MQTT 5.0新增）
        Map<MqttProperty, Object> properties = MqttV5PropertyCodec.decodeProperties(buffer);
        if (properties == null) {
            // 数据不足，重置读取位置
            buffer.resetReaderIndex();
            return null;
        }
        
        MqttConnackVariableHeader variableHeader = MqttConnackVariableHeader.create3x(
            sessionPresent, MqttConnectReturnCode.fromValue(reasonCode)
        );
        
        return new MqttConnackPacket(fixedHeader, variableHeader);
    }
    
    /**
     * 解码PUBLISH包
     */
    private MqttPublishPacket decodePublish(MqttFixedHeader fixedHeader, ByteBuf buffer) {
        // 解码主题名
        String topicName = MqttCodecUtil.decodeString(buffer);
        if (topicName == null) {
            throw new MqttCodecException("Failed to decode topic name");
        }
        
        // 解码包标识符（QoS > 0时必需）
        Integer packetId = null;
        if (fixedHeader.qos().getValue() > 0) {
            if (buffer.readableBytes() < 2) {
                throw new MqttCodecException("Missing packet ID");
            }
            packetId = buffer.readUnsignedShort();
        }
        
        // 解码属性（MQTT 5.0新增）
        Map<MqttProperty, Object> properties = MqttV5PropertyCodec.decodeProperties(buffer);
        if (properties == null) {
            // 数据不足，重置读取位置
            buffer.resetReaderIndex();
            return null;
        }
        
        // 解码负载
        byte[] payload = new byte[buffer.readableBytes()];
        buffer.readBytes(payload);
        
        MqttPublishVariableHeader variableHeader = MqttPublishVariableHeader.createMqtt5(
            topicName, packetId != null ? packetId : 0, properties
        );
        
        return new MqttPublishPacket(fixedHeader, variableHeader, payload);
    }
    
    /**
     * 解码PUBACK包
     */
    private MqttPubackPacket decodePuback(MqttFixedHeader fixedHeader, ByteBuf buffer) {
        if (buffer.readableBytes() < 2) {
            throw new MqttCodecException("PUBACK packet too short");
        }
        
        int packetId = buffer.readUnsignedShort();
        
        // 原因码（MQTT 5.0可选）
        byte reasonCode = 0x00; // 默认成功
        if (buffer.isReadable()) {
            reasonCode = buffer.readByte();
        }
        
        // 解码属性（MQTT 5.0新增）
        Map<MqttProperty, Object> properties = null;
        if (buffer.isReadable()) {
            properties = MqttV5PropertyCodec.decodeProperties(buffer);
            if (properties == null) {
                // 数据不足，重置读取位置
                buffer.resetReaderIndex();
                return null;
            }
        }
        
        return new MqttPubackPacket(fixedHeader, packetId, properties);
    }
    
    /**
     * 解码PUBREC包
     */
    private MqttPubrecPacket decodePubrec(MqttFixedHeader fixedHeader, ByteBuf buffer) {
        if (buffer.readableBytes() < 2) {
            throw new MqttCodecException("PUBREC packet too short");
        }
        
        int packetId = buffer.readUnsignedShort();
        
        // 原因码（MQTT 5.0可选）
        byte reasonCode = 0x00; // 默认成功
        if (buffer.isReadable()) {
            reasonCode = buffer.readByte();
        }
        
        // 解码属性（MQTT 5.0新增）
        Map<MqttProperty, Object> properties = null;
        if (buffer.isReadable()) {
            properties = MqttV5PropertyCodec.decodeProperties(buffer);
            if (properties == null) {
                // 数据不足，重置读取位置
                buffer.resetReaderIndex();
                return null;
            }
        }
        
        return new MqttPubrecPacket(fixedHeader, packetId, properties);
    }
    
    /**
     * 解码PUBREL包
     */
    private MqttPubrelPacket decodePubrel(MqttFixedHeader fixedHeader, ByteBuf buffer) {
        if (buffer.readableBytes() < 2) {
            throw new MqttCodecException("PUBREL packet too short");
        }
        
        int packetId = buffer.readUnsignedShort();
        
        // 原因码（MQTT 5.0可选）
        byte reasonCode = 0x00; // 默认成功
        if (buffer.isReadable()) {
            reasonCode = buffer.readByte();
        }
        
        // 解码属性（MQTT 5.0新增）
        Map<MqttProperty, Object> properties = null;
        if (buffer.isReadable()) {
            properties = MqttV5PropertyCodec.decodeProperties(buffer);
            if (properties == null) {
                // 数据不足，重置读取位置
                buffer.resetReaderIndex();
                return null;
            }
        }
        
        return new MqttPubrelPacket(fixedHeader, packetId, properties);
    }
    
    /**
     * 解码PUBCOMP包
     */
    private MqttPubcompPacket decodePubcomp(MqttFixedHeader fixedHeader, ByteBuf buffer) {
        if (buffer.readableBytes() < 2) {
            throw new MqttCodecException("PUBCOMP packet too short");
        }
        
        int packetId = buffer.readUnsignedShort();
        
        // 原因码（MQTT 5.0可选）
        byte reasonCode = 0x00; // 默认成功
        if (buffer.isReadable()) {
            reasonCode = buffer.readByte();
        }
        
        // 解码属性（MQTT 5.0新增）
        Map<MqttProperty, Object> properties = null;
        if (buffer.isReadable()) {
            properties = MqttV5PropertyCodec.decodeProperties(buffer);
            if (properties == null) {
                // 数据不足，重置读取位置
                buffer.resetReaderIndex();
                return null;
            }
        }
        
        return new MqttPubcompPacket(fixedHeader, packetId, properties);
    }
    
    /**
     * 解码SUBSCRIBE包
     */
    private MqttSubscribePacket decodeSubscribe(MqttFixedHeader fixedHeader, ByteBuf buffer) {
        if (buffer.readableBytes() < 2) {
            throw new MqttCodecException("SUBSCRIBE packet too short");
        }
        
        int packetId = buffer.readUnsignedShort();
        
        // 解码属性（MQTT 5.0新增）
        Map<MqttProperty, Object> properties = MqttV5PropertyCodec.decodeProperties(buffer);
        if (properties == null) {
            // 数据不足，重置读取位置
            buffer.resetReaderIndex();
            return null;
        }
        
        // 解码主题过滤器
        List<MqttTopicSubscription> subscriptions = new ArrayList<>();
        while (buffer.isReadable()) {
            String topicFilter = MqttCodecUtil.decodeString(buffer);
            if (topicFilter == null) {
                throw new MqttCodecException("Failed to decode topic filter");
            }
            
            if (!buffer.isReadable()) {
                throw new MqttCodecException("Missing subscription options");
            }
            
            byte subscriptionOptions = buffer.readByte();
            MqttQos qos = MqttQos.fromValue((subscriptionOptions & 0x03));
            // 简化实现：MQTT 5.0扩展选项暂时忽略
            subscriptions.add(new MqttTopicSubscription(topicFilter, qos.getValue()));
        }
        
        MqttSubscribeVariableHeader variableHeader = MqttSubscribeVariableHeader.create5(
            packetId, properties
        );
        
        MqttSubscribePayload payload = new MqttSubscribePayload(subscriptions);
        
        return new MqttSubscribePacket(fixedHeader, variableHeader, payload);
    }
    
    /**
     * 解码SUBACK包
     */
    private MqttSubackPacket decodeSuback(MqttFixedHeader fixedHeader, ByteBuf buffer) {
        if (buffer.readableBytes() < 2) {
            throw new MqttCodecException("SUBACK packet too short");
        }
        
        int packetId = buffer.readUnsignedShort();
        
        // 解码属性（MQTT 5.0新增）
        Map<MqttProperty, Object> properties = MqttV5PropertyCodec.decodeProperties(buffer);
        if (properties == null) {
            // 数据不足，重置读取位置
            buffer.resetReaderIndex();
            return null;
        }
        
        // 解码原因码列表
        List<MqttSubackReturnCode> reasonCodes = new ArrayList<>();
        while (buffer.isReadable()) {
            byte reasonCodeByte = buffer.readByte();
            reasonCodes.add(MqttSubackReturnCode.fromValue(reasonCodeByte));
        }
        
        MqttSubackVariableHeader variableHeader = MqttSubackVariableHeader.create5(
            packetId, properties
        );
        
        MqttSubackPayload payload = new MqttSubackPayload(reasonCodes);
        
        return new MqttSubackPacket(fixedHeader, variableHeader, payload);
    }
    
    /**
     * 解码UNSUBSCRIBE包
     */
    private MqttPacket decodeUnsubscribe(MqttFixedHeader fixedHeader, ByteBuf buffer) {
        if (buffer.readableBytes() < 2) {
            throw new MqttCodecException("UNSUBSCRIBE packet too short");
        }
        
        int packetId = buffer.readUnsignedShort();
        
        // 解码属性（MQTT 5.0新增）
        Map<MqttProperty, Object> properties = MqttV5PropertyCodec.decodeProperties(buffer);
        if (properties == null) {
            // 数据不足，重置读取位置
            buffer.resetReaderIndex();
            return null;
        }
        
        // 解码主题过滤器列表
        List<String> topicFilters = new ArrayList<>();
        while (buffer.isReadable()) {
            String topicFilter = MqttCodecUtil.decodeString(buffer);
            if (topicFilter == null) {
                throw new MqttCodecException("Failed to decode topic filter");
            }
            topicFilters.add(topicFilter);
        }
        
        // 简化实现：暂时返回简单包
        return new SimpleMqttPacket(fixedHeader);
    }
    
    /**
     * 解码UNSUBACK包
     */
    private MqttPacket decodeUnsuback(MqttFixedHeader fixedHeader, ByteBuf buffer) {
        if (buffer.readableBytes() < 2) {
            throw new MqttCodecException("UNSUBACK packet too short");
        }
        
        int packetId = buffer.readUnsignedShort();
        
        // 解码属性（MQTT 5.0新增）
        Map<MqttProperty, Object> properties = MqttV5PropertyCodec.decodeProperties(buffer);
        if (properties == null) {
            // 数据不足，重置读取位置
            buffer.resetReaderIndex();
            return null;
        }
        
        // 解码原因码列表
        List<Byte> reasonCodes = new ArrayList<>();
        while (buffer.isReadable()) {
            reasonCodes.add(buffer.readByte());
        }
        
        // 简化实现：暂时返回简单包
        return new SimpleMqttPacket(fixedHeader);
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
        // 原因码（MQTT 5.0可选）
        byte reasonCode = 0x00; // 默认正常断开
        if (buffer.isReadable()) {
            reasonCode = buffer.readByte();
        }
        
        // 解码属性（MQTT 5.0新增）
        Map<MqttProperty, Object> properties = null;
        if (buffer.isReadable()) {
            properties = MqttV5PropertyCodec.decodeProperties(buffer);
            if (properties == null) {
                // 数据不足，重置读取位置
                buffer.resetReaderIndex();
                return null;
            }
        }
        
        // 简化实现：暂时返回简单包
        return new SimpleMqttPacket(fixedHeader);
    }
    
    /**
     * 解码AUTH包（MQTT 5.0新增）
     */
    private MqttPacket decodeAuth(MqttFixedHeader fixedHeader, ByteBuf buffer) {
        // 原因码（MQTT 5.0必需）
        if (!buffer.isReadable()) {
            throw new MqttCodecException("Missing AUTH reason code");
        }
        byte reasonCode = buffer.readByte();
        
        // 解码属性（MQTT 5.0新增）
        Map<MqttProperty, Object> properties = MqttV5PropertyCodec.decodeProperties(buffer);
        if (properties == null) {
            // 数据不足，重置读取位置
            buffer.resetReaderIndex();
            return null;
        }
        
        // 简化实现：暂时返回简单包
        return new SimpleMqttPacket(fixedHeader);
    }
    
    /**
     * 简单的MQTT包实现（用于PINGREQ、PINGRESP、DISCONNECT、AUTH等简单包）
     */
    private record SimpleMqttPacket(MqttFixedHeader fixedHeader) implements MqttPacket {
        @Override
        public MqttFixedHeader getFixedHeader() {
            return fixedHeader;
        }
    }
}