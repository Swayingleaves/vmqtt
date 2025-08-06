/**
 * MQTT协议实现测试
 *
 * @author zhenglin
 * @date 2025/08/06
 */
package com.vmqtt.common.protocol;

import com.vmqtt.common.protocol.codec.*;
import com.vmqtt.common.protocol.packet.*;
import com.vmqtt.common.protocol.packet.connect.*;
import com.vmqtt.common.protocol.packet.connack.*;
import com.vmqtt.common.protocol.packet.publish.*;
import com.vmqtt.common.protocol.version.MqttVersionNegotiator;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.DisplayName;

import static org.junit.jupiter.api.Assertions.*;

/**
 * MQTT协议实现测试类
 */
class MqttProtocolTest {
    
    @Test
    @DisplayName("测试MQTT版本枚举")
    void testMqttVersion() {
        // 测试协议版本基本功能
        assertEquals(MqttVersion.MQTT_3_1, MqttVersion.fromLevel((byte) 0x03));
        assertEquals(MqttVersion.MQTT_3_1_1, MqttVersion.fromLevel((byte) 0x04));
        assertEquals(MqttVersion.MQTT_5_0, MqttVersion.fromLevel((byte) 0x05));
        
        assertTrue(MqttVersion.MQTT_5_0.isMqtt5());
        assertFalse(MqttVersion.MQTT_3_1_1.isMqtt5());
        
        assertTrue(MqttVersion.MQTT_5_0.supportsProperties());
        assertFalse(MqttVersion.MQTT_3_1_1.supportsProperties());
    }
    
    @Test
    @DisplayName("测试MQTT包类型")
    void testMqttPacketType() {
        // 测试包类型基本功能
        assertEquals(MqttPacketType.CONNECT, MqttPacketType.fromValue(1));
        assertEquals(MqttPacketType.CONNACK, MqttPacketType.fromValue(2));
        assertEquals(MqttPacketType.PUBLISH, MqttPacketType.fromValue(3));
        
        assertTrue(MqttPacketType.CONNECT.isClientToServer());
        assertTrue(MqttPacketType.CONNACK.isServerToClient());
        assertTrue(MqttPacketType.PUBLISH.isBidirectional());
        
        assertFalse(MqttPacketType.CONNECT.hasPacketId());
        assertTrue(MqttPacketType.SUBSCRIBE.hasPacketId());
        
        assertTrue(MqttPacketType.AUTH.isMqtt5Only());
        assertFalse(MqttPacketType.CONNECT.isMqtt5Only());
    }
    
    @Test
    @DisplayName("测试MQTT QoS")
    void testMqttQos() {
        assertEquals(MqttQos.AT_MOST_ONCE, MqttQos.fromValue(0));
        assertEquals(MqttQos.AT_LEAST_ONCE, MqttQos.fromValue(1));
        assertEquals(MqttQos.EXACTLY_ONCE, MqttQos.fromValue(2));
        
        assertEquals(0, MqttQos.AT_MOST_ONCE.getValue());
        assertEquals(1, MqttQos.AT_LEAST_ONCE.getValue());
        assertEquals(2, MqttQos.EXACTLY_ONCE.getValue());
    }
    
    @Test
    @DisplayName("测试MQTT固定头部")
    void testMqttFixedHeader() {
        // 测试PUBLISH包固定头部
        MqttFixedHeader fixedHeader = MqttFixedHeader.createPublishHeader(
            false, MqttQos.AT_LEAST_ONCE, true, 100);
        
        assertEquals(MqttPacketType.PUBLISH, fixedHeader.packetType());
        assertFalse(fixedHeader.dupFlag());
        assertEquals(MqttQos.AT_LEAST_ONCE, fixedHeader.qos());
        assertTrue(fixedHeader.retainFlag());
        assertEquals(100, fixedHeader.remainingLength());
        
        // 测试固定头部第一个字节
        byte firstByte = fixedHeader.getFirstByte();
        int packetType = (firstByte & 0xF0) >> 4;
        assertEquals(3, packetType); // PUBLISH = 3
        
        boolean dup = (firstByte & 0x08) != 0;
        assertFalse(dup);
        
        int qos = (firstByte & 0x06) >> 1;
        assertEquals(1, qos); // AT_LEAST_ONCE = 1
        
        boolean retain = (firstByte & 0x01) != 0;
        assertTrue(retain);
    }
    
    @Test
    @DisplayName("测试CONNECT包创建")
    void testMqttConnectPacket() {
        // 创建CONNECT包
        MqttConnectPacket connectPacket = MqttConnectPacket.builder("test-client-123")
            .mqttVersion(MqttVersion.MQTT_3_1_1)
            .cleanSession(true)
            .keepAlive(60)
            .credentials("username", "password".getBytes())
            .will("last-will-topic", "offline".getBytes(), false, 0)
            .build();
        
        assertEquals(MqttPacketType.CONNECT, connectPacket.getPacketType());
        assertEquals("test-client-123", connectPacket.getClientId());
        assertEquals(MqttVersion.MQTT_3_1_1, connectPacket.getMqttVersion());
        assertEquals(60, connectPacket.getKeepAlive());
        assertTrue(connectPacket.isCleanSession());
        assertTrue(connectPacket.hasWill());
        assertTrue(connectPacket.hasUsername());
        assertTrue(connectPacket.hasPassword());
    }
    
    @Test
    @DisplayName("测试CONNACK包创建")
    void testMqttConnackPacket() {
        // 创建成功的CONNACK包
        MqttConnackPacket successPacket = MqttConnackPacket.createSuccess(false);
        assertEquals(MqttPacketType.CONNACK, successPacket.getPacketType());
        assertTrue(successPacket.isConnectionAccepted());
        assertFalse(successPacket.isSessionPresent());
        assertEquals(MqttConnectReturnCode.CONNECTION_ACCEPTED, successPacket.getReturnCode());
        
        // 创建失败的CONNACK包
        MqttConnackPacket failurePacket = MqttConnackPacket.createFailure(
            MqttConnectReturnCode.CONNECTION_REFUSED_BAD_USER_NAME_OR_PASSWORD);
        assertEquals(MqttPacketType.CONNACK, failurePacket.getPacketType());
        assertFalse(failurePacket.isConnectionAccepted());
        assertFalse(failurePacket.isSessionPresent());
        assertEquals(MqttConnectReturnCode.CONNECTION_REFUSED_BAD_USER_NAME_OR_PASSWORD, 
                   failurePacket.getReturnCode());
    }
    
    @Test
    @DisplayName("测试PUBLISH包创建")
    void testMqttPublishPacket() {
        String testMessage = "Hello, MQTT!";
        
        // 创建QoS 0的PUBLISH包
        MqttPublishPacket qos0Packet = MqttPublishPacket.createQos0(
            "test/topic", testMessage.getBytes(), false);
        
        assertEquals(MqttPacketType.PUBLISH, qos0Packet.getPacketType());
        assertEquals("test/topic", qos0Packet.getTopicName());
        assertEquals(MqttQos.AT_MOST_ONCE, qos0Packet.getQos());
        assertFalse(qos0Packet.isRetain());
        assertEquals(0, qos0Packet.getPacketId());
        assertEquals(testMessage, qos0Packet.getPayloadAsString());
        
        // 创建QoS 1的PUBLISH包
        MqttPublishPacket qos1Packet = MqttPublishPacket.createQos1(
            "test/topic", testMessage.getBytes(), 12345, true);
        
        assertEquals(MqttQos.AT_LEAST_ONCE, qos1Packet.getQos());
        assertTrue(qos1Packet.isRetain());
        assertEquals(12345, qos1Packet.getPacketId());
    }
    
    @Test
    @DisplayName("测试协议版本协商")
    void testVersionNegotiation() {
        // 创建支持所有版本的协商器
        MqttVersionNegotiator negotiator = MqttVersionNegotiator.createFullSupport();
        
        // 测试成功协商
        MqttVersionNegotiator.NegotiationResult result = negotiator.negotiate(MqttVersion.MQTT_3_1_1);
        assertTrue(result.isSuccess());
        assertEquals(MqttVersion.MQTT_3_1_1, result.getNegotiatedVersion());
        assertEquals(MqttConnectReturnCode.CONNECTION_ACCEPTED, result.getReturnCode());
        
        // 创建仅支持MQTT 3.x的协商器
        MqttVersionNegotiator mqtt3Negotiator = MqttVersionNegotiator.createMqtt3xOnly();
        assertTrue(mqtt3Negotiator.isSupported(MqttVersion.MQTT_3_1_1));
        assertFalse(mqtt3Negotiator.isSupported(MqttVersion.MQTT_5_0));
    }
    
    @Test
    @DisplayName("测试编解码器工厂")
    void testCodecFactory() {
        // 测试支持的版本
        assertTrue(MqttCodecFactory.isVersionSupported(MqttVersion.MQTT_3_1));
        assertTrue(MqttCodecFactory.isVersionSupported(MqttVersion.MQTT_3_1_1));
        assertFalse(MqttCodecFactory.isVersionSupported(MqttVersion.MQTT_5_0));
        
        // 创建编解码器对
        MqttCodecFactory.MqttCodecPair codecPair = MqttCodecFactory.createCodecPair(MqttVersion.MQTT_3_1_1);
        assertEquals(MqttVersion.MQTT_3_1_1, codecPair.getVersion());
        assertNotNull(codecPair.decoder());
        assertNotNull(codecPair.encoder());
    }
    
    @Test
    @DisplayName("测试编解码工具类")
    void testCodecUtil() {
        ByteBuf buffer = Unpooled.buffer();
        
        try {
            // 测试剩余长度编码/解码
            MqttCodecUtil.encodeRemainingLength(buffer, 127);
            assertEquals(127, MqttCodecUtil.decodeRemainingLength(buffer));
            
            buffer.clear();
            MqttCodecUtil.encodeRemainingLength(buffer, 16383);
            assertEquals(16383, MqttCodecUtil.decodeRemainingLength(buffer));
            
            // 测试字符串编码/解码
            buffer.clear();
            String testString = "Hello, MQTT!";
            MqttCodecUtil.encodeString(buffer, testString);
            String decodedString = MqttCodecUtil.decodeString(buffer);
            assertEquals(testString, decodedString);
            
            // 测试二进制数据编码/解码
            buffer.clear();
            byte[] testData = "Binary data".getBytes();
            MqttCodecUtil.encodeBinaryData(buffer, testData);
            byte[] decodedData = MqttCodecUtil.decodeBinaryData(buffer);
            assertArrayEquals(testData, decodedData);
            
        } finally {
            buffer.release();
        }
    }
}