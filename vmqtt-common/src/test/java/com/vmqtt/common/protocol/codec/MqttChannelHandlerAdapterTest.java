/**
 * MQTT通道处理器适配器测试
 *
 * @author zhenglin
 * @date 2025/08/13
 */
package com.vmqtt.common.protocol.codec;

import com.vmqtt.common.protocol.MqttVersion;
import com.vmqtt.common.protocol.packet.MqttPacket;
import com.vmqtt.common.protocol.packet.MqttFixedHeader;
import com.vmqtt.common.protocol.packet.MqttPacketType;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.codec.DecoderException;
import io.netty.handler.codec.EncoderException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.AfterEach;

import java.nio.charset.StandardCharsets;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

/**
 * MQTT通道处理器适配器测试类
 */
@DisplayName("MQTT通道处理器适配器测试")
public class MqttChannelHandlerAdapterTest {
    
    private EmbeddedChannel channel;
    private MqttChannelHandlerAdapter adapter;
    
    @BeforeEach
    void setUp() {
        adapter = new MqttChannelHandlerAdapter();
        channel = new EmbeddedChannel(adapter);
        // 清理编解码器缓存
        MqttProtocolVersionManager.clearCodecCache();
    }
    
    @AfterEach
    void tearDown() {
        if (channel != null) {
            channel.close();
        }
    }
    
    @Test
    @DisplayName("通道激活和关闭")
    void testChannelLifecycle() {
        assertTrue(channel.isActive());
        
        // 获取协议信息
        String protocolInfo = adapter.getProtocolInfo(channel.pipeline().firstContext());
        assertNotNull(protocolInfo);
        assertTrue(protocolInfo.contains("协议版本"));
        
        // 关闭通道
        channel.close();
        assertFalse(channel.isActive());
    }
    
    @Test
    @DisplayName("协议版本自动协商")
    void testProtocolVersionNegotiation() {
        // 创建MQTT 3.1.1 CONNECT消息
        ByteBuf connectMessage = createMqttConnectMessage("MQTT", (byte) 4);
        
        try {
            // 写入数据触发解码
            channel.writeInbound(connectMessage);
            
            // 验证版本是否已协商
            assertTrue(MqttProtocolVersionManager.isVersionNegotiated(channel));
            assertEquals(MqttVersion.MQTT_3_1_1, MqttProtocolVersionManager.getProtocolVersion(channel));
            
        } finally {
            if (connectMessage.refCnt() > 0) {
                connectMessage.release();
            }
        }
    }
    
    @Test
    @DisplayName("数据不完整时等待更多数据")
    void testIncompleteDataWaiting() {
        // 发送不完整的数据
        ByteBuf incompleteData = Unpooled.buffer();
        incompleteData.writeByte(0x10); // CONNECT消息类型
        incompleteData.writeByte(0x10); // 剩余长度
        incompleteData.writeBytes("MQTT".getBytes(StandardCharsets.UTF_8)); // 部分数据
        
        try {
            // 写入不完整数据
            channel.writeInbound(incompleteData);
            
            // 版本应该尚未协商
            assertFalse(MqttProtocolVersionManager.isVersionNegotiated(channel));
            
            // 通道应该仍然活跃，等待更多数据
            assertTrue(channel.isActive());
            
        } finally {
            if (incompleteData.refCnt() > 0) {
                incompleteData.release();
            }
        }
    }
    
    @Test
    @DisplayName("处理解码异常")
    void testHandleDecoderException() {
        // 预先设置一个协议版本以便能够使用编解码器
        MqttProtocolVersionManager.setProtocolVersion(channel, MqttVersion.MQTT_3_1_1);
        
        // 创建畸形数据
        ByteBuf malformedData = Unpooled.buffer();
        malformedData.writeBytes(new byte[]{(byte) 0xFF, (byte) 0xFF, (byte) 0xFF}); // 畸形数据
        
        try {
            // 写入畸形数据应该触发异常
            boolean result = channel.writeInbound(malformedData);
            
            // 通道应该已关闭
            assertFalse(channel.isActive());
            
        } finally {
            if (malformedData.refCnt() > 0) {
                malformedData.release();
            }
        }
    }
    
    @Test
    @DisplayName("空数据处理")
    void testEmptyData() {
        ByteBuf emptyBuffer = Unpooled.EMPTY_BUFFER;
        
        // 写入空数据不应该引起异常
        assertDoesNotThrow(() -> channel.writeInbound(emptyBuffer));
        
        // 版本未协商
        assertFalse(MqttProtocolVersionManager.isVersionNegotiated(channel));
        
        // 通道仍然活跃
        assertTrue(channel.isActive());
    }
    
    @Test
    @DisplayName("获取协议信息")
    void testGetProtocolInfo() {
        // 未协商版本时的信息
        String info1 = adapter.getProtocolInfo(channel.pipeline().firstContext());
        assertTrue(info1.contains("未知"));
        assertTrue(info1.contains("已协商: false"));
        
        // 设置版本后的信息
        MqttProtocolVersionManager.setProtocolVersion(channel, MqttVersion.MQTT_3_1_1);
        
        String info2 = adapter.getProtocolInfo(channel.pipeline().firstContext());
        assertTrue(info2.contains("MQTT 3.1.1"));
        assertTrue(info2.contains("已协商: true"));
    }
    
    @Test
    @DisplayName("MQTT 3.1版本协商")
    void testMqtt31VersionNegotiation() {
        // 创建MQTT 3.1 CONNECT消息
        ByteBuf connectMessage = createMqttConnectMessage("MQIsdp", (byte) 3);
        
        try {
            channel.writeInbound(connectMessage);
            
            // 验证协商结果
            assertTrue(MqttProtocolVersionManager.isVersionNegotiated(channel));
            assertEquals(MqttVersion.MQTT_3_1, MqttProtocolVersionManager.getProtocolVersion(channel));
            
        } finally {
            if (connectMessage.refCnt() > 0) {
                connectMessage.release();
            }
        }
    }
    
    @Test
    @DisplayName("非CONNECT消息在版本协商前应该等待")
    void testNonConnectMessageBeforeNegotiation() {
        // 创建PUBLISH消息
        ByteBuf publishMessage = Unpooled.buffer();
        publishMessage.writeByte(0x30); // PUBLISH消息类型
        publishMessage.writeByte(0x05); // 剩余长度
        publishMessage.writeBytes("hello".getBytes(StandardCharsets.UTF_8));
        
        try {
            channel.writeInbound(publishMessage);
            
            // 版本未协商
            assertFalse(MqttProtocolVersionManager.isVersionNegotiated(channel));
            
            // 通道仍然活跃
            assertTrue(channel.isActive());
            
        } finally {
            if (publishMessage.refCnt() > 0) {
                publishMessage.release();
            }
        }
    }
    
    /**
     * 创建MQTT CONNECT消息
     */
    private ByteBuf createMqttConnectMessage(String protocolName, byte protocolLevel) {
        ByteBuf buffer = Unpooled.buffer();
        
        // 固定头部
        buffer.writeByte(0x10); // CONNECT消息类型
        
        // 计算剩余长度
        int protocolNameLength = protocolName.length();
        int variableHeaderLength = 2 + protocolNameLength + 1 + 1 + 2; // 协议名称 + 协议级别 + 连接标志 + Keep Alive
        int payloadLength = 2 + 8; // 客户端ID
        int remainingLength = variableHeaderLength + payloadLength;
        
        // 写入剩余长度
        writeRemainingLength(buffer, remainingLength);
        
        // 可变头部
        buffer.writeShort(protocolNameLength);
        buffer.writeBytes(protocolName.getBytes(StandardCharsets.UTF_8));
        buffer.writeByte(protocolLevel);
        buffer.writeByte(0x02); // 连接标志
        buffer.writeShort(60);  // Keep Alive
        
        // 载荷
        String clientId = "testclient";
        buffer.writeShort(clientId.length());
        buffer.writeBytes(clientId.getBytes(StandardCharsets.UTF_8));
        
        return buffer;
    }
    
    /**
     * 写入剩余长度
     */
    private void writeRemainingLength(ByteBuf buffer, int length) {
        do {
            byte digit = (byte) (length % 128);
            length /= 128;
            if (length > 0) {
                digit |= 0x80;
            }
            buffer.writeByte(digit);
        } while (length > 0);
    }
}