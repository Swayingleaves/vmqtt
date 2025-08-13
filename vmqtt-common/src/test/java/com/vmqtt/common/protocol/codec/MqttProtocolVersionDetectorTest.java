/**
 * MQTT协议版本检测器测试
 *
 * @author zhenglin
 * @date 2025/08/13
 */
package com.vmqtt.common.protocol.codec;

import com.vmqtt.common.protocol.MqttVersion;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.AfterEach;

import java.nio.charset.StandardCharsets;

import static org.junit.jupiter.api.Assertions.*;

/**
 * MQTT协议版本检测器测试类
 */
@DisplayName("MQTT协议版本检测器测试")
public class MqttProtocolVersionDetectorTest {
    
    private ByteBuf buffer;
    
    @BeforeEach
    void setUp() {
        buffer = Unpooled.buffer();
    }
    
    @AfterEach
    void tearDown() {
        if (buffer != null) {
            buffer.release();
        }
    }
    
    @Test
    @DisplayName("检测MQTT 3.1版本")
    void testDetectMqtt31Version() {
        // 构造MQTT 3.1 CONNECT消息
        createMqttConnectMessage("MQIsdp", (byte) 3);
        
        MqttVersion version = MqttProtocolVersionDetector.detectVersion(buffer);
        
        assertEquals(MqttVersion.MQTT_3_1, version);
    }
    
    @Test
    @DisplayName("检测MQTT 3.1.1版本")
    void testDetectMqtt311Version() {
        // 构造MQTT 3.1.1 CONNECT消息
        createMqttConnectMessage("MQTT", (byte) 4);
        
        MqttVersion version = MqttProtocolVersionDetector.detectVersion(buffer);
        
        assertEquals(MqttVersion.MQTT_3_1_1, version);
    }
    
    @Test
    @DisplayName("检测MQTT 5.0版本")
    void testDetectMqtt5Version() {
        // 构造MQTT 5.0 CONNECT消息
        createMqttConnectMessage("MQTT", (byte) 5);
        
        MqttVersion version = MqttProtocolVersionDetector.detectVersion(buffer);
        
        assertEquals(MqttVersion.MQTT_5_0, version);
    }
    
    @Test
    @DisplayName("数据不完整时返回null")
    void testIncompleteData() {
        // 只写入部分数据
        buffer.writeByte(0x10); // CONNECT消息类型
        buffer.writeByte(0x02); // 剩余长度
        
        MqttVersion version = MqttProtocolVersionDetector.detectVersion(buffer);
        
        assertNull(version);
    }
    
    @Test
    @DisplayName("空缓冲区返回null")
    void testEmptyBuffer() {
        MqttVersion version = MqttProtocolVersionDetector.detectVersion(buffer);
        
        assertNull(version);
    }
    
    @Test
    @DisplayName("null缓冲区返回null")
    void testNullBuffer() {
        MqttVersion version = MqttProtocolVersionDetector.detectVersion(null);
        
        assertNull(version);
    }
    
    @Test
    @DisplayName("非CONNECT消息返回null")
    void testNonConnectMessage() {
        // 构造PUBLISH消息 (0x30)
        buffer.writeByte(0x30);
        buffer.writeByte(0x0A);
        buffer.writeBytes("test data".getBytes(StandardCharsets.UTF_8));
        
        MqttVersion version = MqttProtocolVersionDetector.detectVersion(buffer);
        
        assertNull(version);
    }
    
    @Test
    @DisplayName("检查CONNECT消息")
    void testIsConnectMessage() {
        // CONNECT消息
        buffer.writeByte(0x10);
        assertTrue(MqttProtocolVersionDetector.isConnectMessage(buffer));
        
        // 清空并写入PUBLISH消息
        buffer.clear();
        buffer.writeByte(0x30);
        assertFalse(MqttProtocolVersionDetector.isConnectMessage(buffer));
    }
    
    @Test
    @DisplayName("获取默认版本")
    void testGetDefaultVersion() {
        MqttVersion defaultVersion = MqttProtocolVersionDetector.getDefaultVersion();
        
        assertEquals(MqttVersion.MQTT_3_1_1, defaultVersion);
    }
    
    @Test
    @DisplayName("检查版本支持")
    void testIsVersionSupported() {
        assertTrue(MqttProtocolVersionDetector.isVersionSupported(MqttVersion.MQTT_3_1));
        assertTrue(MqttProtocolVersionDetector.isVersionSupported(MqttVersion.MQTT_3_1_1));
        assertFalse(MqttProtocolVersionDetector.isVersionSupported(MqttVersion.MQTT_5_0));
        assertFalse(MqttProtocolVersionDetector.isVersionSupported(null));
    }
    
    @Test
    @DisplayName("未知协议名称回退到默认版本")
    void testUnknownProtocolName() {
        createMqttConnectMessage("UnknownProtocol", (byte) 4);
        
        MqttVersion version = MqttProtocolVersionDetector.detectVersion(buffer);
        
        assertEquals(MqttVersion.MQTT_3_1_1, version);
    }
    
    @Test
    @DisplayName("不支持的协议级别回退到默认版本")
    void testUnsupportedProtocolLevel() {
        createMqttConnectMessage("MQTT", (byte) 99);
        
        MqttVersion version = MqttProtocolVersionDetector.detectVersion(buffer);
        
        assertEquals(MqttVersion.MQTT_3_1_1, version);
    }
    
    /**
     * 创建MQTT CONNECT消息
     */
    private void createMqttConnectMessage(String protocolName, byte protocolLevel) {
        buffer.clear();
        
        // 固定头部
        buffer.writeByte(0x10); // CONNECT消息类型
        
        // 计算可变头部和载荷长度
        int protocolNameLength = protocolName.length();
        int variableHeaderLength = 2 + protocolNameLength + 1 + 1 + 2; // 协议名称长度 + 协议名称 + 协议级别 + 连接标志 + Keep Alive
        int payloadLength = 2 + 8; // 客户端ID长度 + 客户端ID
        int remainingLength = variableHeaderLength + payloadLength;
        
        // 写入剩余长度
        writeRemainingLength(remainingLength);
        
        // 可变头部
        buffer.writeShort(protocolNameLength);
        buffer.writeBytes(protocolName.getBytes(StandardCharsets.UTF_8));
        buffer.writeByte(protocolLevel);
        buffer.writeByte(0x02); // 连接标志 (Clean Session)
        buffer.writeShort(60);  // Keep Alive
        
        // 载荷 (客户端ID)
        String clientId = "testclient";
        buffer.writeShort(clientId.length());
        buffer.writeBytes(clientId.getBytes(StandardCharsets.UTF_8));
    }
    
    /**
     * 写入剩余长度
     */
    private void writeRemainingLength(int length) {
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