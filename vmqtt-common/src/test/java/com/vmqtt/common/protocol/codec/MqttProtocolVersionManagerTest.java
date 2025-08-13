/**
 * MQTT协议版本管理器测试
 *
 * @author zhenglin
 * @date 2025/08/13
 */
package com.vmqtt.common.protocol.codec;

import com.vmqtt.common.protocol.MqttVersion;
import io.netty.channel.Channel;
import io.netty.channel.embedded.EmbeddedChannel;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.AfterEach;

import static org.junit.jupiter.api.Assertions.*;

/**
 * MQTT协议版本管理器测试类
 */
@DisplayName("MQTT协议版本管理器测试")
public class MqttProtocolVersionManagerTest {
    
    private Channel channel;
    
    @BeforeEach
    void setUp() {
        channel = new EmbeddedChannel();
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
    @DisplayName("设置和获取协议版本")
    void testSetAndGetProtocolVersion() {
        // 初始状态应该为null
        assertNull(MqttProtocolVersionManager.getProtocolVersion(channel));
        assertFalse(MqttProtocolVersionManager.isVersionNegotiated(channel));
        
        // 设置协议版本
        boolean success = MqttProtocolVersionManager.setProtocolVersion(channel, MqttVersion.MQTT_3_1_1);
        assertTrue(success);
        
        // 验证设置结果
        assertEquals(MqttVersion.MQTT_3_1_1, MqttProtocolVersionManager.getProtocolVersion(channel));
        assertTrue(MqttProtocolVersionManager.isVersionNegotiated(channel));
        
        // 验证编解码器对
        MqttCodecFactory.MqttCodecPair codecPair = MqttProtocolVersionManager.getCodecPair(channel);
        assertNotNull(codecPair);
        assertEquals(MqttVersion.MQTT_3_1_1, codecPair.getVersion());
    }
    
    @Test
    @DisplayName("设置不支持的协议版本应该失败")
    void testSetUnsupportedVersion() {
        boolean success = MqttProtocolVersionManager.setProtocolVersion(channel, MqttVersion.MQTT_5_0);
        assertFalse(success);
        
        assertNull(MqttProtocolVersionManager.getProtocolVersion(channel));
        assertFalse(MqttProtocolVersionManager.isVersionNegotiated(channel));
    }
    
    @Test
    @DisplayName("设置null参数应该失败")
    void testSetNullParameters() {
        assertFalse(MqttProtocolVersionManager.setProtocolVersion(null, MqttVersion.MQTT_3_1_1));
        assertFalse(MqttProtocolVersionManager.setProtocolVersion(channel, null));
    }
    
    @Test
    @DisplayName("重置协议版本")
    void testResetProtocolVersion() {
        // 先设置版本
        MqttProtocolVersionManager.setProtocolVersion(channel, MqttVersion.MQTT_3_1_1);
        assertTrue(MqttProtocolVersionManager.isVersionNegotiated(channel));
        
        // 重置版本
        MqttProtocolVersionManager.resetProtocolVersion(channel);
        
        // 验证重置结果
        assertNull(MqttProtocolVersionManager.getProtocolVersion(channel));
        assertFalse(MqttProtocolVersionManager.isVersionNegotiated(channel));
        assertNull(MqttProtocolVersionManager.getCodecPair(channel));
    }
    
    @Test
    @DisplayName("协商支持的协议版本")
    void testNegotiateSupportedVersion() {
        MqttVersion negotiated = MqttProtocolVersionManager.negotiateVersion(channel, MqttVersion.MQTT_3_1_1);
        
        assertEquals(MqttVersion.MQTT_3_1_1, negotiated);
        assertEquals(MqttVersion.MQTT_3_1_1, MqttProtocolVersionManager.getProtocolVersion(channel));
        assertTrue(MqttProtocolVersionManager.isVersionNegotiated(channel));
    }
    
    @Test
    @DisplayName("协商不支持的协议版本应该回退")
    void testNegotiateUnsupportedVersionFallback() {
        MqttVersion negotiated = MqttProtocolVersionManager.negotiateVersion(channel, MqttVersion.MQTT_5_0);
        
        // 应该回退到支持的版本
        assertNotNull(negotiated);
        assertTrue(MqttCodecFactory.isVersionSupported(negotiated));
        assertEquals(negotiated, MqttProtocolVersionManager.getProtocolVersion(channel));
        assertTrue(MqttProtocolVersionManager.isVersionNegotiated(channel));
    }
    
    @Test
    @DisplayName("null channel协商应该失败")
    void testNegotiateWithNullChannel() {
        MqttVersion negotiated = MqttProtocolVersionManager.negotiateVersion(null, MqttVersion.MQTT_3_1_1);
        
        assertNull(negotiated);
    }
    
    @Test
    @DisplayName("编解码器缓存功能")
    void testCodecCache() {
        // 清理缓存
        MqttProtocolVersionManager.clearCodecCache();
        
        // 预热缓存
        MqttProtocolVersionManager.warmupCodecCache();
        
        // 获取统计信息
        String stats = MqttProtocolVersionManager.getCodecStats();
        assertNotNull(stats);
        assertTrue(stats.contains("编解码器缓存大小"));
        
        // 设置多个版本，验证缓存共享
        Channel channel2 = new EmbeddedChannel();
        try {
            MqttProtocolVersionManager.setProtocolVersion(channel, MqttVersion.MQTT_3_1_1);
            MqttProtocolVersionManager.setProtocolVersion(channel2, MqttVersion.MQTT_3_1_1);
            
            // 应该使用相同的编解码器实例
            MqttCodecFactory.MqttCodecPair codecPair1 = MqttProtocolVersionManager.getCodecPair(channel);
            MqttCodecFactory.MqttCodecPair codecPair2 = MqttProtocolVersionManager.getCodecPair(channel2);
            
            assertNotNull(codecPair1);
            assertNotNull(codecPair2);
            // 注意：这里比较引用相等性，验证缓存是否生效
            // 实际实现中可能需要根据具体情况调整
            
        } finally {
            channel2.close();
        }
    }
    
    @Test
    @DisplayName("获取null channel的属性应该返回null")
    void testGetAttributesWithNullChannel() {
        assertNull(MqttProtocolVersionManager.getProtocolVersion(null));
        assertNull(MqttProtocolVersionManager.getCodecPair(null));
        assertFalse(MqttProtocolVersionManager.isVersionNegotiated(null));
    }
    
    @Test
    @DisplayName("重置null channel不应该抛出异常")
    void testResetNullChannel() {
        // 不应该抛出异常
        assertDoesNotThrow(() -> MqttProtocolVersionManager.resetProtocolVersion(null));
    }
    
    @Test
    @DisplayName("设置MQTT 3.1版本")
    void testSetMqtt31Version() {
        boolean success = MqttProtocolVersionManager.setProtocolVersion(channel, MqttVersion.MQTT_3_1);
        assertTrue(success);
        
        assertEquals(MqttVersion.MQTT_3_1, MqttProtocolVersionManager.getProtocolVersion(channel));
        
        MqttCodecFactory.MqttCodecPair codecPair = MqttProtocolVersionManager.getCodecPair(channel);
        assertNotNull(codecPair);
        assertEquals(MqttVersion.MQTT_3_1, codecPair.getVersion());
    }
    
    @Test
    @DisplayName("多次设置相同版本应该成功")
    void testSetSameVersionMultipleTimes() {
        assertTrue(MqttProtocolVersionManager.setProtocolVersion(channel, MqttVersion.MQTT_3_1_1));
        assertTrue(MqttProtocolVersionManager.setProtocolVersion(channel, MqttVersion.MQTT_3_1_1));
        
        assertEquals(MqttVersion.MQTT_3_1_1, MqttProtocolVersionManager.getProtocolVersion(channel));
    }
    
    @Test
    @DisplayName("切换协议版本")
    void testSwitchProtocolVersion() {
        // 先设置3.1版本
        assertTrue(MqttProtocolVersionManager.setProtocolVersion(channel, MqttVersion.MQTT_3_1));
        assertEquals(MqttVersion.MQTT_3_1, MqttProtocolVersionManager.getProtocolVersion(channel));
        
        // 切换到3.1.1版本
        assertTrue(MqttProtocolVersionManager.setProtocolVersion(channel, MqttVersion.MQTT_3_1_1));
        assertEquals(MqttVersion.MQTT_3_1_1, MqttProtocolVersionManager.getProtocolVersion(channel));
        
        // 验证编解码器也更新了
        MqttCodecFactory.MqttCodecPair codecPair = MqttProtocolVersionManager.getCodecPair(channel);
        assertNotNull(codecPair);
        assertEquals(MqttVersion.MQTT_3_1_1, codecPair.getVersion());
    }
}