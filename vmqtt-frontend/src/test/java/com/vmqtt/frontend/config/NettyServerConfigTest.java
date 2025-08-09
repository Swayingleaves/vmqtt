/**
 * Netty服务器配置测试
 *
 * @author zhenglin
 * @date 2025/08/09
 */
package com.vmqtt.frontend.config;

import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.TestPropertySource;

import static org.junit.jupiter.api.Assertions.*;

/**
 * NettyServerConfig配置测试
 */
@SpringBootTest(classes = NettyServerConfig.class)
@TestPropertySource(properties = {
    "mqtt.server.port=1883",
    "mqtt.server.ssl-port=8883",
    "mqtt.connection.max-connections=1000000",
    "mqtt.connection.keep-alive=60",
    "mqtt.performance.virtual-thread-enabled=true"
})
class NettyServerConfigTest {
    
    @Test
    void testDefaultConfiguration() {
        NettyServerConfig config = new NettyServerConfig();
        
        // 测试默认值
        assertEquals(1883, config.getServer().getPort());
        assertEquals(8883, config.getServer().getSslPort());
        assertEquals(8080, config.getServer().getWebsocketPort());
        assertEquals("0.0.0.0", config.getServer().getBindAddress());
        assertFalse(config.getServer().isSslEnabled());
        assertFalse(config.getServer().isWebsocketEnabled());
        
        assertEquals(1000000, config.getConnection().getMaxConnections());
        assertEquals(60, config.getConnection().getKeepAlive());
        assertEquals(30, config.getConnection().getTimeout());
        assertEquals(90, config.getConnection().getIdleTimeout());
        assertEquals(128, config.getConnection().getMaxClientIdLength());
        assertEquals(268435456, config.getConnection().getMaxMessageSize());
        assertEquals(10000, config.getConnection().getConnectionRateLimit());
        assertTrue(config.getConnection().isRateLimitEnabled());
        
        assertEquals(0, config.getPerformance().getIoThreads());
        assertEquals(0, config.getPerformance().getWorkerThreads());
        assertEquals(65536, config.getPerformance().getBufferSize());
        assertTrue(config.getPerformance().isTcpNoDelay());
        assertTrue(config.getPerformance().isKeepAlive());
        assertEquals(65536, config.getPerformance().getReceiveBufferSize());
        assertEquals(65536, config.getPerformance().getSendBufferSize());
        assertEquals(1024, config.getPerformance().getBacklog());
        assertTrue(config.getPerformance().isUseDirectBuffer());
        assertTrue(config.getPerformance().isUsePooledBuffer());
        assertTrue(config.getPerformance().isZeroCopyEnabled());
        assertTrue(config.getPerformance().isUseEpoll());
        assertEquals(1000000, config.getPerformance().getVirtualThreadPoolSize());
        assertTrue(config.getPerformance().isVirtualThreadEnabled());
        
        assertNull(config.getSsl().getCertificatePath());
        assertNull(config.getSsl().getPrivateKeyPath());
        assertNull(config.getSsl().getKeystorePath());
        assertNull(config.getSsl().getKeystorePassword());
        assertNull(config.getSsl().getTruststorePath());
        assertNull(config.getSsl().getTruststorePassword());
        assertFalse(config.getSsl().isClientAuthRequired());
        assertArrayEquals(new String[]{"TLSv1.2", "TLSv1.3"}, config.getSsl().getProtocols());
        assertArrayEquals(new String[]{}, config.getSsl().getCipherSuites());
    }
    
    @Test
    void testServerConfiguration() {
        NettyServerConfig.Server server = new NettyServerConfig.Server();
        
        server.setPort(1883);
        server.setSslPort(8883);
        server.setWebsocketPort(8080);
        server.setBindAddress("127.0.0.1");
        server.setSslEnabled(true);
        server.setWebsocketEnabled(true);
        
        assertEquals(1883, server.getPort());
        assertEquals(8883, server.getSslPort());
        assertEquals(8080, server.getWebsocketPort());
        assertEquals("127.0.0.1", server.getBindAddress());
        assertTrue(server.isSslEnabled());
        assertTrue(server.isWebsocketEnabled());
    }
    
    @Test
    void testConnectionConfiguration() {
        NettyServerConfig.Connection connection = new NettyServerConfig.Connection();
        
        connection.setMaxConnections(500000);
        connection.setKeepAlive(120);
        connection.setTimeout(60);
        connection.setIdleTimeout(180);
        connection.setMaxClientIdLength(256);
        connection.setMaxMessageSize(1048576);
        connection.setConnectionRateLimit(5000);
        connection.setRateLimitEnabled(false);
        
        assertEquals(500000, connection.getMaxConnections());
        assertEquals(120, connection.getKeepAlive());
        assertEquals(60, connection.getTimeout());
        assertEquals(180, connection.getIdleTimeout());
        assertEquals(256, connection.getMaxClientIdLength());
        assertEquals(1048576, connection.getMaxMessageSize());
        assertEquals(5000, connection.getConnectionRateLimit());
        assertFalse(connection.isRateLimitEnabled());
    }
    
    @Test
    void testPerformanceConfiguration() {
        NettyServerConfig.Performance performance = new NettyServerConfig.Performance();
        
        performance.setIoThreads(8);
        performance.setWorkerThreads(16);
        performance.setBufferSize(32768);
        performance.setTcpNoDelay(false);
        performance.setKeepAlive(false);
        performance.setReceiveBufferSize(32768);
        performance.setSendBufferSize(32768);
        performance.setBacklog(512);
        performance.setUseDirectBuffer(false);
        performance.setUsePooledBuffer(false);
        performance.setZeroCopyEnabled(false);
        performance.setUseEpoll(false);
        performance.setVirtualThreadPoolSize(500000);
        performance.setVirtualThreadEnabled(false);
        
        assertEquals(8, performance.getIoThreads());
        assertEquals(16, performance.getWorkerThreads());
        assertEquals(32768, performance.getBufferSize());
        assertFalse(performance.isTcpNoDelay());
        assertFalse(performance.isKeepAlive());
        assertEquals(32768, performance.getReceiveBufferSize());
        assertEquals(32768, performance.getSendBufferSize());
        assertEquals(512, performance.getBacklog());
        assertFalse(performance.isUseDirectBuffer());
        assertFalse(performance.isUsePooledBuffer());
        assertFalse(performance.isZeroCopyEnabled());
        assertFalse(performance.isUseEpoll());
        assertEquals(500000, performance.getVirtualThreadPoolSize());
        assertFalse(performance.isVirtualThreadEnabled());
    }
    
    @Test
    void testSslConfiguration() {
        NettyServerConfig.Ssl ssl = new NettyServerConfig.Ssl();
        
        ssl.setCertificatePath("/path/to/cert.pem");
        ssl.setPrivateKeyPath("/path/to/key.pem");
        ssl.setKeystorePath("/path/to/keystore.jks");
        ssl.setKeystorePassword("password");
        ssl.setTruststorePath("/path/to/truststore.jks");
        ssl.setTruststorePassword("trustpassword");
        ssl.setClientAuthRequired(true);
        ssl.setProtocols(new String[]{"TLSv1.3"});
        ssl.setCipherSuites(new String[]{"TLS_AES_256_GCM_SHA384"});
        
        assertEquals("/path/to/cert.pem", ssl.getCertificatePath());
        assertEquals("/path/to/key.pem", ssl.getPrivateKeyPath());
        assertEquals("/path/to/keystore.jks", ssl.getKeystorePath());
        assertEquals("password", ssl.getKeystorePassword());
        assertEquals("/path/to/truststore.jks", ssl.getTruststorePath());
        assertEquals("trustpassword", ssl.getTruststorePassword());
        assertTrue(ssl.isClientAuthRequired());
        assertArrayEquals(new String[]{"TLSv1.3"}, ssl.getProtocols());
        assertArrayEquals(new String[]{"TLS_AES_256_GCM_SHA384"}, ssl.getCipherSuites());
    }
}