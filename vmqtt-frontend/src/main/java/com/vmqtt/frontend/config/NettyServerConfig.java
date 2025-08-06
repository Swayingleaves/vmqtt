/**
 * Netty服务器配置
 *
 * @author zhenglin
 * @date 2025/08/06
 */
package com.vmqtt.frontend.config;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

/**
 * Netty服务器配置属性
 */
@Data
@Component
@ConfigurationProperties(prefix = "vmqtt.server")
public class NettyServerConfig {
    
    /**
     * 服务器监听端口
     */
    private int port = 1883;
    
    /**
     * SSL/TLS端口
     */
    private int sslPort = 8883;
    
    /**
     * 是否启用SSL/TLS
     */
    private boolean sslEnabled = false;
    
    /**
     * SSL密钥存储文件路径
     */
    private String keyStorePath;
    
    /**
     * SSL密钥存储密码
     */
    private String keyStorePassword;
    
    /**
     * SSL证书密码
     */
    private String certificatePassword;
    
    /**
     * Boss线程组大小
     */
    private int bossThreads = 1;
    
    /**
     * Worker线程组大小
     */
    private int workerThreads = Runtime.getRuntime().availableProcessors() * 2;
    
    /**
     * 虚拟线程池大小
     */
    private int virtualThreadPoolSize = 1000000;
    
    /**
     * 是否启用虚拟线程
     */
    private boolean virtualThreadEnabled = true;
    
    /**
     * 最大帧长度
     */
    private int maxFrameLength = 256 * 1024 * 1024; // 256MB
    
    /**
     * SO_BACKLOG参数
     */
    private int soBacklog = 1024;
    
    /**
     * SO_KEEPALIVE参数
     */
    private boolean keepAlive = true;
    
    /**
     * TCP_NODELAY参数
     */
    private boolean tcpNoDelay = true;
    
    /**
     * 接收缓冲区大小
     */
    private int receiveBufferSize = 65536;
    
    /**
     * 发送缓冲区大小
     */
    private int sendBufferSize = 65536;
    
    /**
     * 客户端连接超时时间(秒)
     */
    private int connectTimeout = 60;
    
    /**
     * Keep-Alive超时时间(秒)
     */
    private int keepAliveTimeout = 300;
    
    /**
     * 读写空闲超时时间(秒)
     */
    private int idleTimeout = 600;
    
    /**
     * 最大连接数
     */
    private int maxConnections = 1000000;
    
    /**
     * 连接速率限制（每秒）
     */
    private int connectionRateLimit = 10000;
    
    /**
     * 是否启用连接速率限制
     */
    private boolean rateLimitEnabled = true;
    
    /**
     * 心跳检查间隔(秒)
     */
    private int heartbeatInterval = 30;
    
    /**
     * 是否启用Epoll（Linux平台）
     */
    private boolean useEpoll = true;
    
    /**
     * 是否启用零拷贝
     */
    private boolean zeroCopyEnabled = true;
    
    /**
     * 直接内存比例
     */
    private double directMemoryRatio = 0.8;
}