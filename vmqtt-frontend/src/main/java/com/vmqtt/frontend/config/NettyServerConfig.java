/**
 * Netty服务器配置
 *
 * @author zhenglin
 * @date 2025/08/08
 */
package com.vmqtt.frontend.config;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

/**
 * Netty服务器配置类
 * 包含MQTT服务器的所有配置参数
 */
@Data
@Component
@ConfigurationProperties(prefix = "mqtt")
public class NettyServerConfig {
    
    /**
     * 服务器配置
     */
    private Server server = new Server();
    
    /**
     * 连接配置
     */
    private Connection connection = new Connection();
    
    /**
     * 性能配置
     */
    private Performance performance = new Performance();
    
    /**
     * SSL/TLS配置
     */
    private Ssl ssl = new Ssl();
    
    /**
     * 服务器配置类
     */
    @Data
    public static class Server {
        /**
         * MQTT协议端口
         */
        private int port = 1883;
        
        /**
         * SSL端口
         */
        private int sslPort = 8883;
        
        /**
         * WebSocket端口
         */
        private int websocketPort = 8080;
        
        /**
         * 绑定地址
         */
        private String bindAddress = "0.0.0.0";
        
        /**
         * 是否启用SSL
         */
        private boolean sslEnabled = false;
        
        /**
         * 是否启用WebSocket
         */
        private boolean websocketEnabled = false;
    }
    
    /**
     * 连接配置类
     */
    @Data
    public static class Connection {
        /**
         * 最大连接数
         */
        private int maxConnections = 1000000;
        
        /**
         * 保活时间（秒）
         */
        private int keepAlive = 60;
        
        /**
         * 连接超时时间（秒）
         */
        private int timeout = 30;
        
        /**
         * 空闲检测时间（秒）
         */
        private int idleTimeout = 90;
        
        /**
         * 客户端ID长度限制
         */
        private int maxClientIdLength = 128;
        
        /**
         * 最大消息大小
         */
        private int maxMessageSize = 268435456; // 256MB
        
        /**
         * 连接速率限制（每秒）
         */
        private int connectionRateLimit = 10000;
        
        /**
         * 是否启用连接速率限制
         */
        private boolean rateLimitEnabled = true;
    }
    
    /**
     * 性能配置类
     */
    @Data
    public static class Performance {
        /**
         * IO线程数，0表示使用CPU核心数*2
         */
        private int ioThreads = 0;
        
        /**
         * 工作线程数，0表示使用虚拟线程
         */
        private int workerThreads = 0;
        
        /**
         * 缓冲区大小
         */
        private int bufferSize = 65536;
        
        /**
         * 是否启用TCP_NODELAY
         */
        private boolean tcpNoDelay = true;
        
        /**
         * 是否启用SO_KEEPALIVE
         */
        private boolean keepAlive = true;
        
        /**
         * 接收缓冲区大小
         */
        private int receiveBufferSize = 65536;
        
        /**
         * 发送缓冲区大小
         */
        private int sendBufferSize = 65536;
        
        /**
         * 连接队列长度
         */
        private int backlog = 1024;
        
        /**
         * 是否启用直接内存
         */
        private boolean useDirectBuffer = true;
        
        /**
         * 是否启用内存池
         */
        private boolean usePooledBuffer = true;
        
        /**
         * 是否启用零拷贝
         */
        private boolean zeroCopyEnabled = true;
        
        /**
         * 是否启用Epoll（Linux平台）
         */
        private boolean useEpoll = true;
        
        /**
         * 虚拟线程池大小
         */
        private int virtualThreadPoolSize = 1000000;
        
        /**
         * 是否启用虚拟线程
         */
        private boolean virtualThreadEnabled = true;
    }
    
    /**
     * SSL/TLS配置类
     */
    @Data
    public static class Ssl {
        /**
         * 证书文件路径
         */
        private String certificatePath;
        
        /**
         * 私钥文件路径
         */
        private String privateKeyPath;
        
        /**
         * 密钥库路径
         */
        private String keystorePath;
        
        /**
         * 密钥库密码
         */
        private String keystorePassword;
        
        /**
         * 信任库路径
         */
        private String truststorePath;
        
        /**
         * 信任库密码
         */
        private String truststorePassword;
        
        /**
         * 是否需要客户端认证
         */
        private boolean clientAuthRequired = false;
        
        /**
         * 支持的协议版本
         */
        private String[] protocols = {"TLSv1.2", "TLSv1.3"};
        
        /**
         * 支持的加密套件
         */
        private String[] cipherSuites = {};
    }
}