/**
 * Netty MQTT服务器
 *
 * @author zhenglin
 * @date 2025/08/06
 */
package com.vmqtt.frontend.server;

import com.vmqtt.frontend.config.NettyServerConfig;
import com.vmqtt.frontend.server.initializer.MqttChannelInitializer;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.*;
import io.netty.channel.epoll.EpollEventLoopGroup;
import io.netty.channel.epoll.EpollServerSocketChannel;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.util.concurrent.DefaultThreadFactory;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * 高性能Netty MQTT服务器实现
 * 支持百万连接和虚拟线程
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class NettyMqttServer {
    
    private final NettyServerConfig config;
    private final MqttChannelInitializer channelInitializer;
    
    private EventLoopGroup bossGroup;
    private EventLoopGroup workerGroup;
    private Channel serverChannel;
    private Channel sslServerChannel;
    
    private final AtomicBoolean running = new AtomicBoolean(false);
    private final AtomicBoolean shutdown = new AtomicBoolean(false);
    
    /**
     * 启动服务器
     *
     * @return 异步启动结果
     */
    @PostConstruct
    public CompletableFuture<Void> start() {
        if (!running.compareAndSet(false, true)) {
            return CompletableFuture.completedFuture(null);
        }
        
        log.info("正在启动V-MQTT服务器...");
        
        return CompletableFuture.runAsync(() -> {
            try {
                initializeEventLoopGroups();
                startTcpServer();
                
                if (config.isSslEnabled()) {
                    startSslServer();
                }
                
                log.info("V-MQTT服务器启动成功");
                log.info("TCP端口: {}", config.getPort());
                if (config.isSslEnabled()) {
                    log.info("SSL端口: {}", config.getSslPort());
                }
                log.info("最大连接数: {}", config.getMaxConnections());
                log.info("虚拟线程: {}", config.isVirtualThreadEnabled() ? "启用" : "禁用");
                
            } catch (Exception e) {
                log.error("启动V-MQTT服务器失败", e);
                running.set(false);
                throw new RuntimeException("服务器启动失败", e);
            }
        });
    }
    
    /**
     * 停止服务器
     *
     * @return 异步停止结果
     */
    @PreDestroy
    public CompletableFuture<Void> stop() {
        if (!shutdown.compareAndSet(false, true)) {
            return CompletableFuture.completedFuture(null);
        }
        
        log.info("正在关闭V-MQTT服务器...");
        
        return CompletableFuture.runAsync(() -> {
            try {
                // 停止接受新连接
                if (serverChannel != null) {
                    serverChannel.close().sync();
                }
                if (sslServerChannel != null) {
                    sslServerChannel.close().sync();
                }
                
                // 优雅关闭线程池
                if (bossGroup != null) {
                    bossGroup.shutdownGracefully().sync();
                }
                if (workerGroup != null) {
                    workerGroup.shutdownGracefully().sync();
                }
                
                running.set(false);
                log.info("V-MQTT服务器已关闭");
                
            } catch (Exception e) {
                log.error("关闭V-MQTT服务器时发生错误", e);
                throw new RuntimeException("服务器关闭失败", e);
            }
        });
    }
    
    /**
     * 初始化事件循环组
     */
    private void initializeEventLoopGroups() {
        // 检查是否使用Epoll（Linux平台）
        boolean useEpoll = config.isUseEpoll() && isLinux();
        
        if (useEpoll) {
            log.info("使用Epoll事件循环组");
            bossGroup = new EpollEventLoopGroup(config.getBossThreads(),
                new DefaultThreadFactory("mqtt-boss"));
            workerGroup = new EpollEventLoopGroup(config.getWorkerThreads(),
                new DefaultThreadFactory("mqtt-worker"));
        } else {
            log.info("使用NIO事件循环组");
            bossGroup = new NioEventLoopGroup(config.getBossThreads(),
                new DefaultThreadFactory("mqtt-boss"));
            workerGroup = new NioEventLoopGroup(config.getWorkerThreads(),
                new DefaultThreadFactory("mqtt-worker"));
        }
    }
    
    /**
     * 启动TCP服务器
     */
    private void startTcpServer() throws InterruptedException {
        ServerBootstrap bootstrap = createServerBootstrap();
        
        ChannelFuture future = bootstrap.bind(config.getPort()).sync();
        serverChannel = future.channel();
        
        log.info("MQTT TCP服务器已绑定到端口: {}", config.getPort());
    }
    
    /**
     * 启动SSL服务器
     */
    private void startSslServer() throws InterruptedException {
        if (!config.isSslEnabled()) {
            return;
        }
        
        ServerBootstrap sslBootstrap = createServerBootstrap();
        
        ChannelFuture future = sslBootstrap.bind(config.getSslPort()).sync();
        sslServerChannel = future.channel();
        
        log.info("MQTT SSL服务器已绑定到端口: {}", config.getSslPort());
    }
    
    /**
     * 创建服务器引导配置
     */
    private ServerBootstrap createServerBootstrap() {
        boolean useEpoll = config.isUseEpoll() && isLinux();
        
        ServerBootstrap bootstrap = new ServerBootstrap()
            .group(bossGroup, workerGroup)
            .channel(useEpoll ? EpollServerSocketChannel.class : NioServerSocketChannel.class)
            .childHandler(channelInitializer)
            .option(ChannelOption.SO_BACKLOG, config.getSoBacklog())
            .option(ChannelOption.SO_REUSEADDR, true)
            .childOption(ChannelOption.SO_KEEPALIVE, config.isKeepAlive())
            .childOption(ChannelOption.TCP_NODELAY, config.isTcpNoDelay())
            .childOption(ChannelOption.SO_RCVBUF, config.getReceiveBufferSize())
            .childOption(ChannelOption.SO_SNDBUF, config.getSendBufferSize())
            .childOption(ChannelOption.CONNECT_TIMEOUT_MILLIS, config.getConnectTimeout() * 1000);
        
        // 启用零拷贝
        if (config.isZeroCopyEnabled()) {
            bootstrap.childOption(ChannelOption.ALLOCATOR, io.netty.buffer.PooledByteBufAllocator.DEFAULT);
        }
        
        return bootstrap;
    }
    
    /**
     * 检查是否为Linux平台
     */
    private boolean isLinux() {
        return System.getProperty("os.name").toLowerCase().contains("linux");
    }
    
    /**
     * 获取服务器运行状态
     *
     * @return true表示运行中
     */
    public boolean isRunning() {
        return running.get() && !shutdown.get();
    }
    
    /**
     * 获取服务器统计信息
     *
     * @return 服务器状态信息
     */
    public ServerStats getServerStats() {
        return ServerStats.builder()
            .running(isRunning())
            .tcpPort(config.getPort())
            .sslPort(config.getSslPort())
            .sslEnabled(config.isSslEnabled())
            .maxConnections(config.getMaxConnections())
            .virtualThreadEnabled(config.isVirtualThreadEnabled())
            .bossThreads(config.getBossThreads())
            .workerThreads(config.getWorkerThreads())
            .build();
    }
    
    /**
     * 服务器统计信息
     */
    public static class ServerStats {
        private boolean running;
        private int tcpPort;
        private int sslPort;
        private boolean sslEnabled;
        private int maxConnections;
        private boolean virtualThreadEnabled;
        private int bossThreads;
        private int workerThreads;
        
        public static ServerStatsBuilder builder() {
            return new ServerStatsBuilder();
        }
        
        public static class ServerStatsBuilder {
            private boolean running;
            private int tcpPort;
            private int sslPort;
            private boolean sslEnabled;
            private int maxConnections;
            private boolean virtualThreadEnabled;
            private int bossThreads;
            private int workerThreads;
            
            public ServerStatsBuilder running(boolean running) {
                this.running = running;
                return this;
            }
            
            public ServerStatsBuilder tcpPort(int tcpPort) {
                this.tcpPort = tcpPort;
                return this;
            }
            
            public ServerStatsBuilder sslPort(int sslPort) {
                this.sslPort = sslPort;
                return this;
            }
            
            public ServerStatsBuilder sslEnabled(boolean sslEnabled) {
                this.sslEnabled = sslEnabled;
                return this;
            }
            
            public ServerStatsBuilder maxConnections(int maxConnections) {
                this.maxConnections = maxConnections;
                return this;
            }
            
            public ServerStatsBuilder virtualThreadEnabled(boolean virtualThreadEnabled) {
                this.virtualThreadEnabled = virtualThreadEnabled;
                return this;
            }
            
            public ServerStatsBuilder bossThreads(int bossThreads) {
                this.bossThreads = bossThreads;
                return this;
            }
            
            public ServerStatsBuilder workerThreads(int workerThreads) {
                this.workerThreads = workerThreads;
                return this;
            }
            
            public ServerStats build() {
                ServerStats stats = new ServerStats();
                stats.running = this.running;
                stats.tcpPort = this.tcpPort;
                stats.sslPort = this.sslPort;
                stats.sslEnabled = this.sslEnabled;
                stats.maxConnections = this.maxConnections;
                stats.virtualThreadEnabled = this.virtualThreadEnabled;
                stats.bossThreads = this.bossThreads;
                stats.workerThreads = this.workerThreads;
                return stats;
            }
        }
        
        // Getters
        public boolean isRunning() { return running; }
        public int getTcpPort() { return tcpPort; }
        public int getSslPort() { return sslPort; }
        public boolean isSslEnabled() { return sslEnabled; }
        public int getMaxConnections() { return maxConnections; }
        public boolean isVirtualThreadEnabled() { return virtualThreadEnabled; }
        public int getBossThreads() { return bossThreads; }
        public int getWorkerThreads() { return workerThreads; }
    }
}