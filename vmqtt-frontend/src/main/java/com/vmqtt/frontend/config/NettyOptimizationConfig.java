/**
 * Netty性能优化配置
 *
 * @author zhenglin
 * @date 2025/08/10
 */
package com.vmqtt.frontend.config;

import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.buffer.UnpooledByteBufAllocator;
import io.netty.channel.ChannelOption;
import io.netty.channel.epoll.Epoll;
import io.netty.channel.epoll.EpollChannelOption;
import io.netty.channel.epoll.EpollServerSocketChannel;
import io.netty.channel.epoll.EpollSocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.util.NettyRuntime;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.HashMap;
import java.util.Map;

/**
 * Netty零拷贝和性能优化配置
 */
@Slf4j
@Configuration
public class NettyOptimizationConfig {
    
    private final NettyServerConfig nettyServerConfig;
    
    public NettyOptimizationConfig(NettyServerConfig nettyServerConfig) {
        this.nettyServerConfig = nettyServerConfig;
    }
    
    /**
     * 获取优化的ByteBuf分配器
     *
     * @return 优化的ByteBuf分配器
     */
    @Bean("optimizedByteBufAllocator")
    public ByteBufAllocator optimizedByteBufAllocator() {
        NettyServerConfig.Performance perf = nettyServerConfig.getPerformance();
        
        if (perf.isUsePooledBuffer()) {
            log.info("使用池化ByteBuf分配器");
            
            // 创建池化分配器，启用零拷贝优化
            return new PooledByteBufAllocator(
                perf.isUseDirectBuffer(), // preferDirect
                Math.max(0, NettyRuntime.availableProcessors() * 2), // nHeapArena
                Math.max(0, NettyRuntime.availableProcessors() * 2), // nDirectArena
                8192, // pageSize
                11, // maxOrder
                64, // smallCacheSize
                32, // normalCacheSize
                perf.isUseDirectBuffer(), // useCacheForAllThreads
                0 // directMemoryCacheAlignment
            );
        } else {
            log.info("使用非池化ByteBuf分配器");
            return new UnpooledByteBufAllocator(perf.isUseDirectBuffer());
        }
    }
    
    /**
     * 获取优化的服务端Channel类型
     *
     * @return 服务端Channel类型
     */
    public Class<?> getOptimizedServerChannelClass() {
        if (Epoll.isAvailable() && nettyServerConfig.getPerformance().isUseEpoll()) {
            log.info("使用Epoll服务端Channel");
            return EpollServerSocketChannel.class;
        } else {
            log.info("使用NIO服务端Channel");
            return NioServerSocketChannel.class;
        }
    }
    
    /**
     * 获取优化的客户端Channel类型
     *
     * @return 客户端Channel类型
     */
    public Class<?> getOptimizedClientChannelClass() {
        if (Epoll.isAvailable() && nettyServerConfig.getPerformance().isUseEpoll()) {
            log.info("使用Epoll客户端Channel");
            return EpollSocketChannel.class;
        } else {
            log.info("使用NIO客户端Channel");
            return NioSocketChannel.class;
        }
    }
    
    /**
     * 获取优化的服务端Channel选项
     *
     * @return 服务端Channel选项映射
     */
    @Bean("optimizedServerChannelOptions")
    public Map<ChannelOption<?>, Object> getOptimizedServerChannelOptions() {
        NettyServerConfig.Performance perf = nettyServerConfig.getPerformance();
        NettyServerConfig.Connection conn = nettyServerConfig.getConnection();
        
        Map<ChannelOption<?>, Object> options = new HashMap<>();
        
        // 基础TCP选项
        options.put(ChannelOption.SO_BACKLOG, perf.getBacklog());
        options.put(ChannelOption.SO_REUSEADDR, true);
        options.put(ChannelOption.TCP_NODELAY, perf.isTcpNoDelay());
        options.put(ChannelOption.SO_KEEPALIVE, perf.isKeepAlive());
        
        // 缓冲区配置（零拷贝优化）
        options.put(ChannelOption.SO_RCVBUF, perf.getReceiveBufferSize());
        options.put(ChannelOption.SO_SNDBUF, perf.getSendBufferSize());
        options.put(ChannelOption.RCVBUF_ALLOCATOR, 
            new io.netty.channel.AdaptiveRecvByteBufAllocator(1024, perf.getBufferSize(), 65536));
        
        // 分配器配置
        options.put(ChannelOption.ALLOCATOR, optimizedByteBufAllocator());
        
        // 写缓冲区水位线设置
        options.put(ChannelOption.WRITE_BUFFER_WATER_MARK, 
            new io.netty.channel.WriteBufferWaterMark(32 * 1024, 64 * 1024));
        
        // 连接超时
        options.put(ChannelOption.CONNECT_TIMEOUT_MILLIS, conn.getTimeout() * 1000);
        
        // 如果支持Epoll，添加Epoll特有的优化选项
        if (Epoll.isAvailable() && perf.isUseEpoll()) {
            addEpollServerOptions(options, perf);
        }
        
        log.info("服务端Channel选项配置完成，选项数量: {}", options.size());
        return options;
    }
    
    /**
     * 获取优化的子Channel选项
     *
     * @return 子Channel选项映射
     */
    @Bean("optimizedChildChannelOptions")
    public Map<ChannelOption<?>, Object> getOptimizedChildChannelOptions() {
        NettyServerConfig.Performance perf = nettyServerConfig.getPerformance();
        NettyServerConfig.Connection conn = nettyServerConfig.getConnection();
        
        Map<ChannelOption<?>, Object> options = new HashMap<>();
        
        // 基础TCP选项
        options.put(ChannelOption.TCP_NODELAY, perf.isTcpNoDelay());
        options.put(ChannelOption.SO_KEEPALIVE, perf.isKeepAlive());
        
        // 缓冲区配置（零拷贝优化）
        options.put(ChannelOption.SO_RCVBUF, perf.getReceiveBufferSize());
        options.put(ChannelOption.SO_SNDBUF, perf.getSendBufferSize());
        
        // 接收缓冲区分配器（自适应大小）
        options.put(ChannelOption.RCVBUF_ALLOCATOR, 
            new io.netty.channel.AdaptiveRecvByteBufAllocator(
                1024, // minimum
                perf.getBufferSize(), // initial
                65536 // maximum
            ));
        
        // ByteBuf分配器
        options.put(ChannelOption.ALLOCATOR, optimizedByteBufAllocator());
        
        // 写缓冲区水位线（用于背压控制）
        options.put(ChannelOption.WRITE_BUFFER_WATER_MARK, 
            new io.netty.channel.WriteBufferWaterMark(
                32 * 1024, // low water mark
                64 * 1024  // high water mark
            ));
        
        // 连接超时
        options.put(ChannelOption.CONNECT_TIMEOUT_MILLIS, conn.getTimeout() * 1000);
        
        // 禁用Nagle算法以减少延迟
        options.put(ChannelOption.TCP_NODELAY, true);
        
        // 如果支持Epoll，添加Epoll特有的优化选项
        if (Epoll.isAvailable() && perf.isUseEpoll()) {
            addEpollChildOptions(options, perf, conn);
        }
        
        log.info("子Channel选项配置完成，选项数量: {}", options.size());
        return options;
    }
    
    /**
     * 添加Epoll服务端特有的优化选项
     */
    private void addEpollServerOptions(Map<ChannelOption<?>, Object> options, 
                                     NettyServerConfig.Performance perf) {
        // SO_REUSEPORT支持（Linux 3.9+）
        options.put(EpollChannelOption.SO_REUSEPORT, true);
        
        // 禁用Nagle算法
        options.put(EpollChannelOption.TCP_NODELAY, perf.isTcpNoDelay());
        
        // 快速重用TIME_WAIT状态的连接
        options.put(EpollChannelOption.SO_REUSEADDR, true);
        
        log.info("添加了Epoll服务端优化选项");
    }
    
    /**
     * 添加Epoll子Channel特有的优化选项
     */
    private void addEpollChildOptions(Map<ChannelOption<?>, Object> options, 
                                    NettyServerConfig.Performance perf,
                                    NettyServerConfig.Connection conn) {
        // TCP相关选项
        options.put(EpollChannelOption.TCP_NODELAY, perf.isTcpNoDelay());
        options.put(EpollChannelOption.SO_KEEPALIVE, perf.isKeepAlive());
        
        // 快速打开（TCP Fast Open）
        options.put(EpollChannelOption.TCP_FASTOPEN, 3);
        
        // TCP用户超时（毫秒）
        options.put(EpollChannelOption.TCP_USER_TIMEOUT, conn.getTimeout() * 1000);
        
        // TCP快速回收
        options.put(EpollChannelOption.TCP_QUICKACK, true);
        
        // TCP拥塞控制算法 - 注意：此选项可能不在所有Netty版本中可用
        // options.put(EpollChannelOption.TCP_CONGESTION, "bbr");
        
        log.info("添加了Epoll子Channel优化选项");
    }
    
    /**
     * 创建零拷贝优化的文件传输配置
     *
     * @return 文件传输配置
     */
    @Bean("zeroCopyFileTransferConfig")
    public ZeroCopyFileTransferConfig zeroCopyFileTransferConfig() {
        return ZeroCopyFileTransferConfig.builder()
            .enableSendfile(true)
            .enableSplice(Epoll.isAvailable())
            .chunkSize(8192)
            .maxChunksPerWrite(16)
            .build();
    }
    
    /**
     * 零拷贝文件传输配置
     */
    public static class ZeroCopyFileTransferConfig {
        private final boolean enableSendfile;
        private final boolean enableSplice;
        private final int chunkSize;
        private final int maxChunksPerWrite;
        
        private ZeroCopyFileTransferConfig(boolean enableSendfile, boolean enableSplice, 
                                         int chunkSize, int maxChunksPerWrite) {
            this.enableSendfile = enableSendfile;
            this.enableSplice = enableSplice;
            this.chunkSize = chunkSize;
            this.maxChunksPerWrite = maxChunksPerWrite;
        }
        
        public static ZeroCopyFileTransferConfigBuilder builder() {
            return new ZeroCopyFileTransferConfigBuilder();
        }
        
        public static class ZeroCopyFileTransferConfigBuilder {
            private boolean enableSendfile = false;
            private boolean enableSplice = false;
            private int chunkSize = 8192;
            private int maxChunksPerWrite = 16;
            
            public ZeroCopyFileTransferConfigBuilder enableSendfile(boolean enableSendfile) {
                this.enableSendfile = enableSendfile;
                return this;
            }
            
            public ZeroCopyFileTransferConfigBuilder enableSplice(boolean enableSplice) {
                this.enableSplice = enableSplice;
                return this;
            }
            
            public ZeroCopyFileTransferConfigBuilder chunkSize(int chunkSize) {
                this.chunkSize = chunkSize;
                return this;
            }
            
            public ZeroCopyFileTransferConfigBuilder maxChunksPerWrite(int maxChunksPerWrite) {
                this.maxChunksPerWrite = maxChunksPerWrite;
                return this;
            }
            
            public ZeroCopyFileTransferConfig build() {
                return new ZeroCopyFileTransferConfig(enableSendfile, enableSplice, 
                                                    chunkSize, maxChunksPerWrite);
            }
        }
        
        // Getters
        public boolean isEnableSendfile() { return enableSendfile; }
        public boolean isEnableSplice() { return enableSplice; }
        public int getChunkSize() { return chunkSize; }
        public int getMaxChunksPerWrite() { return maxChunksPerWrite; }
    }
    
    /**
     * 创建零拷贝消息编码器配置
     *
     * @return 消息编码器配置
     */
    @Bean("zeroCopyMessageEncoderConfig")
    public ZeroCopyMessageEncoderConfig zeroCopyMessageEncoderConfig() {
        return ZeroCopyMessageEncoderConfig.builder()
            .enableCompositeBuffer(true)
            .enableDirectBuffer(nettyServerConfig.getPerformance().isUseDirectBuffer())
            .maxComponentsPerComposite(16)
            .enableWriteAndFlushBatching(true)
            .batchingDelayMicros(10)
            .build();
    }
    
    /**
     * 零拷贝消息编码器配置
     */
    public static class ZeroCopyMessageEncoderConfig {
        private final boolean enableCompositeBuffer;
        private final boolean enableDirectBuffer;
        private final int maxComponentsPerComposite;
        private final boolean enableWriteAndFlushBatching;
        private final int batchingDelayMicros;
        
        private ZeroCopyMessageEncoderConfig(boolean enableCompositeBuffer, boolean enableDirectBuffer,
                                           int maxComponentsPerComposite, boolean enableWriteAndFlushBatching,
                                           int batchingDelayMicros) {
            this.enableCompositeBuffer = enableCompositeBuffer;
            this.enableDirectBuffer = enableDirectBuffer;
            this.maxComponentsPerComposite = maxComponentsPerComposite;
            this.enableWriteAndFlushBatching = enableWriteAndFlushBatching;
            this.batchingDelayMicros = batchingDelayMicros;
        }
        
        public static ZeroCopyMessageEncoderConfigBuilder builder() {
            return new ZeroCopyMessageEncoderConfigBuilder();
        }
        
        public static class ZeroCopyMessageEncoderConfigBuilder {
            private boolean enableCompositeBuffer = true;
            private boolean enableDirectBuffer = true;
            private int maxComponentsPerComposite = 16;
            private boolean enableWriteAndFlushBatching = true;
            private int batchingDelayMicros = 10;
            
            public ZeroCopyMessageEncoderConfigBuilder enableCompositeBuffer(boolean enableCompositeBuffer) {
                this.enableCompositeBuffer = enableCompositeBuffer;
                return this;
            }
            
            public ZeroCopyMessageEncoderConfigBuilder enableDirectBuffer(boolean enableDirectBuffer) {
                this.enableDirectBuffer = enableDirectBuffer;
                return this;
            }
            
            public ZeroCopyMessageEncoderConfigBuilder maxComponentsPerComposite(int maxComponentsPerComposite) {
                this.maxComponentsPerComposite = maxComponentsPerComposite;
                return this;
            }
            
            public ZeroCopyMessageEncoderConfigBuilder enableWriteAndFlushBatching(boolean enableWriteAndFlushBatching) {
                this.enableWriteAndFlushBatching = enableWriteAndFlushBatching;
                return this;
            }
            
            public ZeroCopyMessageEncoderConfigBuilder batchingDelayMicros(int batchingDelayMicros) {
                this.batchingDelayMicros = batchingDelayMicros;
                return this;
            }
            
            public ZeroCopyMessageEncoderConfig build() {
                return new ZeroCopyMessageEncoderConfig(enableCompositeBuffer, enableDirectBuffer,
                                                      maxComponentsPerComposite, enableWriteAndFlushBatching,
                                                      batchingDelayMicros);
            }
        }
        
        // Getters
        public boolean isEnableCompositeBuffer() { return enableCompositeBuffer; }
        public boolean isEnableDirectBuffer() { return enableDirectBuffer; }
        public int getMaxComponentsPerComposite() { return maxComponentsPerComposite; }
        public boolean isEnableWriteAndFlushBatching() { return enableWriteAndFlushBatching; }
        public int getBatchingDelayMicros() { return batchingDelayMicros; }
    }
}