/**
 * MQTT连接处理器
 *
 * @author zhenglin
 * @date 2025/08/08
 */
package com.vmqtt.frontend.server.handler;

import com.vmqtt.frontend.config.NettyServerConfig;
import com.vmqtt.frontend.service.ConnectionLifecycleManager;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.handler.timeout.IdleStateEvent;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.util.concurrent.atomic.AtomicLong;

/**
 * MQTT连接处理器
 * 处理连接的建立、维护和关闭
 */
@Slf4j
@Component
@ChannelHandler.Sharable
@RequiredArgsConstructor
public class MqttConnectionHandler extends ChannelInboundHandlerAdapter {
    
    private final NettyServerConfig config;
    private final ConnectionLifecycleManager lifecycleManager;
    
    // 连接统计
    private final AtomicLong activeConnections = new AtomicLong(0);
    private final AtomicLong totalConnections = new AtomicLong(0);
    private final AtomicLong failedConnections = new AtomicLong(0);
    
    @Override
    public void channelActive(ChannelHandlerContext ctx) throws Exception {
        // 检查连接数限制
        long currentConnections = activeConnections.get();
        if (currentConnections >= config.getConnection().getMaxConnections()) {
            log.warn("连接数超过限制: current={}, max={}, remote={}", 
                currentConnections, config.getConnection().getMaxConnections(), 
                ctx.channel().remoteAddress());
            
            failedConnections.incrementAndGet();
            ctx.close();
            return;
        }
        
        // 增加连接计数
        activeConnections.incrementAndGet();
        totalConnections.incrementAndGet();
        
        log.debug("新连接建立: {}, 当前连接数: {}", 
            ctx.channel().remoteAddress(), activeConnections.get());
        
        try {
            // 处理连接激活
            lifecycleManager.handleChannelActive(ctx.channel());
            
            // 继续传播事件
            super.channelActive(ctx);
            
        } catch (Exception e) {
            log.error("处理连接激活时发生错误: {}", ctx.channel().remoteAddress(), e);
            failedConnections.incrementAndGet();
            ctx.close();
        }
    }
    
    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
        // 减少连接计数
        long currentConnections = activeConnections.decrementAndGet();
        
        log.debug("连接关闭: {}, 当前连接数: {}", 
            ctx.channel().remoteAddress(), currentConnections);
        
        try {
            // 处理连接关闭
            lifecycleManager.handleChannelInactive(ctx.channel());
            
            // 继续传播事件
            super.channelInactive(ctx);
            
        } catch (Exception e) {
            log.error("处理连接关闭时发生错误: {}", ctx.channel().remoteAddress(), e);
        }
    }
    
    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        try {
            // 更新连接活动时间
            lifecycleManager.updateLastActivity(ctx.channel());
            
            // 继续传播消息
            super.channelRead(ctx, msg);
            
        } catch (Exception e) {
            log.error("处理消息读取时发生错误: {}", ctx.channel().remoteAddress(), e);
            lifecycleManager.handleConnectionException(ctx.channel(), e);
        }
    }
    
    @Override
    public void userEventTriggered(ChannelHandlerContext ctx, Object evt) throws Exception {
        if (evt instanceof IdleStateEvent idleStateEvent) {
            log.debug("空闲事件触发: {}, state={}", 
                ctx.channel().remoteAddress(), idleStateEvent.state());
            
            // 处理空闲超时
            lifecycleManager.handleIdleTimeout(ctx.channel(), idleStateEvent.state());
        } else {
            // 继续传播其他事件
            super.userEventTriggered(ctx, evt);
        }
    }
    
    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
        log.error("连接处理器捕获异常: {}", ctx.channel().remoteAddress(), cause);
        
        try {
            // 处理连接异常
            lifecycleManager.handleConnectionException(ctx.channel(), cause);
            
        } catch (Exception e) {
            log.error("处理连接异常时发生错误", e);
            ctx.close();
        }
    }
    
    /**
     * 获取当前活跃连接数
     *
     * @return 活跃连接数
     */
    public long getActiveConnections() {
        return activeConnections.get();
    }
    
    /**
     * 获取总连接数
     *
     * @return 总连接数
     */
    public long getTotalConnections() {
        return totalConnections.get();
    }
    
    /**
     * 获取失败连接数
     *
     * @return 失败连接数
     */
    public long getFailedConnections() {
        return failedConnections.get();
    }
    
    /**
     * 重置统计信息
     */
    public void resetStats() {
        totalConnections.set(0);
        failedConnections.set(0);
        log.info("连接统计信息已重置");
    }
    
    /**
     * 获取连接统计信息
     *
     * @return 统计信息
     */
    public ConnectionStats getConnectionStats() {
        return ConnectionStats.builder()
            .activeConnections(activeConnections.get())
            .totalConnections(totalConnections.get())
            .failedConnections(failedConnections.get())
            .maxConnections(config.getConnection().getMaxConnections())
            .connectionUtilization((double) activeConnections.get() / config.getConnection().getMaxConnections())
            .build();
    }
    
    /**
     * 连接统计信息
     */
    public static class ConnectionStats {
        private long activeConnections;
        private long totalConnections;
        private long failedConnections;
        private int maxConnections;
        private double connectionUtilization;
        
        public static ConnectionStatsBuilder builder() {
            return new ConnectionStatsBuilder();
        }
        
        public static class ConnectionStatsBuilder {
            private long activeConnections;
            private long totalConnections;
            private long failedConnections;
            private int maxConnections;
            private double connectionUtilization;
            
            public ConnectionStatsBuilder activeConnections(long activeConnections) {
                this.activeConnections = activeConnections;
                return this;
            }
            
            public ConnectionStatsBuilder totalConnections(long totalConnections) {
                this.totalConnections = totalConnections;
                return this;
            }
            
            public ConnectionStatsBuilder failedConnections(long failedConnections) {
                this.failedConnections = failedConnections;
                return this;
            }
            
            public ConnectionStatsBuilder maxConnections(int maxConnections) {
                this.maxConnections = maxConnections;
                return this;
            }
            
            public ConnectionStatsBuilder connectionUtilization(double connectionUtilization) {
                this.connectionUtilization = connectionUtilization;
                return this;
            }
            
            public ConnectionStats build() {
                ConnectionStats stats = new ConnectionStats();
                stats.activeConnections = this.activeConnections;
                stats.totalConnections = this.totalConnections;
                stats.failedConnections = this.failedConnections;
                stats.maxConnections = this.maxConnections;
                stats.connectionUtilization = this.connectionUtilization;
                return stats;
            }
        }
        
        // Getters
        public long getActiveConnections() { return activeConnections; }
        public long getTotalConnections() { return totalConnections; }
        public long getFailedConnections() { return failedConnections; }
        public int getMaxConnections() { return maxConnections; }
        public double getConnectionUtilization() { return connectionUtilization; }
    }
}