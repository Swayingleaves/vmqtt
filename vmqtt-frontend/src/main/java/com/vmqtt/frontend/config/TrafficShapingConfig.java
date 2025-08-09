/**
 * 流量整形配置
 *
 * @author zhenglin
 * @date 2025/08/08
 */
package com.vmqtt.frontend.config;

import io.netty.handler.traffic.GlobalTrafficShapingHandler;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

/**
 * 流量整形配置类
 * 用于配置网络流量控制和限速
 */
@Slf4j
@Configuration
@RequiredArgsConstructor
public class TrafficShapingConfig {
    
    private final NettyServerConfig serverConfig;
    
    /**
     * 创建全局流量整形处理器
     *
     * @return 流量整形处理器
     */
    @Bean
    public GlobalTrafficShapingHandler globalTrafficShapingHandler() {
        if (!serverConfig.getConnection().isRateLimitEnabled()) {
            log.info("流量限制未启用，创建无限制的流量处理器");
            return createUnlimitedTrafficHandler();
        }
        
        // 创建用于流量监控的线程池
        ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor(
            r -> {
                Thread thread = new Thread(r, "traffic-shaping");
                thread.setDaemon(true);
                return thread;
            }
        );
        
        // 计算流量限制（字节/秒）
        long writeLimit = calculateWriteLimit();
        long readLimit = calculateReadLimit();
        
        log.info("创建流量整形处理器: writeLimit={}B/s, readLimit={}B/s", writeLimit, readLimit);
        
        GlobalTrafficShapingHandler handler = new GlobalTrafficShapingHandler(
            executor,
            writeLimit,    // 写入限制（字节/秒）
            readLimit,     // 读取限制（字节/秒）
            1000           // 检查间隔（毫秒）
        );
        
        // 配置处理器参数
        configureTrafficHandler(handler);
        
        return handler;
    }
    
    /**
     * 创建无限制的流量处理器
     */
    private GlobalTrafficShapingHandler createUnlimitedTrafficHandler() {
        ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor(
            r -> {
                Thread thread = new Thread(r, "traffic-monitoring");
                thread.setDaemon(true);
                return thread;
            }
        );
        
        return new GlobalTrafficShapingHandler(executor, 0, 0, 1000);
    }
    
    /**
     * 计算写入限制
     */
    private long calculateWriteLimit() {
        int connectionRateLimit = serverConfig.getConnection().getConnectionRateLimit();
        int avgMessageSize = 1024; // 假设平均消息大小1KB
        
        // 基于连接速率和平均消息大小计算写入限制
        // 这里是一个简化的计算，实际应用中可能需要更复杂的算法
        long baseLimit = connectionRateLimit * avgMessageSize;
        
        // 添加一些缓冲空间
        return (long) (baseLimit * 1.2);
    }
    
    /**
     * 计算读取限制
     */
    private long calculateReadLimit() {
        int connectionRateLimit = serverConfig.getConnection().getConnectionRateLimit();
        int avgMessageSize = 512; // 假设平均接收消息大小512B
        
        // 基于连接速率和平均消息大小计算读取限制
        long baseLimit = connectionRateLimit * avgMessageSize;
        
        // 添加一些缓冲空间
        return (long) (baseLimit * 1.5);
    }
    
    /**
     * 配置流量处理器参数
     */
    private void configureTrafficHandler(GlobalTrafficShapingHandler handler) {
        // 设置最大等待时间
        handler.setMaxTimeWait(15000); // 15秒
        
        // 设置检查间隔
        handler.configure(
            handler.getWriteLimit(),
            handler.getReadLimit(),
            1000 // 1秒检查间隔
        );
        
        log.debug("流量处理器配置完成: writeLimit={}, readLimit={}, maxTimeWait={}", 
            handler.getWriteLimit(), handler.getReadLimit(), handler.getMaxTimeWait());
    }
}