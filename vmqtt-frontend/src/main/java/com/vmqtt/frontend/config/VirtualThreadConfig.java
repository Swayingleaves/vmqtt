/**
 * 虚拟线程配置
 *
 * @author zhenglin
 * @date 2025/08/06
 */
package com.vmqtt.frontend.config;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;

/**
 * Java 21虚拟线程配置
 * 支持百万并发连接的异步处理
 */
@Slf4j
@Configuration
public class VirtualThreadConfig {
    
    @Autowired
    private NettyServerConfig serverConfig;
    
    /**
     * 创建主要的虚拟线程执行器
     * 用于处理MQTT协议消息
     *
     * @return 虚拟线程执行器
     */
    @Bean("mqttVirtualExecutor")
    public ExecutorService mqttVirtualExecutor() {
        if (!serverConfig.getPerformance().isVirtualThreadEnabled()) {
            log.info("虚拟线程已禁用，使用传统线程池");
            int workerThreads = serverConfig.getPerformance().getWorkerThreads() > 0 
                ? serverConfig.getPerformance().getWorkerThreads() 
                : Runtime.getRuntime().availableProcessors() * 2;
            return Executors.newFixedThreadPool(
                workerThreads * 4,
                createThreadFactory("mqtt-traditional")
            );
        }
        
        log.info("启用虚拟线程池，支持{}个并发任务", serverConfig.getPerformance().getVirtualThreadPoolSize());
        
        // Java 17-19中虚拟线程是预览功能，Java 21+中已正式可用
        try {
            // 尝试使用反射调用虚拟线程API（适配不同JDK版本）
            return (ExecutorService) Class.forName("java.util.concurrent.Executors")
                .getMethod("newVirtualThreadPerTaskExecutor")
                .invoke(null);
        } catch (Exception e) {
            log.warn("虚拟线程不可用，降级使用ForkJoinPool: {}", e.getMessage());
            return java.util.concurrent.ForkJoinPool.commonPool();
        }
    }
    
    /**
     * 创建连接处理虚拟线程执行器
     * 专门处理连接建立和关闭
     *
     * @return 连接处理执行器
     */
    @Bean("connectionVirtualExecutor")
    public ExecutorService connectionVirtualExecutor() {
        if (!serverConfig.getPerformance().isVirtualThreadEnabled()) {
            int workerThreads = serverConfig.getPerformance().getWorkerThreads() > 0 
                ? serverConfig.getPerformance().getWorkerThreads() 
                : Runtime.getRuntime().availableProcessors() * 2;
            return Executors.newFixedThreadPool(
                workerThreads * 2,
                createThreadFactory("connection-traditional")
            );
        }
        
        // Java 17-19中虚拟线程是预览功能，Java 21+中已正式可用
        try {
            // 尝试使用反射调用虚拟线程API（适配不同JDK版本）
            return (ExecutorService) Class.forName("java.util.concurrent.Executors")
                .getMethod("newVirtualThreadPerTaskExecutor")
                .invoke(null);
        } catch (Exception e) {
            log.warn("虚拟线程不可用，降级使用ForkJoinPool: {}", e.getMessage());
            return java.util.concurrent.ForkJoinPool.commonPool();
        }
    }
    
    /**
     * 创建消息处理虚拟线程执行器
     * 专门处理消息路由和分发
     *
     * @return 消息处理执行器
     */
    @Bean("messageVirtualExecutor")
    public ExecutorService messageVirtualExecutor() {
        if (!serverConfig.getPerformance().isVirtualThreadEnabled()) {
            int workerThreads = serverConfig.getPerformance().getWorkerThreads() > 0 
                ? serverConfig.getPerformance().getWorkerThreads() 
                : Runtime.getRuntime().availableProcessors() * 2;
            return Executors.newFixedThreadPool(
                workerThreads * 8,
                createThreadFactory("message-traditional")
            );
        }
        
        // Java 17-19中虚拟线程是预览功能，Java 21+中已正式可用
        try {
            // 尝试使用反射调用虚拟线程API（适配不同JDK版本）
            return (ExecutorService) Class.forName("java.util.concurrent.Executors")
                .getMethod("newVirtualThreadPerTaskExecutor")
                .invoke(null);
        } catch (Exception e) {
            log.warn("虚拟线程不可用，降级使用ForkJoinPool: {}", e.getMessage());
            return java.util.concurrent.ForkJoinPool.commonPool();
        }
    }
    
    /**
     * 创建认证处理虚拟线程执行器
     * 专门处理客户端认证和授权
     *
     * @return 认证处理执行器
     */
    @Bean("authVirtualExecutor")
    public ExecutorService authVirtualExecutor() {
        if (!serverConfig.getPerformance().isVirtualThreadEnabled()) {
            int workerThreads = serverConfig.getPerformance().getWorkerThreads() > 0 
                ? serverConfig.getPerformance().getWorkerThreads() 
                : Runtime.getRuntime().availableProcessors() * 2;
            return Executors.newFixedThreadPool(
                workerThreads,
                createThreadFactory("auth-traditional")
            );
        }
        
        // Java 17-19中虚拟线程是预览功能，Java 21+中已正式可用
        try {
            // 尝试使用反射调用虚拟线程API（适配不同JDK版本）
            return (ExecutorService) Class.forName("java.util.concurrent.Executors")
                .getMethod("newVirtualThreadPerTaskExecutor")
                .invoke(null);
        } catch (Exception e) {
            log.warn("虚拟线程不可用，降级使用ForkJoinPool: {}", e.getMessage());
            return java.util.concurrent.ForkJoinPool.commonPool();
        }
    }
    
    /**
     * 创建心跳检查虚拟线程执行器
     * 处理Keep-Alive和连接超时检查
     *
     * @return 心跳检查执行器
     */
    @Bean("heartbeatVirtualExecutor")
    public ExecutorService heartbeatVirtualExecutor() {
        if (!serverConfig.getPerformance().isVirtualThreadEnabled()) {
            int workerThreads = serverConfig.getPerformance().getWorkerThreads() > 0 
                ? serverConfig.getPerformance().getWorkerThreads() 
                : Runtime.getRuntime().availableProcessors() * 2;
            return Executors.newScheduledThreadPool(
                Math.max(1, workerThreads / 4),
                createThreadFactory("heartbeat-traditional")
            );
        }
        
        // Java 17-19中虚拟线程是预览功能，Java 21+中已正式可用
        try {
            // 尝试使用反射调用虚拟线程API（适配不同JDK版本）
            return (ExecutorService) Class.forName("java.util.concurrent.Executors")
                .getMethod("newVirtualThreadPerTaskExecutor")
                .invoke(null);
        } catch (Exception e) {
            log.warn("虚拟线程不可用，降级使用ForkJoinPool: {}", e.getMessage());
            return java.util.concurrent.ForkJoinPool.commonPool();
        }
    }
    
    /**
     * 创建Spring异步执行器（基于虚拟线程）
     * 用于@Async注解的方法
     *
     * @return Spring异步执行器
     */
    @Bean("taskExecutor")
    public Executor taskExecutor() {
        if (!serverConfig.getPerformance().isVirtualThreadEnabled()) {
            int workerThreads = serverConfig.getPerformance().getWorkerThreads() > 0 
                ? serverConfig.getPerformance().getWorkerThreads() 
                : Runtime.getRuntime().availableProcessors() * 2;
            return Executors.newFixedThreadPool(
                workerThreads * 2,
                createThreadFactory("spring-async")
            );
        }
        
        // Java 17-19中虚拟线程是预览功能，Java 21+中已正式可用
        try {
            // 尝试使用反射调用虚拟线程API（适配不同JDK版本）
            return (ExecutorService) Class.forName("java.util.concurrent.Executors")
                .getMethod("newVirtualThreadPerTaskExecutor")
                .invoke(null);
        } catch (Exception e) {
            log.warn("虚拟线程不可用，降级使用ForkJoinPool: {}", e.getMessage());
            return java.util.concurrent.ForkJoinPool.commonPool();
        }
    }
    
    /**
     * 创建自定义线程工厂
     *
     * @param namePrefix 线程名前缀
     * @return 线程工厂
     */
    private ThreadFactory createThreadFactory(String namePrefix) {
        return new ThreadFactory() {
            private int counter = 0;
            
            @Override
            public Thread newThread(Runnable r) {
                Thread thread = new Thread(r, namePrefix + "-" + (++counter));
                thread.setDaemon(true);
                return thread;
            }
        };
    }
}