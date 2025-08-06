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
        if (!serverConfig.isVirtualThreadEnabled()) {
            log.info("虚拟线程已禁用，使用传统线程池");
            return Executors.newFixedThreadPool(
                serverConfig.getWorkerThreads() * 4,
                createThreadFactory("mqtt-traditional")
            );
        }
        
        log.info("启用虚拟线程池，支持{}个并发任务", serverConfig.getVirtualThreadPoolSize());
        
        return Executors.newVirtualThreadPerTaskExecutor();
    }
    
    /**
     * 创建连接处理虚拟线程执行器
     * 专门处理连接建立和关闭
     *
     * @return 连接处理执行器
     */
    @Bean("connectionVirtualExecutor")
    public ExecutorService connectionVirtualExecutor() {
        if (!serverConfig.isVirtualThreadEnabled()) {
            return Executors.newFixedThreadPool(
                serverConfig.getWorkerThreads() * 2,
                createThreadFactory("connection-traditional")
            );
        }
        
        return Executors.newVirtualThreadPerTaskExecutor();
    }
    
    /**
     * 创建消息处理虚拟线程执行器
     * 专门处理消息路由和分发
     *
     * @return 消息处理执行器
     */
    @Bean("messageVirtualExecutor")
    public ExecutorService messageVirtualExecutor() {
        if (!serverConfig.isVirtualThreadEnabled()) {
            return Executors.newFixedThreadPool(
                serverConfig.getWorkerThreads() * 8,
                createThreadFactory("message-traditional")
            );
        }
        
        return Executors.newVirtualThreadPerTaskExecutor();
    }
    
    /**
     * 创建认证处理虚拟线程执行器
     * 专门处理客户端认证和授权
     *
     * @return 认证处理执行器
     */
    @Bean("authVirtualExecutor")
    public ExecutorService authVirtualExecutor() {
        if (!serverConfig.isVirtualThreadEnabled()) {
            return Executors.newFixedThreadPool(
                serverConfig.getWorkerThreads(),
                createThreadFactory("auth-traditional")
            );
        }
        
        return Executors.newVirtualThreadPerTaskExecutor();
    }
    
    /**
     * 创建心跳检查虚拟线程执行器
     * 处理Keep-Alive和连接超时检查
     *
     * @return 心跳检查执行器
     */
    @Bean("heartbeatVirtualExecutor")
    public ExecutorService heartbeatVirtualExecutor() {
        if (!serverConfig.isVirtualThreadEnabled()) {
            return Executors.newScheduledThreadPool(
                Math.max(1, serverConfig.getWorkerThreads() / 4),
                createThreadFactory("heartbeat-traditional")
            );
        }
        
        return Executors.newVirtualThreadPerTaskExecutor();
    }
    
    /**
     * 创建Spring异步执行器（基于虚拟线程）
     * 用于@Async注解的方法
     *
     * @return Spring异步执行器
     */
    @Bean("taskExecutor")
    public Executor taskExecutor() {
        if (!serverConfig.isVirtualThreadEnabled()) {
            return Executors.newFixedThreadPool(
                serverConfig.getWorkerThreads() * 2,
                createThreadFactory("spring-async")
            );
        }
        
        return Executors.newVirtualThreadPerTaskExecutor();
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