/**
 * 虚拟线程管理器
 *
 * @author zhenglin
 * @date 2025/08/06
 */
package com.vmqtt.frontend.server;

import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import java.lang.management.ManagementFactory;
import java.lang.management.ThreadMXBean;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 虚拟线程管理和监控组件
 * 提供线程池状态监控和资源管理
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class VirtualThreadManager {
    
    @Qualifier("mqttVirtualExecutor")
    private final ExecutorService mqttExecutor;
    
    @Qualifier("connectionVirtualExecutor")
    private final ExecutorService connectionExecutor;
    
    @Qualifier("messageVirtualExecutor")
    private final ExecutorService messageExecutor;
    
    @Qualifier("authVirtualExecutor")
    private final ExecutorService authExecutor;
    
    @Qualifier("heartbeatVirtualExecutor")
    private final ExecutorService heartbeatExecutor;
    
    private final MeterRegistry meterRegistry;
    
    // 线程统计计数器
    private final AtomicLong mqttTaskCount = new AtomicLong(0);
    private final AtomicLong connectionTaskCount = new AtomicLong(0);
    private final AtomicLong messageTaskCount = new AtomicLong(0);
    private final AtomicLong authTaskCount = new AtomicLong(0);
    private final AtomicLong heartbeatTaskCount = new AtomicLong(0);
    
    private final AtomicLong mqttCompletedTasks = new AtomicLong(0);
    private final AtomicLong connectionCompletedTasks = new AtomicLong(0);
    private final AtomicLong messageCompletedTasks = new AtomicLong(0);
    private final AtomicLong authCompletedTasks = new AtomicLong(0);
    private final AtomicLong heartbeatCompletedTasks = new AtomicLong(0);
    
    private ThreadMXBean threadMXBean;
    
    /**
     * 初始化线程监控指标
     */
    @PostConstruct
    public void initializeMetrics() {
        threadMXBean = ManagementFactory.getThreadMXBean();
        
        // 注册JVM线程指标
        Gauge.builder("vmqtt.threads.total")
            .description("总线程数")
            .register(meterRegistry, this, VirtualThreadManager::getTotalThreadCount);
            
        Gauge.builder("vmqtt.threads.virtual")
            .description("虚拟线程数")
            .register(meterRegistry, this, VirtualThreadManager::getVirtualThreadCount);
        
        // 注册任务执行指标
        Gauge.builder("vmqtt.tasks.mqtt.active")
            .description("活跃MQTT任务数")
            .register(meterRegistry, this, vm -> vm.mqttTaskCount.get());
            
        Gauge.builder("vmqtt.tasks.connection.active")
            .description("活跃连接任务数")
            .register(meterRegistry, this, vm -> vm.connectionTaskCount.get());
            
        Gauge.builder("vmqtt.tasks.message.active")
            .description("活跃消息任务数")
            .register(meterRegistry, this, vm -> vm.messageTaskCount.get());
            
        Gauge.builder("vmqtt.tasks.auth.active")
            .description("活跃认证任务数")
            .register(meterRegistry, this, vm -> vm.authTaskCount.get());
            
        Gauge.builder("vmqtt.tasks.heartbeat.active")
            .description("活跃心跳任务数")
            .register(meterRegistry, this, vm -> vm.heartbeatTaskCount.get());
        
        log.info("虚拟线程监控指标已初始化");
    }
    
    /**
     * 异步执行MQTT处理任务
     *
     * @param task 任务
     * @return 异步结果
     */
    public CompletableFuture<Void> executeMqttTask(Runnable task) {
        mqttTaskCount.incrementAndGet();
        
        return CompletableFuture.runAsync(() -> {
            try {
                task.run();
            } finally {
                mqttTaskCount.decrementAndGet();
                mqttCompletedTasks.incrementAndGet();
            }
        }, mqttExecutor);
    }
    
    /**
     * 异步执行连接处理任务
     *
     * @param task 任务
     * @return 异步结果
     */
    public CompletableFuture<Void> executeConnectionTask(Runnable task) {
        connectionTaskCount.incrementAndGet();
        
        return CompletableFuture.runAsync(() -> {
            try {
                task.run();
            } finally {
                connectionTaskCount.decrementAndGet();
                connectionCompletedTasks.incrementAndGet();
            }
        }, connectionExecutor);
    }
    
    /**
     * 异步执行消息处理任务
     *
     * @param task 任务
     * @return 异步结果
     */
    public CompletableFuture<Void> executeMessageTask(Runnable task) {
        messageTaskCount.incrementAndGet();
        
        return CompletableFuture.runAsync(() -> {
            try {
                task.run();
            } finally {
                messageTaskCount.decrementAndGet();
                messageCompletedTasks.incrementAndGet();
            }
        }, messageExecutor);
    }
    
    /**
     * 异步执行认证任务
     *
     * @param task 任务
     * @return 异步结果
     */
    public CompletableFuture<Void> executeAuthTask(Runnable task) {
        authTaskCount.incrementAndGet();
        
        return CompletableFuture.runAsync(() -> {
            try {
                task.run();
            } finally {
                authTaskCount.decrementAndGet();
                authCompletedTasks.incrementAndGet();
            }
        }, authExecutor);
    }
    
    /**
     * 异步执行心跳检查任务
     *
     * @param task 任务
     * @return 异步结果
     */
    public CompletableFuture<Void> executeHeartbeatTask(Runnable task) {
        heartbeatTaskCount.incrementAndGet();
        
        return CompletableFuture.runAsync(() -> {
            try {
                task.run();
            } finally {
                heartbeatTaskCount.decrementAndGet();
                heartbeatCompletedTasks.incrementAndGet();
            }
        }, heartbeatExecutor);
    }
    
    /**
     * 获取总线程数
     *
     * @return 总线程数
     */
    public double getTotalThreadCount() {
        return threadMXBean.getThreadCount();
    }
    
    /**
     * 获取虚拟线程数
     * 注意：这是一个估算值，实际实现可能需要JVM特定的API
     *
     * @return 虚拟线程数估算值
     */
    public double getVirtualThreadCount() {
        // 这里是一个简单的估算，实际实现可能需要更复杂的逻辑
        long activeTasks = mqttTaskCount.get() + connectionTaskCount.get() + 
                          messageTaskCount.get() + authTaskCount.get() + 
                          heartbeatTaskCount.get();
        return activeTasks;
    }
    
    /**
     * 获取线程池统计信息
     *
     * @return 统计信息
     */
    public ThreadPoolStats getThreadPoolStats() {
        return ThreadPoolStats.builder()
            .totalThreads((long) getTotalThreadCount())
            .virtualThreads((long) getVirtualThreadCount())
            .mqttActiveTasks(mqttTaskCount.get())
            .connectionActiveTasks(connectionTaskCount.get())
            .messageActiveTasks(messageTaskCount.get())
            .authActiveTasks(authTaskCount.get())
            .heartbeatActiveTasks(heartbeatTaskCount.get())
            .mqttCompletedTasks(mqttCompletedTasks.get())
            .connectionCompletedTasks(connectionCompletedTasks.get())
            .messageCompletedTasks(messageCompletedTasks.get())
            .authCompletedTasks(authCompletedTasks.get())
            .heartbeatCompletedTasks(heartbeatCompletedTasks.get())
            .build();
    }
    
    /**
     * 优雅关闭所有线程池
     */
    @PreDestroy
    public void shutdown() {
        log.info("正在关闭虚拟线程池...");
        
        try {
            // 关闭所有执行器
            shutdownExecutor(mqttExecutor, "MQTT");
            shutdownExecutor(connectionExecutor, "连接");
            shutdownExecutor(messageExecutor, "消息");
            shutdownExecutor(authExecutor, "认证");
            shutdownExecutor(heartbeatExecutor, "心跳");
            
            log.info("所有虚拟线程池已关闭");
        } catch (Exception e) {
            log.error("关闭虚拟线程池时发生错误", e);
        }
    }
    
    /**
     * 优雅关闭单个执行器
     */
    private void shutdownExecutor(ExecutorService executor, String name) {
        try {
            executor.shutdown();
            if (!executor.awaitTermination(10, TimeUnit.SECONDS)) {
                log.warn("{}执行器未在10秒内关闭，强制关闭", name);
                executor.shutdownNow();
            }
        } catch (InterruptedException e) {
            log.warn("等待{}执行器关闭时被中断", name);
            executor.shutdownNow();
            Thread.currentThread().interrupt();
        }
    }
    
    /**
     * 线程池统计信息
     */
    public static class ThreadPoolStats {
        private long totalThreads;
        private long virtualThreads;
        private long mqttActiveTasks;
        private long connectionActiveTasks;
        private long messageActiveTasks;
        private long authActiveTasks;
        private long heartbeatActiveTasks;
        private long mqttCompletedTasks;
        private long connectionCompletedTasks;
        private long messageCompletedTasks;
        private long authCompletedTasks;
        private long heartbeatCompletedTasks;
        
        public static ThreadPoolStatsBuilder builder() {
            return new ThreadPoolStatsBuilder();
        }
        
        public static class ThreadPoolStatsBuilder {
            private long totalThreads;
            private long virtualThreads;
            private long mqttActiveTasks;
            private long connectionActiveTasks;
            private long messageActiveTasks;
            private long authActiveTasks;
            private long heartbeatActiveTasks;
            private long mqttCompletedTasks;
            private long connectionCompletedTasks;
            private long messageCompletedTasks;
            private long authCompletedTasks;
            private long heartbeatCompletedTasks;
            
            public ThreadPoolStatsBuilder totalThreads(long totalThreads) {
                this.totalThreads = totalThreads;
                return this;
            }
            
            public ThreadPoolStatsBuilder virtualThreads(long virtualThreads) {
                this.virtualThreads = virtualThreads;
                return this;
            }
            
            public ThreadPoolStatsBuilder mqttActiveTasks(long mqttActiveTasks) {
                this.mqttActiveTasks = mqttActiveTasks;
                return this;
            }
            
            public ThreadPoolStatsBuilder connectionActiveTasks(long connectionActiveTasks) {
                this.connectionActiveTasks = connectionActiveTasks;
                return this;
            }
            
            public ThreadPoolStatsBuilder messageActiveTasks(long messageActiveTasks) {
                this.messageActiveTasks = messageActiveTasks;
                return this;
            }
            
            public ThreadPoolStatsBuilder authActiveTasks(long authActiveTasks) {
                this.authActiveTasks = authActiveTasks;
                return this;
            }
            
            public ThreadPoolStatsBuilder heartbeatActiveTasks(long heartbeatActiveTasks) {
                this.heartbeatActiveTasks = heartbeatActiveTasks;
                return this;
            }
            
            public ThreadPoolStatsBuilder mqttCompletedTasks(long mqttCompletedTasks) {
                this.mqttCompletedTasks = mqttCompletedTasks;
                return this;
            }
            
            public ThreadPoolStatsBuilder connectionCompletedTasks(long connectionCompletedTasks) {
                this.connectionCompletedTasks = connectionCompletedTasks;
                return this;
            }
            
            public ThreadPoolStatsBuilder messageCompletedTasks(long messageCompletedTasks) {
                this.messageCompletedTasks = messageCompletedTasks;
                return this;
            }
            
            public ThreadPoolStatsBuilder authCompletedTasks(long authCompletedTasks) {
                this.authCompletedTasks = authCompletedTasks;
                return this;
            }
            
            public ThreadPoolStatsBuilder heartbeatCompletedTasks(long heartbeatCompletedTasks) {
                this.heartbeatCompletedTasks = heartbeatCompletedTasks;
                return this;
            }
            
            public ThreadPoolStats build() {
                ThreadPoolStats stats = new ThreadPoolStats();
                stats.totalThreads = this.totalThreads;
                stats.virtualThreads = this.virtualThreads;
                stats.mqttActiveTasks = this.mqttActiveTasks;
                stats.connectionActiveTasks = this.connectionActiveTasks;
                stats.messageActiveTasks = this.messageActiveTasks;
                stats.authActiveTasks = this.authActiveTasks;
                stats.heartbeatActiveTasks = this.heartbeatActiveTasks;
                stats.mqttCompletedTasks = this.mqttCompletedTasks;
                stats.connectionCompletedTasks = this.connectionCompletedTasks;
                stats.messageCompletedTasks = this.messageCompletedTasks;
                stats.authCompletedTasks = this.authCompletedTasks;
                stats.heartbeatCompletedTasks = this.heartbeatCompletedTasks;
                return stats;
            }
        }
        
        // Getters
        public long getTotalThreads() { return totalThreads; }
        public long getVirtualThreads() { return virtualThreads; }
        public long getMqttActiveTasks() { return mqttActiveTasks; }
        public long getConnectionActiveTasks() { return connectionActiveTasks; }
        public long getMessageActiveTasks() { return messageActiveTasks; }
        public long getAuthActiveTasks() { return authActiveTasks; }
        public long getHeartbeatActiveTasks() { return heartbeatActiveTasks; }
        public long getMqttCompletedTasks() { return mqttCompletedTasks; }
        public long getConnectionCompletedTasks() { return connectionCompletedTasks; }
        public long getMessageCompletedTasks() { return messageCompletedTasks; }
        public long getAuthCompletedTasks() { return authCompletedTasks; }
        public long getHeartbeatCompletedTasks() { return heartbeatCompletedTasks; }
    }
}