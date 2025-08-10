/**
 * 虚拟线程优化配置
 *
 * @author zhenglin
 * @date 2025/08/10
 */
package com.vmqtt.frontend.config;

import lombok.extern.slf4j.Slf4j;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.concurrent.CustomizableThreadFactory;

import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Java 21虚拟线程高性能优化配置
 * 针对百万连接场景的特殊优化
 */
@Slf4j
@Configuration
public class VirtualThreadOptimizationConfig {
    
    /**
     * 虚拟线程携带者线程池
     * 用于承载虚拟线程的平台线程
     */
    @Bean("virtualThreadCarrierPool")
    public ForkJoinPool virtualThreadCarrierPool() {
        int parallelism = Math.max(1, Runtime.getRuntime().availableProcessors());
        
        log.info("创建虚拟线程载体池，并发度: {}", parallelism);
        
        return new ForkJoinPool(
            parallelism,
            pool -> {
                ForkJoinWorkerThread worker = ForkJoinPool.defaultForkJoinWorkerThreadFactory.newThread(pool);
                worker.setName("vmqtt-vt-carrier-" + worker.getPoolIndex());
                return worker;
            },
            (thread, exception) -> {
                log.error("虚拟线程载体异常: thread={}, error={}", 
                    thread.getName(), exception.getMessage(), exception);
            },
            true // asyncMode = true for better throughput
        );
    }
    
    /**
     * 高性能虚拟线程执行器
     * 使用定制的线程工厂优化性能
     */
    @Bean("highPerformanceVirtualExecutor")
    public ExecutorService highPerformanceVirtualExecutor() {
        try {
            ThreadFactory virtualThreadFactory = Thread.ofVirtual()
                .name("vmqtt-hpvt-", 0)
                .uncaughtExceptionHandler((thread, exception) -> {
                    log.error("高性能虚拟线程异常: thread={}, error={}", 
                        thread.getName(), exception.getMessage(), exception);
                })
                .factory();
            
            log.info("创建高性能虚拟线程执行器");
            return Executors.newThreadPerTaskExecutor(virtualThreadFactory);
            
        } catch (Exception e) {
            log.warn("高性能虚拟线程不可用，使用优化的ForkJoinPool: {}", e.getMessage());
            return createOptimizedForkJoinPool();
        }
    }
    
    /**
     * 批处理虚拟线程执行器
     * 专门用于批量消息处理
     */
    @Bean("batchProcessingVirtualExecutor")
    public ExecutorService batchProcessingVirtualExecutor() {
        try {
            ThreadFactory batchVirtualFactory = Thread.ofVirtual()
                .name("vmqtt-batch-vt-", 0)
                .uncaughtExceptionHandler((thread, exception) -> {
                    log.error("批处理虚拟线程异常: thread={}, error={}", 
                        thread.getName(), exception.getMessage(), exception);
                })
                .factory();
            
            log.info("创建批处理虚拟线程执行器");
            return Executors.newThreadPerTaskExecutor(batchVirtualFactory);
            
        } catch (Exception e) {
            log.warn("批处理虚拟线程不可用，使用ForkJoinPool: {}", e.getMessage());
            return ForkJoinPool.commonPool();
        }
    }
    
    /**
     * 流量控制虚拟线程执行器
     * 带有内建流量控制和背压机制
     */
    @Bean("flowControlVirtualExecutor")
    public ExecutorService flowControlVirtualExecutor() {
        try {
            ThreadFactory flowControlFactory = Thread.ofVirtual()
                .name("vmqtt-flow-vt-", 0)
                .uncaughtExceptionHandler((thread, exception) -> {
                    log.error("流控虚拟线程异常: thread={}, error={}", 
                        thread.getName(), exception.getMessage(), exception);
                })
                .factory();
            
            // 使用自定义的流控执行器包装
            ExecutorService baseExecutor = Executors.newThreadPerTaskExecutor(flowControlFactory);
            
            log.info("创建流量控制虚拟线程执行器");
            return new FlowControlledExecutorService(baseExecutor, 10000); // 最大并发10000个任务
            
        } catch (Exception e) {
            log.warn("流控虚拟线程不可用，使用有界线程池: {}", e.getMessage());
            return Executors.newFixedThreadPool(
                Runtime.getRuntime().availableProcessors() * 4,
                new CustomizableThreadFactory("vmqtt-flow-fallback-")
            );
        }
    }
    
    /**
     * 创建优化的ForkJoinPool
     */
    private ForkJoinPool createOptimizedForkJoinPool() {
        int parallelism = Runtime.getRuntime().availableProcessors() * 2;
        
        return new ForkJoinPool(
            parallelism,
            pool -> {
                ForkJoinWorkerThread worker = ForkJoinPool.defaultForkJoinWorkerThreadFactory.newThread(pool);
                worker.setName("vmqtt-optimized-" + worker.getPoolIndex());
                return worker;
            },
            (thread, exception) -> {
                log.error("优化线程池异常: thread={}, error={}", 
                    thread.getName(), exception.getMessage(), exception);
            },
            true // asyncMode = true
        );
    }
    
    /**
     * 带有流量控制的执行器包装类
     */
    private static class FlowControlledExecutorService implements ExecutorService {
        private final ExecutorService delegate;
        private final Semaphore permits;
        private final AtomicInteger activeTasks = new AtomicInteger(0);
        private volatile boolean shutdown = false;
        
        public FlowControlledExecutorService(ExecutorService delegate, int maxConcurrentTasks) {
            this.delegate = delegate;
            this.permits = new Semaphore(maxConcurrentTasks);
        }
        
        @Override
        public void execute(Runnable command) {
            if (shutdown) {
                throw new RejectedExecutionException("Executor已关闭");
            }
            
            try {
                permits.acquire();
                activeTasks.incrementAndGet();
                
                delegate.execute(() -> {
                    try {
                        command.run();
                    } finally {
                        permits.release();
                        activeTasks.decrementAndGet();
                    }
                });
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RejectedExecutionException("等待执行许可时被中断", e);
            }
        }
        
        public int getActiveTasks() {
            return activeTasks.get();
        }
        
        public int getAvailablePermits() {
            return permits.availablePermits();
        }
        
        @Override
        public void shutdown() {
            shutdown = true;
            delegate.shutdown();
        }
        
        @Override
        public java.util.List<Runnable> shutdownNow() {
            shutdown = true;
            return delegate.shutdownNow();
        }
        
        @Override
        public boolean isShutdown() {
            return shutdown || delegate.isShutdown();
        }
        
        @Override
        public boolean isTerminated() {
            return delegate.isTerminated();
        }
        
        @Override
        public boolean awaitTermination(long timeout, TimeUnit unit) throws InterruptedException {
            return delegate.awaitTermination(timeout, unit);
        }
        
        @Override
        public <T> Future<T> submit(Callable<T> task) {
            return CompletableFuture.supplyAsync(() -> {
                try {
                    return task.call();
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }, this);
        }
        
        @Override
        public <T> Future<T> submit(Runnable task, T result) {
            return CompletableFuture.supplyAsync(() -> {
                task.run();
                return result;
            }, this);
        }
        
        @Override
        public Future<?> submit(Runnable task) {
            return CompletableFuture.runAsync(task, this);
        }
        
        @Override
        public <T> java.util.List<Future<T>> invokeAll(java.util.Collection<? extends Callable<T>> tasks) 
            throws InterruptedException {
            return delegate.invokeAll(tasks);
        }
        
        @Override
        public <T> java.util.List<Future<T>> invokeAll(java.util.Collection<? extends Callable<T>> tasks, 
                                                       long timeout, TimeUnit unit) 
            throws InterruptedException {
            return delegate.invokeAll(tasks, timeout, unit);
        }
        
        @Override
        public <T> T invokeAny(java.util.Collection<? extends Callable<T>> tasks) 
            throws InterruptedException, ExecutionException {
            return delegate.invokeAny(tasks);
        }
        
        @Override
        public <T> T invokeAny(java.util.Collection<? extends Callable<T>> tasks, long timeout, TimeUnit unit) 
            throws InterruptedException, ExecutionException, TimeoutException {
            return delegate.invokeAny(tasks, timeout, unit);
        }
    }
}