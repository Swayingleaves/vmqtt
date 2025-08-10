/**
 * 内存池管理器
 *
 * @author zhenglin
 * @date 2025/08/10
 */
package com.vmqtt.common.util;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.buffer.UnpooledByteBufAllocator;
import io.netty.util.concurrent.FastThreadLocal;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import java.lang.ref.WeakReference;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 高性能内存池管理器
 * 支持多种内存池策略，针对MQTT高并发场景优化
 */
@Slf4j
@Component
public class MemoryPoolManager {
    
    // 内存池配置
    private static final int SMALL_BUFFER_SIZE = 1024;        // 1KB
    private static final int MEDIUM_BUFFER_SIZE = 8192;       // 8KB
    private static final int LARGE_BUFFER_SIZE = 65536;       // 64KB
    private static final int MAX_POOL_SIZE = 10000;           // 每种大小的最大池化对象数
    
    // ByteBuf内存池
    private ByteBufAllocator byteBufAllocator;
    
    // 自定义对象池
    private final ObjectPool<byte[]> smallByteArrayPool = new ObjectPool<>("SmallByteArray");
    private final ObjectPool<byte[]> mediumByteArrayPool = new ObjectPool<>("MediumByteArray");  
    private final ObjectPool<byte[]> largeByteArrayPool = new ObjectPool<>("LargeByteArray");
    
    // 字符串池（用于客户端ID、主题等）
    private final ConcurrentHashMap<String, WeakReference<String>> stringPool = new ConcurrentHashMap<>();
    
    // 线程本地缓存
    private final FastThreadLocal<ThreadLocalCache> threadLocalCache = new FastThreadLocal<ThreadLocalCache>() {
        @Override
        protected ThreadLocalCache initialValue() throws Exception {
            return new ThreadLocalCache();
        }
    };
    
    // 统计信息
    private final AtomicLong totalAllocations = new AtomicLong(0);
    private final AtomicLong totalDeallocations = new AtomicLong(0);
    private final AtomicLong poolHits = new AtomicLong(0);
    private final AtomicLong poolMisses = new AtomicLong(0);
    
    // 后台清理任务
    private ScheduledExecutorService cleanupScheduler;
    
    /**
     * 初始化内存池管理器
     */
    @PostConstruct
    public void initialize() {
        log.info("正在初始化内存池管理器...");
        
        // 创建优化的ByteBuf分配器
        byteBufAllocator = new PooledByteBufAllocator(
            true,  // preferDirect
            Runtime.getRuntime().availableProcessors() * 2, // nHeapArena
            Runtime.getRuntime().availableProcessors() * 2, // nDirectArena
            8192,  // pageSize
            11,    // maxOrder
            64,    // smallCacheSize
            32,    // normalCacheSize
            true,  // useCacheForAllThreads
            0      // directMemoryCacheAlignment
        );
        
        // 预填充对象池
        preFillPools();
        
        // 启动清理任务
        startCleanupTasks();
        
        log.info("内存池管理器初始化完成");
        logMemoryPoolInfo();
    }
    
    /**
     * 关闭内存池管理器
     */
    @PreDestroy
    public void shutdown() {
        log.info("正在关闭内存池管理器...");
        
        if (cleanupScheduler != null) {
            cleanupScheduler.shutdown();
            try {
                if (!cleanupScheduler.awaitTermination(10, TimeUnit.SECONDS)) {
                    cleanupScheduler.shutdownNow();
                }
            } catch (InterruptedException e) {
                cleanupScheduler.shutdownNow();
                Thread.currentThread().interrupt();
            }
        }
        
        // 清理线程本地缓存
        threadLocalCache.removeAll();
        
        // 清理字符串池
        stringPool.clear();
        
        log.info("内存池管理器已关闭");
        logFinalStatistics();
    }
    
    /**
     * 预填充对象池
     */
    private void preFillPools() {
        // 预分配一定数量的对象到池中
        int preFillSize = Math.min(1000, MAX_POOL_SIZE / 10);
        
        // 预填充小字节数组池
        for (int i = 0; i < preFillSize; i++) {
            smallByteArrayPool.returnObject(new byte[SMALL_BUFFER_SIZE]);
        }
        
        // 预填充中等字节数组池
        for (int i = 0; i < preFillSize / 2; i++) {
            mediumByteArrayPool.returnObject(new byte[MEDIUM_BUFFER_SIZE]);
        }
        
        // 预填充大字节数组池
        for (int i = 0; i < preFillSize / 4; i++) {
            largeByteArrayPool.returnObject(new byte[LARGE_BUFFER_SIZE]);
        }
        
        log.info("对象池预填充完成: small={}, medium={}, large={}", 
            preFillSize, preFillSize / 2, preFillSize / 4);
    }
    
    /**
     * 启动清理任务
     */
    private void startCleanupTasks() {
        cleanupScheduler = Executors.newScheduledThreadPool(2);
        
        // 定期清理字符串池中的死引用
        cleanupScheduler.scheduleAtFixedRate(this::cleanupStringPool, 5, 5, TimeUnit.MINUTES);
        
        // 定期记录统计信息
        cleanupScheduler.scheduleAtFixedRate(this::logStatistics, 1, 1, TimeUnit.MINUTES);
        
        log.info("内存池清理任务已启动");
    }
    
    /**
     * 获取ByteBuf分配器
     */
    public ByteBufAllocator getByteBufAllocator() {
        return byteBufAllocator;
    }
    
    /**
     * 分配ByteBuf
     */
    public ByteBuf allocateByteBuf(int capacity) {
        totalAllocations.incrementAndGet();
        
        if (capacity <= SMALL_BUFFER_SIZE) {
            return byteBufAllocator.buffer(SMALL_BUFFER_SIZE);
        } else if (capacity <= MEDIUM_BUFFER_SIZE) {
            return byteBufAllocator.buffer(MEDIUM_BUFFER_SIZE);
        } else if (capacity <= LARGE_BUFFER_SIZE) {
            return byteBufAllocator.buffer(LARGE_BUFFER_SIZE);
        } else {
            return byteBufAllocator.buffer(capacity);
        }
    }
    
    /**
     * 分配直接ByteBuf
     */
    public ByteBuf allocateDirectByteBuf(int capacity) {
        totalAllocations.incrementAndGet();
        return byteBufAllocator.directBuffer(capacity);
    }
    
    /**
     * 分配字节数组
     */
    public byte[] allocateByteArray(int size) {
        totalAllocations.incrementAndGet();
        
        // 先检查线程本地缓存
        ThreadLocalCache cache = threadLocalCache.get();
        byte[] cached = cache.getByteArray(size);
        if (cached != null) {
            poolHits.incrementAndGet();
            return cached;
        }
        
        byte[] array = null;
        
        if (size <= SMALL_BUFFER_SIZE) {
            array = smallByteArrayPool.borrowObject();
            if (array == null) {
                array = new byte[SMALL_BUFFER_SIZE];
                poolMisses.incrementAndGet();
            } else {
                poolHits.incrementAndGet();
            }
        } else if (size <= MEDIUM_BUFFER_SIZE) {
            array = mediumByteArrayPool.borrowObject();
            if (array == null) {
                array = new byte[MEDIUM_BUFFER_SIZE];
                poolMisses.incrementAndGet();
            } else {
                poolHits.incrementAndGet();
            }
        } else if (size <= LARGE_BUFFER_SIZE) {
            array = largeByteArrayPool.borrowObject();
            if (array == null) {
                array = new byte[LARGE_BUFFER_SIZE];
                poolMisses.incrementAndGet();
            } else {
                poolHits.incrementAndGet();
            }
        } else {
            array = new byte[size];
            poolMisses.incrementAndGet();
        }
        
        return array;
    }
    
    /**
     * 释放字节数组
     */
    public void deallocateByteArray(byte[] array) {
        if (array == null) return;
        
        totalDeallocations.incrementAndGet();
        
        // 先尝试放入线程本地缓存
        ThreadLocalCache cache = threadLocalCache.get();
        if (cache.putByteArray(array)) {
            return;
        }
        
        // 根据大小归还到对应的池
        int length = array.length;
        if (length == SMALL_BUFFER_SIZE) {
            smallByteArrayPool.returnObject(array);
        } else if (length == MEDIUM_BUFFER_SIZE) {
            mediumByteArrayPool.returnObject(array);
        } else if (length == LARGE_BUFFER_SIZE) {
            largeByteArrayPool.returnObject(array);
        }
        // 其他大小的数组直接丢弃，让GC回收
    }
    
    /**
     * 字符串内驻（减少重复字符串占用的内存）
     */
    public String intern(String str) {
        if (str == null) return null;
        
        WeakReference<String> ref = stringPool.get(str);
        if (ref != null) {
            String cached = ref.get();
            if (cached != null) {
                return cached;
            } else {
                // 引用已失效，移除
                stringPool.remove(str, ref);
            }
        }
        
        // 创建新的引用
        stringPool.put(str, new WeakReference<>(str));
        return str;
    }
    
    /**
     * 清理字符串池中的死引用
     */
    private void cleanupStringPool() {
        int removedCount = 0;
        
        for (var entry : stringPool.entrySet()) {
            if (entry.getValue().get() == null) {
                if (stringPool.remove(entry.getKey(), entry.getValue())) {
                    removedCount++;
                }
            }
        }
        
        if (removedCount > 0) {
            log.debug("清理字符串池死引用: {} 个", removedCount);
        }
    }
    
    /**
     * 记录统计信息
     */
    private void logStatistics() {
        long allocations = totalAllocations.get();
        long deallocations = totalDeallocations.get();
        long hits = poolHits.get();
        long misses = poolMisses.get();
        
        double hitRate = hits + misses > 0 ? (double) hits / (hits + misses) * 100 : 0;
        
        log.debug("内存池统计 - 分配: {}, 释放: {}, 命中率: {:.1f}%, 字符串池大小: {}", 
            allocations, deallocations, hitRate, stringPool.size());
    }
    
    /**
     * 记录内存池信息
     */
    private void logMemoryPoolInfo() {
        log.info("=== 内存池管理器配置 ===");
        log.info("ByteBuf分配器: {}", byteBufAllocator.getClass().getSimpleName());
        log.info("小缓冲区大小: {} 字节", SMALL_BUFFER_SIZE);
        log.info("中等缓冲区大小: {} 字节", MEDIUM_BUFFER_SIZE);
        log.info("大缓冲区大小: {} 字节", LARGE_BUFFER_SIZE);
        log.info("最大池大小: {}", MAX_POOL_SIZE);
        log.info("线程本地缓存: 启用");
        log.info("字符串内驻池: 启用");
    }
    
    /**
     * 记录最终统计信息
     */
    private void logFinalStatistics() {
        long allocations = totalAllocations.get();
        long deallocations = totalDeallocations.get();
        long hits = poolHits.get();
        long misses = poolMisses.get();
        
        double hitRate = hits + misses > 0 ? (double) hits / (hits + misses) * 100 : 0;
        
        log.info("=== 内存池最终统计 ===");
        log.info("总分配次数: {}", allocations);
        log.info("总释放次数: {}", deallocations);
        log.info("池命中次数: {}", hits);
        log.info("池未命中次数: {}", misses);
        log.info("整体命中率: {:.1f}%", hitRate);
        log.info("字符串池最终大小: {}", stringPool.size());
    }
    
    /**
     * 获取内存池统计信息
     */
    public MemoryPoolStats getStats() {
        long allocations = totalAllocations.get();
        long deallocations = totalDeallocations.get();
        long hits = poolHits.get();
        long misses = poolMisses.get();
        
        double hitRate = hits + misses > 0 ? (double) hits / (hits + misses) * 100 : 0;
        
        return MemoryPoolStats.builder()
            .totalAllocations(allocations)
            .totalDeallocations(deallocations)
            .poolHits(hits)
            .poolMisses(misses)
            .hitRate(hitRate)
            .stringPoolSize(stringPool.size())
            .smallPoolSize(smallByteArrayPool.size())
            .mediumPoolSize(mediumByteArrayPool.size())
            .largePoolSize(largeByteArrayPool.size())
            .build();
    }
    
    /**
     * 线程本地缓存
     */
    private static class ThreadLocalCache {
        private static final int MAX_CACHED_ARRAYS = 8;
        
        private final ConcurrentLinkedQueue<byte[]> smallArrays = new ConcurrentLinkedQueue<>();
        private final ConcurrentLinkedQueue<byte[]> mediumArrays = new ConcurrentLinkedQueue<>();
        private final ConcurrentLinkedQueue<byte[]> largeArrays = new ConcurrentLinkedQueue<>();
        
        public byte[] getByteArray(int size) {
            if (size <= SMALL_BUFFER_SIZE) {
                return smallArrays.poll();
            } else if (size <= MEDIUM_BUFFER_SIZE) {
                return mediumArrays.poll();
            } else if (size <= LARGE_BUFFER_SIZE) {
                return largeArrays.poll();
            }
            return null;
        }
        
        public boolean putByteArray(byte[] array) {
            if (array == null) return false;
            
            int length = array.length;
            if (length == SMALL_BUFFER_SIZE && smallArrays.size() < MAX_CACHED_ARRAYS) {
                smallArrays.offer(array);
                return true;
            } else if (length == MEDIUM_BUFFER_SIZE && mediumArrays.size() < MAX_CACHED_ARRAYS) {
                mediumArrays.offer(array);
                return true;
            } else if (length == LARGE_BUFFER_SIZE && largeArrays.size() < MAX_CACHED_ARRAYS) {
                largeArrays.offer(array);
                return true;
            }
            
            return false;
        }
    }
    
    /**
     * 通用对象池
     */
    private static class ObjectPool<T> {
        private final String name;
        private final ConcurrentLinkedQueue<T> pool = new ConcurrentLinkedQueue<>();
        
        public ObjectPool(String name) {
            this.name = name;
        }
        
        public T borrowObject() {
            return pool.poll();
        }
        
        public void returnObject(T object) {
            if (object != null && pool.size() < MAX_POOL_SIZE) {
                pool.offer(object);
            }
        }
        
        public int size() {
            return pool.size();
        }
        
        public String getName() {
            return name;
        }
    }
    
    /**
     * 内存池统计信息
     */
    public static class MemoryPoolStats {
        private final long totalAllocations;
        private final long totalDeallocations;
        private final long poolHits;
        private final long poolMisses;
        private final double hitRate;
        private final int stringPoolSize;
        private final int smallPoolSize;
        private final int mediumPoolSize;
        private final int largePoolSize;
        
        private MemoryPoolStats(long totalAllocations, long totalDeallocations, long poolHits,
                               long poolMisses, double hitRate, int stringPoolSize,
                               int smallPoolSize, int mediumPoolSize, int largePoolSize) {
            this.totalAllocations = totalAllocations;
            this.totalDeallocations = totalDeallocations;
            this.poolHits = poolHits;
            this.poolMisses = poolMisses;
            this.hitRate = hitRate;
            this.stringPoolSize = stringPoolSize;
            this.smallPoolSize = smallPoolSize;
            this.mediumPoolSize = mediumPoolSize;
            this.largePoolSize = largePoolSize;
        }
        
        public static MemoryPoolStatsBuilder builder() {
            return new MemoryPoolStatsBuilder();
        }
        
        public static class MemoryPoolStatsBuilder {
            private long totalAllocations;
            private long totalDeallocations;
            private long poolHits;
            private long poolMisses;
            private double hitRate;
            private int stringPoolSize;
            private int smallPoolSize;
            private int mediumPoolSize;
            private int largePoolSize;
            
            public MemoryPoolStatsBuilder totalAllocations(long totalAllocations) {
                this.totalAllocations = totalAllocations;
                return this;
            }
            
            public MemoryPoolStatsBuilder totalDeallocations(long totalDeallocations) {
                this.totalDeallocations = totalDeallocations;
                return this;
            }
            
            public MemoryPoolStatsBuilder poolHits(long poolHits) {
                this.poolHits = poolHits;
                return this;
            }
            
            public MemoryPoolStatsBuilder poolMisses(long poolMisses) {
                this.poolMisses = poolMisses;
                return this;
            }
            
            public MemoryPoolStatsBuilder hitRate(double hitRate) {
                this.hitRate = hitRate;
                return this;
            }
            
            public MemoryPoolStatsBuilder stringPoolSize(int stringPoolSize) {
                this.stringPoolSize = stringPoolSize;
                return this;
            }
            
            public MemoryPoolStatsBuilder smallPoolSize(int smallPoolSize) {
                this.smallPoolSize = smallPoolSize;
                return this;
            }
            
            public MemoryPoolStatsBuilder mediumPoolSize(int mediumPoolSize) {
                this.mediumPoolSize = mediumPoolSize;
                return this;
            }
            
            public MemoryPoolStatsBuilder largePoolSize(int largePoolSize) {
                this.largePoolSize = largePoolSize;
                return this;
            }
            
            public MemoryPoolStats build() {
                return new MemoryPoolStats(totalAllocations, totalDeallocations, poolHits, poolMisses,
                                          hitRate, stringPoolSize, smallPoolSize, mediumPoolSize, largePoolSize);
            }
        }
        
        // Getters
        public long getTotalAllocations() { return totalAllocations; }
        public long getTotalDeallocations() { return totalDeallocations; }
        public long getPoolHits() { return poolHits; }
        public long getPoolMisses() { return poolMisses; }
        public double getHitRate() { return hitRate; }
        public int getStringPoolSize() { return stringPoolSize; }
        public int getSmallPoolSize() { return smallPoolSize; }
        public int getMediumPoolSize() { return mediumPoolSize; }
        public int getLargePoolSize() { return largePoolSize; }
    }
}