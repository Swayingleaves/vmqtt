/**
 * JVM优化配置
 *
 * @author zhenglin
 * @date 2025/08/10
 */
package com.vmqtt.vmqttcore.config;

import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.event.EventListener;

import java.lang.management.GarbageCollectorMXBean;
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.lang.management.RuntimeMXBean;
import java.util.List;

/**
 * JVM和ZGC优化配置类
 * 提供JVM性能监控和优化建议
 */
@Slf4j
@Configuration
public class JvmOptimizationConfig {
    
    /**
     * 应用启动后检查JVM配置
     */
    @EventListener(ApplicationReadyEvent.class)
    public void checkJvmConfiguration() {
        logJvmInfo();
        checkZgcConfiguration();
        logGcConfiguration();
        logMemoryConfiguration();
        provideOptimizationSuggestions();
    }
    
    /**
     * 记录JVM基本信息
     */
    private void logJvmInfo() {
        RuntimeMXBean runtimeMXBean = ManagementFactory.getRuntimeMXBean();
        
        log.info("=== JVM 基本信息 ===");
        log.info("JVM 名称: {}", runtimeMXBean.getVmName());
        log.info("JVM 版本: {}", runtimeMXBean.getVmVersion());
        log.info("JVM 厂商: {}", runtimeMXBean.getVmVendor());
        log.info("Java 版本: {}", System.getProperty("java.version"));
        log.info("Java 运行时版本: {}", System.getProperty("java.runtime.version"));
        
        List<String> inputArguments = runtimeMXBean.getInputArguments();
        log.info("JVM 启动参数数量: {}", inputArguments.size());
        
        for (String arg : inputArguments) {
            if (arg.startsWith("-X") || arg.startsWith("-XX")) {
                log.info("  JVM 参数: {}", arg);
            }
        }
    }
    
    /**
     * 检查ZGC配置
     */
    private void checkZgcConfiguration() {
        RuntimeMXBean runtimeMXBean = ManagementFactory.getRuntimeMXBean();
        List<String> inputArguments = runtimeMXBean.getInputArguments();
        
        boolean useZGC = inputArguments.stream().anyMatch(arg -> 
            arg.equals("-XX:+UseZGC") || arg.equals("-XX:+UnlockExperimentalVMOptions"));
            
        boolean enablePreview = inputArguments.stream().anyMatch(arg -> 
            arg.equals("--enable-preview"));
            
        log.info("=== ZGC 配置检查 ===");
        log.info("ZGC 启用状态: {}", useZGC ? "已启用" : "未启用");
        log.info("预览功能启用: {}", enablePreview ? "已启用" : "未启用");
        
        if (!useZGC) {
            log.warn("推荐启用ZGC以获得更好的低延迟性能");
            log.warn("添加JVM参数: -XX:+UseZGC -XX:+UnlockExperimentalVMOptions");
        }
        
        if (!enablePreview) {
            log.warn("推荐启用预览功能以使用虚拟线程");
            log.warn("添加JVM参数: --enable-preview");
        }
    }
    
    /**
     * 记录GC配置信息
     */
    private void logGcConfiguration() {
        List<GarbageCollectorMXBean> gcBeans = ManagementFactory.getGarbageCollectorMXBeans();
        
        log.info("=== GC 配置信息 ===");
        log.info("垃圾收集器数量: {}", gcBeans.size());
        
        for (GarbageCollectorMXBean gcBean : gcBeans) {
            log.info("GC 名称: {}", gcBean.getName());
            log.info("  内存池名称: {}", String.join(", ", gcBean.getMemoryPoolNames()));
            log.info("  收集次数: {}", gcBean.getCollectionCount());
            log.info("  收集时间: {} ms", gcBean.getCollectionTime());
            
            if (gcBean.getCollectionCount() > 0) {
                double avgTime = (double) gcBean.getCollectionTime() / gcBean.getCollectionCount();
                log.info("  平均收集时间: {:.2f} ms", avgTime);
            }
        }
    }
    
    /**
     * 记录内存配置信息
     */
    private void logMemoryConfiguration() {
        MemoryMXBean memoryMXBean = ManagementFactory.getMemoryMXBean();
        
        long heapUsed = memoryMXBean.getHeapMemoryUsage().getUsed();
        long heapMax = memoryMXBean.getHeapMemoryUsage().getMax();
        long nonHeapUsed = memoryMXBean.getNonHeapMemoryUsage().getUsed();
        long nonHeapMax = memoryMXBean.getNonHeapMemoryUsage().getMax();
        
        log.info("=== 内存配置信息 ===");
        log.info("堆内存使用: {} MB / {} MB ({:.1f}%)", 
            heapUsed / 1024 / 1024, 
            heapMax / 1024 / 1024,
            (double) heapUsed / heapMax * 100);
            
        log.info("非堆内存使用: {} MB / {} MB", 
            nonHeapUsed / 1024 / 1024,
            nonHeapMax > 0 ? nonHeapMax / 1024 / 1024 : -1);
            
        // 检查直接内存配置
        String maxDirectMemory = System.getProperty("sun.nio.MaxDirectMemorySize");
        if (maxDirectMemory != null) {
            log.info("最大直接内存: {} MB", Long.parseLong(maxDirectMemory) / 1024 / 1024);
        } else {
            log.info("最大直接内存: 未设置 (默认为堆内存大小)");
        }
    }
    
    /**
     * 提供优化建议
     */
    private void provideOptimizationSuggestions() {
        RuntimeMXBean runtimeMXBean = ManagementFactory.getRuntimeMXBean();
        List<String> inputArguments = runtimeMXBean.getInputArguments();
        
        log.info("=== 性能优化建议 ===");
        
        // 检查是否使用了推荐的ZGC参数
        boolean hasOptimalZgcConfig = inputArguments.stream().anyMatch(arg -> 
            arg.contains("-XX:+UseZGC"));
            
        if (!hasOptimalZgcConfig) {
            log.warn("建议使用以下ZGC优化参数:");
            log.warn("  -XX:+UseZGC");
            log.warn("  -XX:+UnlockExperimentalVMOptions");
            log.warn("  -XX:+UseTransparentHugePages");
            log.warn("  -XX:SoftMaxHeapSize=8g");
        }
        
        // 检查虚拟线程配置
        boolean hasPreviewEnabled = inputArguments.stream().anyMatch(arg -> 
            arg.equals("--enable-preview"));
            
        if (!hasPreviewEnabled) {
            log.warn("建议启用预览功能以使用虚拟线程:");
            log.warn("  --enable-preview");
        }
        
        // 检查内存配置
        long maxHeap = ManagementFactory.getMemoryMXBean().getHeapMemoryUsage().getMax();
        long recommendedHeap = 8L * 1024 * 1024 * 1024; // 8GB
        
        if (maxHeap < recommendedHeap) {
            log.warn("建议增加堆内存大小到8GB或更大:");
            log.warn("  -Xmx8g");
        }
        
        // 检查直接内存配置
        String maxDirectMemory = System.getProperty("sun.nio.MaxDirectMemorySize");
        if (maxDirectMemory == null) {
            log.warn("建议设置直接内存大小:");
            log.warn("  -XX:MaxDirectMemorySize=4g");
        }
        
        log.info("完整的推荐JVM启动参数:");
        log.info("  -XX:+UseZGC");
        log.info("  -XX:+UnlockExperimentalVMOptions");
        log.info("  -XX:+UseTransparentHugePages");
        log.info("  -XX:SoftMaxHeapSize=8g");
        log.info("  -Xmx8g");
        log.info("  -XX:MaxDirectMemorySize=4g");
        log.info("  --enable-preview");
        log.info("  -XX:+UseStringDeduplication");
        log.info("  -XX:+OptimizeStringConcat");
        log.info("  -XX:+UseCompressedOops");
        log.info("  -XX:+UseCompressedClassPointers");
    }
    
    /**
     * 获取当前GC统计信息
     *
     * @return GC统计信息
     */
    public GcStats getCurrentGcStats() {
        List<GarbageCollectorMXBean> gcBeans = ManagementFactory.getGarbageCollectorMXBeans();
        
        long totalCollectionCount = 0;
        long totalCollectionTime = 0;
        
        for (GarbageCollectorMXBean gcBean : gcBeans) {
            totalCollectionCount += gcBean.getCollectionCount();
            totalCollectionTime += gcBean.getCollectionTime();
        }
        
        return GcStats.builder()
            .totalCollections(totalCollectionCount)
            .totalCollectionTimeMs(totalCollectionTime)
            .averageCollectionTimeMs(totalCollectionCount > 0 ? 
                (double) totalCollectionTime / totalCollectionCount : 0.0)
            .build();
    }
    
    /**
     * GC统计信息
     */
    public static class GcStats {
        private final long totalCollections;
        private final long totalCollectionTimeMs;
        private final double averageCollectionTimeMs;
        
        private GcStats(long totalCollections, long totalCollectionTimeMs, 
                       double averageCollectionTimeMs) {
            this.totalCollections = totalCollections;
            this.totalCollectionTimeMs = totalCollectionTimeMs;
            this.averageCollectionTimeMs = averageCollectionTimeMs;
        }
        
        public static GcStatsBuilder builder() {
            return new GcStatsBuilder();
        }
        
        public static class GcStatsBuilder {
            private long totalCollections;
            private long totalCollectionTimeMs;
            private double averageCollectionTimeMs;
            
            public GcStatsBuilder totalCollections(long totalCollections) {
                this.totalCollections = totalCollections;
                return this;
            }
            
            public GcStatsBuilder totalCollectionTimeMs(long totalCollectionTimeMs) {
                this.totalCollectionTimeMs = totalCollectionTimeMs;
                return this;
            }
            
            public GcStatsBuilder averageCollectionTimeMs(double averageCollectionTimeMs) {
                this.averageCollectionTimeMs = averageCollectionTimeMs;
                return this;
            }
            
            public GcStats build() {
                return new GcStats(totalCollections, totalCollectionTimeMs, averageCollectionTimeMs);
            }
        }
        
        public long getTotalCollections() { return totalCollections; }
        public long getTotalCollectionTimeMs() { return totalCollectionTimeMs; }
        public double getAverageCollectionTimeMs() { return averageCollectionTimeMs; }
    }
}