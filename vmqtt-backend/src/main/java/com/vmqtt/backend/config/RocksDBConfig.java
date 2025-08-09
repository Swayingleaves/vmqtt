/**
 * RocksDB存储引擎配置
 *
 * @author zhenglin
 * @date 2025/08/09
 */
package com.vmqtt.backend.config;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

/**
 * RocksDB配置类
 * 针对高并发MQTT场景进行优化配置
 */
@Data
@Configuration
@ConfigurationProperties(prefix = "vmqtt.storage.rocksdb")
public class RocksDBConfig {
    
    /**
     * RocksDB数据目录
     */
    private String dataDir = "./data/rocksdb";
    
    /**
     * WAL日志目录
     */
    private String walDir = "./data/wal";
    
    /**
     * 是否创建目录（如果不存在）
     */
    private boolean createIfMissing = true;
    
    /**
     * 是否创建列族（如果不存在）
     */
    private boolean createMissingColumnFamilies = true;
    
    /**
     * 最大打开文件数
     */
    private int maxOpenFiles = 10000;
    
    /**
     * 写缓冲区大小（64MB）
     */
    private long writeBufferSize = 64L * 1024 * 1024;
    
    /**
     * 最大写缓冲区数量
     */
    private int maxWriteBufferNumber = 4;
    
    /**
     * 最小合并写缓冲区数量
     */
    private int minWriteBufferNumberToMerge = 2;
    
    /**
     * 块缓存大小（256MB）
     */
    private long blockCacheSize = 256L * 1024 * 1024;
    
    /**
     * 块大小（16KB）
     */
    private long blockSize = 16 * 1024;
    
    /**
     * 块重启间隔
     */
    private int blockRestartInterval = 16;
    
    /**
     * 是否启用布隆过滤器
     */
    private boolean enableBloomFilter = true;
    
    /**
     * 布隆过滤器位数/键
     */
    private double bloomFilterBitsPerKey = 10.0;
    
    /**
     * Level 0文件数量慢写触发阈值
     */
    private int level0SlowdownWritesTrigger = 20;
    
    /**
     * Level 0文件数量停止写触发阈值
     */
    private int level0StopWritesTrigger = 36;
    
    /**
     * Level 0到Level 1的压缩触发阈值
     */
    private int level0FileNumCompactionTrigger = 4;
    
    /**
     * 最大后台压缩作业数
     */
    private int maxBackgroundCompactions = 4;
    
    /**
     * 最大后台刷新作业数
     */
    private int maxBackgroundFlushes = 2;
    
    /**
     * 目标文件大小（64MB）
     */
    private long targetFileSizeBase = 64L * 1024 * 1024;
    
    /**
     * 目标文件大小倍数
     */
    private int targetFileSizeMultiplier = 1;
    
    /**
     * 最大字节数（256MB）
     */
    private long maxBytesForLevelBase = 256L * 1024 * 1024;
    
    /**
     * Level字节数倍数
     */
    private double maxBytesForLevelMultiplier = 10.0;
    
    /**
     * 是否启用压缩
     */
    private boolean compressionEnabled = true;
    
    /**
     * 压缩类型（SNAPPY, LZ4, ZSTD等）
     */
    private String compressionType = "SNAPPY";
    
    /**
     * WAL日志大小限制（64MB）
     */
    private long walSizeLimitMb = 64;
    
    /**
     * WAL日志TTL秒数
     */
    private long walTtlSeconds = 3600;
    
    /**
     * 是否启用统计
     */
    private boolean enableStatistics = true;
    
    /**
     * 统计历史大小
     */
    private int statisticsHistorySize = 10;
    
    /**
     * 是否启用write ahead log
     */
    private boolean disableWal = false;
    
    /**
     * 是否同步写入
     */
    private boolean syncWrites = false;
    
    /**
     * 批处理原子写入
     */
    private boolean atomicFlush = true;
    
    /**
     * 是否允许mmap读取
     */
    private boolean allowMmapReads = false;
    
    /**
     * 是否允许mmap写入
     */
    private boolean allowMmapWrites = false;
    
    /**
     * 删除过期文件周期（秒）
     */
    private long deleteObsoleteFilesPeriodMicros = 6 * 60 * 60 * 1000000L; // 6小时
    
    /**
     * 最大manifest文件大小（64MB）
     */
    private long maxManifestFileSize = 64L * 1024 * 1024;
    
    /**
     * 表缓存元素数量去除计数
     */
    private int tableCacheNumshardbits = 6;
    
    /**
     * 是否启用Pipeline写入
     */
    private boolean enablePipelinedWrite = true;
    
    /**
     * 是否启用写线程自适应产出
     */
    private boolean enableWriteThreadAdaptiveYield = true;
    
    /**
     * 压缩优先级
     */
    private String compactionPriority = "kMinOverlappingRatio";
    
    /**
     * 报告后台IO统计
     */
    private boolean reportBgIoStats = true;
    
    /**
     * TTL天数（用于数据自动清理）
     */
    private int defaultTtlDays = 30;
    
    /**
     * 快照保留天数
     */
    private int snapshotRetentionDays = 7;
    
    /**
     * 是否启用增量备份
     */
    private boolean enableIncrementalBackup = true;
    
    /**
     * 备份目录
     */
    private String backupDir = "./data/backup";
    
    /**
     * 压缩样式
     */
    private String compactionStyle = "LEVEL";
    
    /**
     * 是否优化点查询
     */
    private boolean optimizeForPointLookup = false;
    
    /**
     * 内存预算（用于优化内存使用）
     */
    private long memoryBudgetBytes = 512L * 1024 * 1024; // 512MB
    
    // ================== 列族配置 ==================
    
    /**
     * 会话列族配置
     */
    private ColumnFamilyConfig sessionCf = new ColumnFamilyConfig();
    
    /**
     * 消息列族配置
     */
    private ColumnFamilyConfig messageCf = new ColumnFamilyConfig();
    
    /**
     * 订阅列族配置
     */
    private ColumnFamilyConfig subscriptionCf = new ColumnFamilyConfig();
    
    /**
     * 保留消息列族配置
     */
    private ColumnFamilyConfig retainedCf = new ColumnFamilyConfig();
    
    /**
     * QoS状态列族配置
     */
    private ColumnFamilyConfig qosCf = new ColumnFamilyConfig();
    
    /**
     * 列族配置内部类
     */
    @Data
    public static class ColumnFamilyConfig {
        
        /**
         * 写缓冲区大小
         */
        private Long writeBufferSize;
        
        /**
         * 最大写缓冲区数量
         */
        private Integer maxWriteBufferNumber;
        
        /**
         * 压缩类型
         */
        private String compressionType;
        
        /**
         * 是否启用布隆过滤器
         */
        private Boolean enableBloomFilter;
        
        /**
         * TTL秒数
         */
        private Long ttlSeconds;
        
        /**
         * 是否启用压缩过滤器
         */
        private Boolean enableCompactionFilter;
        
        /**
         * 目标文件大小
         */
        private Long targetFileSizeBase;
        
        /**
         * 最大字节数
         */
        private Long maxBytesForLevelBase;
        
        /**
         * 自定义属性
         */
        private java.util.Map<String, Object> customProperties = new java.util.HashMap<>();
    }
    
    // ================== 性能调优配置 ==================
    
    /**
     * 性能模式：BALANCED, THROUGHPUT, LATENCY
     */
    private String performanceMode = "BALANCED";
    
    /**
     * 预分配写缓冲区
     */
    private boolean preAllocateWriteBuffers = true;
    
    /**
     * 使用直接IO读取
     */
    private boolean useDirectIoForFlushAndCompaction = false;
    
    /**
     * 使用直接IO读取
     */
    private boolean useDirectReads = false;
    
    /**
     * 写入速率限制（字节/秒，0表示无限制）
     */
    private long rateLimitBytesPerSec = 0L;
    
    /**
     * 延迟写入速率（字节/秒）
     */
    private long delayedWriteRate = 2L * 1024 * 1024; // 2MB/s
    
    /**
     * 最大后台压缩队列大小
     */
    private int maxSubcompactions = 1;
    
    /**
     * 获取性能模式特定的配置
     *
     * @return 是否为高吞吐量模式
     */
    public boolean isHighThroughputMode() {
        return "THROUGHPUT".equalsIgnoreCase(performanceMode);
    }
    
    /**
     * 获取性能模式特定的配置
     *
     * @return 是否为低延迟模式
     */
    public boolean isLowLatencyMode() {
        return "LATENCY".equalsIgnoreCase(performanceMode);
    }
    
    /**
     * 获取调整后的写缓冲区大小（基于性能模式）
     *
     * @return 调整后的写缓冲区大小
     */
    public long getAdjustedWriteBufferSize() {
        if (isHighThroughputMode()) {
            return writeBufferSize * 2; // 高吞吐量模式下增大缓冲区
        } else if (isLowLatencyMode()) {
            return writeBufferSize / 2; // 低延迟模式下减小缓冲区
        }
        return writeBufferSize;
    }
    
    /**
     * 获取调整后的最大写缓冲区数量
     *
     * @return 调整后的最大写缓冲区数量
     */
    public int getAdjustedMaxWriteBufferNumber() {
        if (isHighThroughputMode()) {
            return Math.max(maxWriteBufferNumber, 6);
        } else if (isLowLatencyMode()) {
            return Math.min(maxWriteBufferNumber, 2);
        }
        return maxWriteBufferNumber;
    }
}