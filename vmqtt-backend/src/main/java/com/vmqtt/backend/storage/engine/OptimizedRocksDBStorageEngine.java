/**
 * 优化的RocksDB存储引擎
 *
 * @author zhenglin
 * @date 2025/08/10
 */
package com.vmqtt.backend.storage.engine;

import com.vmqtt.backend.config.RocksDBConfig;
import lombok.extern.slf4j.Slf4j;
import org.rocksdb.*;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 高性能RocksDB存储引擎实现
 * 针对MQTT场景进行优化，支持高并发读写
 */
@Slf4j
@Component
public class OptimizedRocksDBStorageEngine {
    
    private final RocksDBConfig config;
    private RocksDB rocksDB;
    private Options dbOptions;
    private final Map<String, ColumnFamilyHandle> columnFamilyHandles = new ConcurrentHashMap<>();
    private final List<ColumnFamilyDescriptor> columnFamilyDescriptors = new ArrayList<>();
    
    // 性能统计
    private Statistics statistics;
    private final AtomicLong totalReads = new AtomicLong(0);
    private final AtomicLong totalWrites = new AtomicLong(0);
    private final AtomicLong totalDeletes = new AtomicLong(0);
    
    // 后台任务调度器
    private ScheduledExecutorService scheduler;
    
    // 写入批处理
    private final ThreadLocal<WriteBatch> writeBatchThreadLocal = ThreadLocal.withInitial(() -> new WriteBatch());
    
    public OptimizedRocksDBStorageEngine(RocksDBConfig config) {
        this.config = config;
    }
    
    /**
     * 初始化RocksDB存储引擎
     */
    @PostConstruct
    public void initialize() {
        try {
            log.info("正在初始化优化的RocksDB存储引擎...");
            
            // 加载RocksDB原生库
            RocksDB.loadLibrary();
            
            // 创建数据目录
            createDirectories();
            
            // 创建优化的数据库选项
            createOptimizedDatabaseOptions();
            
            // 定义列族
            defineColumnFamilies();
            
            // 打开数据库
            openDatabase();
            
            // 启动后台监控任务
            startBackgroundTasks();
            
            log.info("RocksDB存储引擎初始化完成");
            logDatabaseInfo();
            
        } catch (Exception e) {
            log.error("RocksDB存储引擎初始化失败", e);
            throw new RuntimeException("存储引擎初始化失败", e);
        }
    }
    
    /**
     * 关闭RocksDB存储引擎
     */
    @PreDestroy
    public void shutdown() {
        log.info("正在关闭RocksDB存储引擎...");
        
        try {
            // 关闭后台任务
            if (scheduler != null) {
                scheduler.shutdown();
                scheduler.awaitTermination(10, TimeUnit.SECONDS);
            }
            
            // 清理WriteBatch
            writeBatchThreadLocal.remove();
            
            // 关闭列族句柄
            for (ColumnFamilyHandle handle : columnFamilyHandles.values()) {
                handle.close();
            }
            columnFamilyHandles.clear();
            
            // 关闭数据库
            if (rocksDB != null) {
                rocksDB.close();
            }
            
            // 关闭选项对象
            if (dbOptions != null) {
                dbOptions.close();
            }
            
            // 关闭统计对象
            if (statistics != null) {
                statistics.close();
            }
            
            log.info("RocksDB存储引擎已关闭");
            
        } catch (Exception e) {
            log.error("关闭RocksDB存储引擎时发生错误", e);
        }
    }
    
    /**
     * 创建必要的目录
     */
    private void createDirectories() {
        try {
            Files.createDirectories(Paths.get(config.getDataDir()));
            Files.createDirectories(Paths.get(config.getWalDir()));
            
            if (config.isEnableIncrementalBackup()) {
                Files.createDirectories(Paths.get(config.getBackupDir()));
            }
            
        } catch (Exception e) {
            throw new RuntimeException("创建数据目录失败", e);
        }
    }
    
    /**
     * 创建优化的数据库选项
     */
    private void createOptimizedDatabaseOptions() {
        try {
            // 创建统计对象
            statistics = new Statistics();
            statistics.setStatsLevel(StatsLevel.EXCEPT_TIME_FOR_MUTEX);
            
            // 创建数据库选项
            dbOptions = new Options();
            
            // 基础配置
            dbOptions.setCreateIfMissing(config.isCreateIfMissing());
            dbOptions.setCreateMissingColumnFamilies(config.isCreateMissingColumnFamilies());
            dbOptions.setStatistics(statistics);
            
            // 内存相关配置
            dbOptions.setWriteBufferSize(config.getAdjustedWriteBufferSize());
            dbOptions.setMaxWriteBufferNumber(config.getAdjustedMaxWriteBufferNumber());
            dbOptions.setMinWriteBufferNumberToMerge(config.getMinWriteBufferNumberToMerge());
            
            // 文件系统配置
            dbOptions.setMaxOpenFiles(config.getMaxOpenFiles());
            dbOptions.setMaxBackgroundCompactions(config.getMaxBackgroundCompactions());
            dbOptions.setMaxBackgroundFlushes(config.getMaxBackgroundFlushes());
            
            // 压缩配置
            if (config.isCompressionEnabled()) {
                dbOptions.setCompressionType(CompressionType.valueOf(config.getCompressionType()));
                dbOptions.setBottommostCompressionType(CompressionType.ZSTD);
            } else {
                dbOptions.setCompressionType(CompressionType.NO_COMPRESSION);
            }
            
            // 块缓存配置
            BlockBasedTableConfig tableConfig = new BlockBasedTableConfig();
            tableConfig.setBlockSize(config.getBlockSize());
            tableConfig.setBlockRestartInterval(config.getBlockRestartInterval());
            
            // 布隆过滤器
            if (config.isEnableBloomFilter()) {
                BloomFilter bloomFilter = new BloomFilter(config.getBloomFilterBitsPerKey());
                tableConfig.setFilterPolicy(bloomFilter);
            }
            
            // 块缓存
            Cache blockCache = new LRUCache(config.getBlockCacheSize());
            tableConfig.setBlockCache(blockCache);
            
            dbOptions.setTableFormatConfig(tableConfig);
            
            // Level相关配置
            dbOptions.setLevel0FileNumCompactionTrigger(config.getLevel0FileNumCompactionTrigger());
            dbOptions.setLevel0SlowdownWritesTrigger(config.getLevel0SlowdownWritesTrigger());
            dbOptions.setLevel0StopWritesTrigger(config.getLevel0StopWritesTrigger());
            dbOptions.setTargetFileSizeBase(config.getTargetFileSizeBase());
            dbOptions.setTargetFileSizeMultiplier(config.getTargetFileSizeMultiplier());
            dbOptions.setMaxBytesForLevelBase(config.getMaxBytesForLevelBase());
            dbOptions.setMaxBytesForLevelMultiplier(config.getMaxBytesForLevelMultiplier());
            
            // WAL配置
            dbOptions.setWalDir(config.getWalDir());
            dbOptions.setWalSizeLimitMB(config.getWalSizeLimitMb());
            dbOptions.setWalTtlSeconds(config.getWalTtlSeconds());
            dbOptions.setDisableWal(config.isDisableWal());
            dbOptions.setSync(config.isSyncWrites());
            
            // 高级性能优化配置
            configureAdvancedPerformanceOptions();
            
            log.info("RocksDB数据库选项配置完成");
            
        } catch (Exception e) {
            throw new RuntimeException("创建数据库选项失败", e);
        }
    }
    
    /**
     * 配置高级性能优化选项
     */
    private void configureAdvancedPerformanceOptions() {
        // 原子刷新
        dbOptions.setAtomicFlush(config.isAtomicFlush());
        
        // 删除过期文件周期
        dbOptions.setDeleteObsoleteFilesPeriodMicros(config.getDeleteObsoleteFilesPeriodMicros());
        
        // 最大manifest文件大小
        dbOptions.setMaxManifestFileSize(config.getMaxManifestFileSize());
        
        // 写入速率限制
        if (config.getRateLimitBytesPerSec() > 0) {
            RateLimiter rateLimiter = new RateLimiter(config.getRateLimitBytesPerSec());
            dbOptions.setRateLimiter(rateLimiter);
        }
        
        // 延迟写入速率
        dbOptions.setDelayedWriteRate(config.getDelayedWriteRate());
        
        // Pipeline写入
        dbOptions.setEnablePipelinedWrite(config.isEnablePipelinedWrite());
        
        // 写线程自适应产出
        dbOptions.setEnableWriteThreadAdaptiveYield(config.isEnableWriteThreadAdaptiveYield());
        
        // 报告后台IO统计
        dbOptions.setReportBgIoStats(config.isReportBgIoStats());
        
        // 根据性能模式调整
        if (config.isHighThroughputMode()) {
            configureHighThroughputMode();
        } else if (config.isLowLatencyMode()) {
            configureLowLatencyMode();
        }
    }
    
    /**
     * 配置高吞吐量模式
     */
    private void configureHighThroughputMode() {
        log.info("启用高吞吐量性能模式");
        
        // 增加写缓冲区
        dbOptions.setWriteBufferSize(config.getWriteBufferSize() * 2);
        dbOptions.setMaxWriteBufferNumber(Math.max(config.getMaxWriteBufferNumber(), 6));
        
        // 增加压缩线程
        dbOptions.setMaxBackgroundCompactions(Math.max(config.getMaxBackgroundCompactions(), 8));
        
        // 延迟压缩以提高写入吞吐量
        dbOptions.setLevel0SlowdownWritesTrigger(config.getLevel0SlowdownWritesTrigger() * 2);
    }
    
    /**
     * 配置低延迟模式
     */
    private void configureLowLatencyMode() {
        log.info("启用低延迟性能模式");
        
        // 减少写缓冲区大小以快速刷新
        dbOptions.setWriteBufferSize(config.getWriteBufferSize() / 2);
        dbOptions.setMaxWriteBufferNumber(Math.min(config.getMaxWriteBufferNumber(), 2));
        
        // 更积极的压缩策略
        dbOptions.setLevel0FileNumCompactionTrigger(2);
        dbOptions.setLevel0SlowdownWritesTrigger(4);
        
        // 禁用WAL以减少延迟（注意：这会影响持久性）
        // dbOptions.setDisableWal(true);
    }
    
    /**
     * 定义列族
     */
    private void defineColumnFamilies() {
        // 默认列族
        columnFamilyDescriptors.add(new ColumnFamilyDescriptor(RocksDB.DEFAULT_COLUMN_FAMILY));
        
        // 会话列族
        columnFamilyDescriptors.add(createColumnFamilyDescriptor("sessions", config.getSessionCf()));
        
        // 消息列族
        columnFamilyDescriptors.add(createColumnFamilyDescriptor("messages", config.getMessageCf()));
        
        // 订阅列族
        columnFamilyDescriptors.add(createColumnFamilyDescriptor("subscriptions", config.getSubscriptionCf()));
        
        // 保留消息列族
        columnFamilyDescriptors.add(createColumnFamilyDescriptor("retained", config.getRetainedCf()));
        
        // QoS状态列族
        columnFamilyDescriptors.add(createColumnFamilyDescriptor("qos_states", config.getQosCf()));
        
        log.info("定义了{}个列族", columnFamilyDescriptors.size());
    }
    
    /**
     * 创建列族描述符
     */
    private ColumnFamilyDescriptor createColumnFamilyDescriptor(String name, RocksDBConfig.ColumnFamilyConfig cfConfig) {
        ColumnFamilyOptions options = new ColumnFamilyOptions();
        
        // 应用列族特定配置
        if (cfConfig.getWriteBufferSize() != null) {
            options.setWriteBufferSize(cfConfig.getWriteBufferSize());
        }
        
        if (cfConfig.getMaxWriteBufferNumber() != null) {
            options.setMaxWriteBufferNumber(cfConfig.getMaxWriteBufferNumber());
        }
        
        if (cfConfig.getCompressionType() != null) {
            options.setCompressionType(CompressionType.valueOf(cfConfig.getCompressionType()));
        }
        
        if (cfConfig.getTargetFileSizeBase() != null) {
            options.setTargetFileSizeBase(cfConfig.getTargetFileSizeBase());
        }
        
        if (cfConfig.getMaxBytesForLevelBase() != null) {
            options.setMaxBytesForLevelBase(cfConfig.getMaxBytesForLevelBase());
        }
        
        // 布隆过滤器配置
        if (Boolean.TRUE.equals(cfConfig.getEnableBloomFilter())) {
            BlockBasedTableConfig tableConfig = new BlockBasedTableConfig();
            BloomFilter bloomFilter = new BloomFilter(config.getBloomFilterBitsPerKey());
            tableConfig.setFilterPolicy(bloomFilter);
            options.setTableFormatConfig(tableConfig);
        }
        
        // TTL支持
        if (cfConfig.getTtlSeconds() != null && cfConfig.getTtlSeconds() > 0) {
            // RocksDB的TTL需要使用TtlDB包装器，这里记录配置
            log.info("列族{}配置了TTL: {}秒", name, cfConfig.getTtlSeconds());
        }
        
        return new ColumnFamilyDescriptor(name.getBytes(), options);
    }
    
    /**
     * 打开数据库
     */
    private void openDatabase() throws RocksDBException {
        List<ColumnFamilyHandle> handles = new ArrayList<>();
        
        rocksDB = RocksDB.open(dbOptions, config.getDataDir(), columnFamilyDescriptors, handles);
        
        // 存储列族句柄
        for (int i = 0; i < columnFamilyDescriptors.size(); i++) {
            String name = new String(columnFamilyDescriptors.get(i).getName());
            columnFamilyHandles.put(name, handles.get(i));
        }
        
        log.info("RocksDB数据库已打开: {}", config.getDataDir());
    }
    
    /**
     * 启动后台监控任务
     */
    private void startBackgroundTasks() {
        scheduler = Executors.newScheduledThreadPool(2);
        
        // 统计信息监控
        scheduler.scheduleAtFixedRate(this::logStatistics, 60, 60, TimeUnit.SECONDS);
        
        // 压缩统计
        scheduler.scheduleAtFixedRate(this::logCompactionStats, 300, 300, TimeUnit.SECONDS);
        
        log.info("后台监控任务已启动");
    }
    
    /**
     * 记录数据库信息
     */
    private void logDatabaseInfo() {
        try {
            log.info("=== RocksDB 数据库信息 ===");
            log.info("数据目录: {}", config.getDataDir());
            log.info("WAL目录: {}", config.getWalDir());
            log.info("性能模式: {}", config.getPerformanceMode());
            log.info("写缓冲区大小: {} MB", config.getAdjustedWriteBufferSize() / 1024 / 1024);
            log.info("块缓存大小: {} MB", config.getBlockCacheSize() / 1024 / 1024);
            log.info("最大打开文件: {}", config.getMaxOpenFiles());
            log.info("压缩类型: {}", config.getCompressionType());
            log.info("布隆过滤器: {}", config.isEnableBloomFilter() ? "启用" : "禁用");
            log.info("列族数量: {}", columnFamilyHandles.size());
            
            for (String cfName : columnFamilyHandles.keySet()) {
                log.info("  - {}", cfName);
            }
            
        } catch (Exception e) {
            log.warn("记录数据库信息时发生错误", e);
        }
    }
    
    /**
     * 记录统计信息
     */
    private void logStatistics() {
        try {
            if (statistics == null) return;
            
            log.info("=== RocksDB 性能统计 ===");
            log.info("读操作总数: {}", totalReads.get());
            log.info("写操作总数: {}", totalWrites.get());
            log.info("删除操作总数: {}", totalDeletes.get());
            
            // 获取RocksDB内部统计
            String stats = rocksDB.getProperty("rocksdb.stats");
            if (stats != null && !stats.isEmpty()) {
                log.debug("RocksDB内部统计:\n{}", stats);
            }
            
        } catch (Exception e) {
            log.warn("记录统计信息时发生错误", e);
        }
    }
    
    /**
     * 记录压缩统计
     */
    private void logCompactionStats() {
        try {
            String compactionStats = rocksDB.getProperty("rocksdb.compaction-stats");
            if (compactionStats != null && !compactionStats.isEmpty()) {
                log.info("=== RocksDB 压缩统计 ===\n{}", compactionStats);
            }
            
        } catch (Exception e) {
            log.warn("记录压缩统计时发生错误", e);
        }
    }
    
    // ================== 数据操作API ==================
    
    /**
     * 获取列族句柄
     */
    public ColumnFamilyHandle getColumnFamilyHandle(String columnFamily) {
        return columnFamilyHandles.get(columnFamily);
    }
    
    /**
     * 写入数据
     */
    public void put(String columnFamily, byte[] key, byte[] value) throws RocksDBException {
        ColumnFamilyHandle handle = getColumnFamilyHandle(columnFamily);
        if (handle == null) {
            throw new IllegalArgumentException("未知的列族: " + columnFamily);
        }
        
        rocksDB.put(handle, key, value);
        totalWrites.incrementAndGet();
    }
    
    /**
     * 读取数据
     */
    public byte[] get(String columnFamily, byte[] key) throws RocksDBException {
        ColumnFamilyHandle handle = getColumnFamilyHandle(columnFamily);
        if (handle == null) {
            throw new IllegalArgumentException("未知的列族: " + columnFamily);
        }
        
        byte[] result = rocksDB.get(handle, key);
        totalReads.incrementAndGet();
        return result;
    }
    
    /**
     * 删除数据
     */
    public void delete(String columnFamily, byte[] key) throws RocksDBException {
        ColumnFamilyHandle handle = getColumnFamilyHandle(columnFamily);
        if (handle == null) {
            throw new IllegalArgumentException("未知的列族: " + columnFamily);
        }
        
        rocksDB.delete(handle, key);
        totalDeletes.incrementAndGet();
    }
    
    /**
     * 批量写入
     */
    public void batchWrite(List<WriteOperation> operations) throws RocksDBException {
        try (WriteBatch batch = new WriteBatch()) {
            for (WriteOperation op : operations) {
                ColumnFamilyHandle handle = getColumnFamilyHandle(op.getColumnFamily());
                if (handle == null) {
                    continue;
                }
                
                switch (op.getType()) {
                    case PUT:
                        batch.put(handle, op.getKey(), op.getValue());
                        totalWrites.incrementAndGet();
                        break;
                    case DELETE:
                        batch.delete(handle, op.getKey());
                        totalDeletes.incrementAndGet();
                        break;
                }
            }
            
            rocksDB.write(new WriteOptions(), batch);
        }
    }
    
    /**
     * 获取迭代器
     */
    public RocksIterator newIterator(String columnFamily) {
        ColumnFamilyHandle handle = getColumnFamilyHandle(columnFamily);
        if (handle == null) {
            throw new IllegalArgumentException("未知的列族: " + columnFamily);
        }
        
        return rocksDB.newIterator(handle);
    }
    
    /**
     * 获取RocksDB实例（用于高级操作）
     */
    public RocksDB getRocksDB() {
        return rocksDB;
    }
    
    /**
     * 写操作类型
     */
    public enum WriteOperationType {
        PUT, DELETE
    }
    
    /**
     * 写操作封装
     */
    public static class WriteOperation {
        private final String columnFamily;
        private final WriteOperationType type;
        private final byte[] key;
        private final byte[] value;
        
        public WriteOperation(String columnFamily, WriteOperationType type, byte[] key, byte[] value) {
            this.columnFamily = columnFamily;
            this.type = type;
            this.key = key;
            this.value = value;
        }
        
        public WriteOperation(String columnFamily, WriteOperationType type, byte[] key) {
            this(columnFamily, type, key, null);
        }
        
        // Getters
        public String getColumnFamily() { return columnFamily; }
        public WriteOperationType getType() { return type; }
        public byte[] getKey() { return key; }
        public byte[] getValue() { return value; }
    }
}