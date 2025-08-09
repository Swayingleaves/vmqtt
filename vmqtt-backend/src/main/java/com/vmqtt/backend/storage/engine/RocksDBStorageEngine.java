/**
 * RocksDB存储引擎核心实现
 *
 * @author zhenglin
 * @date 2025/08/09
 */
package com.vmqtt.backend.storage.engine;

import com.vmqtt.backend.config.RocksDBConfig;
import com.vmqtt.backend.storage.StorageKeyspace;
import lombok.extern.slf4j.Slf4j;
import org.rocksdb.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import java.io.File;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * RocksDB存储引擎实现
 * 提供高性能的键值存储服务，支持多列族操作
 */
@Slf4j
@Component
public class RocksDBStorageEngine {
    
    @Autowired
    private RocksDBConfig config;
    
    private RocksDB db;
    private Options options;
    private final Map<String, ColumnFamilyHandle> columnFamilyHandles = new ConcurrentHashMap<>();
    private final Map<String, ColumnFamilyDescriptor> columnFamilyDescriptors = new ConcurrentHashMap<>();
    
    private Statistics statistics;
    private final AtomicBoolean initialized = new AtomicBoolean(false);
    
    // 列族名称常量
    public static final String DEFAULT_CF = "default";
    public static final String SESSION_CF = "session";
    public static final String MESSAGE_CF = "message";
    public static final String SUBSCRIPTION_CF = "subscription";
    public static final String RETAINED_CF = "retained";
    public static final String QOS_STATE_CF = "qos_state";
    public static final String METADATA_CF = "metadata";
    
    /**
     * 初始化RocksDB存储引擎
     */
    @PostConstruct
    public void initialize() {
        if (initialized.get()) {
            return;
        }
        
        try {
            log.info("Initializing RocksDB storage engine...");
            
            // 加载RocksDB原生库
            RocksDB.loadLibrary();
            
            // 创建数据目录
            createDataDirectories();
            
            // 配置RocksDB选项
            configureOptions();
            
            // 配置列族
            configureColumnFamilies();
            
            // 打开数据库
            openDatabase();
            
            initialized.set(true);
            log.info("RocksDB storage engine initialized successfully at: {}", config.getDataDir());
            
            // 打印统计信息
            logDatabaseStatistics();
            
        } catch (Exception e) {
            log.error("Failed to initialize RocksDB storage engine", e);
            throw new RuntimeException("RocksDB initialization failed", e);
        }
    }
    
    /**
     * 关闭存储引擎
     */
    @PreDestroy
    public void shutdown() {
        if (!initialized.get()) {
            return;
        }
        
        log.info("Shutting down RocksDB storage engine...");
        
        try {
            // 关闭列族句柄
            columnFamilyHandles.values().forEach(handle -> {
                if (handle != null) {
                    handle.close();
                }
            });
            columnFamilyHandles.clear();
            
            // 关闭数据库
            if (db != null) {
                db.close();
                db = null;
            }
            
            // 关闭选项
            if (options != null) {
                options.close();
                options = null;
            }
            
            // 关闭统计
            if (statistics != null) {
                statistics.close();
                statistics = null;
            }
            
            initialized.set(false);
            log.info("RocksDB storage engine shutdown completed");
            
        } catch (Exception e) {
            log.error("Error during RocksDB shutdown", e);
        }
    }
    
    /**
     * 创建数据目录
     */
    private void createDataDirectories() throws Exception {
        File dataDir = new File(config.getDataDir());
        if (!dataDir.exists()) {
            Files.createDirectories(Paths.get(config.getDataDir()));
        }
        
        File walDir = new File(config.getWalDir());
        if (!walDir.exists()) {
            Files.createDirectories(Paths.get(config.getWalDir()));
        }
        
        File backupDir = new File(config.getBackupDir());
        if (!backupDir.exists()) {
            Files.createDirectories(Paths.get(config.getBackupDir()));
        }
    }
    
    /**
     * 配置RocksDB选项
     */
    private void configureOptions() {
        options = new Options();
        
        // 基础配置
        options.setCreateIfMissing(config.isCreateIfMissing());
        options.setCreateMissingColumnFamilies(config.isCreateMissingColumnFamilies());
        options.setMaxOpenFiles(config.getMaxOpenFiles());
        
        // 写入配置
        options.setWriteBufferSize(config.getAdjustedWriteBufferSize());
        options.setMaxWriteBufferNumber(config.getAdjustedMaxWriteBufferNumber());
        options.setMinWriteBufferNumberToMerge(config.getMinWriteBufferNumberToMerge());
        
        // 压缩配置
        if (config.isCompressionEnabled()) {
            switch (config.getCompressionType().toUpperCase()) {
                case "SNAPPY":
                    options.setCompressionType(CompressionType.SNAPPY_COMPRESSION);
                    break;
                case "LZ4":
                    options.setCompressionType(CompressionType.LZ4_COMPRESSION);
                    break;
                case "ZSTD":
                    options.setCompressionType(CompressionType.ZSTD_COMPRESSION);
                    break;
                default:
                    options.setCompressionType(CompressionType.NO_COMPRESSION);
            }
        } else {
            options.setCompressionType(CompressionType.NO_COMPRESSION);
        }
        
        // 性能配置
        options.setLevel0SlowdownWritesTrigger(config.getLevel0SlowdownWritesTrigger());
        options.setLevel0StopWritesTrigger(config.getLevel0StopWritesTrigger());
        options.setLevel0FileNumCompactionTrigger(config.getLevel0FileNumCompactionTrigger());
        options.setMaxBackgroundJobs(Math.max(config.getMaxBackgroundCompactions(), config.getMaxBackgroundFlushes()));
        options.setTargetFileSizeBase(config.getTargetFileSizeBase());
        options.setTargetFileSizeMultiplier(config.getTargetFileSizeMultiplier());
        options.setMaxBytesForLevelBase(config.getMaxBytesForLevelBase());
        options.setMaxBytesForLevelMultiplier(config.getMaxBytesForLevelMultiplier());
        
        // WAL配置  
        options.setWalTtlSeconds(config.getWalTtlSeconds());
        options.setUseDirectReads(config.isUseDirectReads());
        options.setUseDirectIoForFlushAndCompaction(config.isUseDirectIoForFlushAndCompaction());
        
        // 高级配置
        options.setAtomicFlush(config.isAtomicFlush());
        options.setAllowMmapReads(config.isAllowMmapReads());
        options.setAllowMmapWrites(config.isAllowMmapWrites());
        options.setDeleteObsoleteFilesPeriodMicros(config.getDeleteObsoleteFilesPeriodMicros());
        options.setMaxManifestFileSize(config.getMaxManifestFileSize());
        options.setEnablePipelinedWrite(config.isEnablePipelinedWrite());
        options.setEnableWriteThreadAdaptiveYield(config.isEnableWriteThreadAdaptiveYield());
        
        // 统计配置
        if (config.isEnableStatistics()) {
            statistics = new Statistics();
            options.setStatistics(statistics);
        }
        
        // 块缓存配置
        BlockBasedTableConfig tableConfig = new BlockBasedTableConfig();
        tableConfig.setBlockSize(config.getBlockSize());
        tableConfig.setBlockRestartInterval(config.getBlockRestartInterval());
        
        // 设置块缓存
        Cache cache = new LRUCache(config.getBlockCacheSize());
        tableConfig.setBlockCache(cache);
        
        if (config.isEnableBloomFilter()) {
            tableConfig.setFilterPolicy(new BloomFilter(config.getBloomFilterBitsPerKey()));
        }
        
        options.setTableFormatConfig(tableConfig);
        
        // WAL目录
        if (!config.getWalDir().equals(config.getDataDir())) {
            options.setWalDir(config.getWalDir());
        }
        
        log.info("RocksDB options configured with write buffer size: {}MB, block cache: {}MB",
                config.getAdjustedWriteBufferSize() / (1024 * 1024),
                config.getBlockCacheSize() / (1024 * 1024));
    }
    
    /**
     * 配置列族
     */
    private void configureColumnFamilies() {
        // 创建列族描述符
        List<String> cfNames = Arrays.asList(
                DEFAULT_CF, SESSION_CF, MESSAGE_CF, SUBSCRIPTION_CF, 
                RETAINED_CF, QOS_STATE_CF, METADATA_CF
        );
        
        for (String cfName : cfNames) {
            ColumnFamilyOptions cfOptions = createColumnFamilyOptions(cfName);
            ColumnFamilyDescriptor cfDescriptor = new ColumnFamilyDescriptor(cfName.getBytes(), cfOptions);
            columnFamilyDescriptors.put(cfName, cfDescriptor);
        }
    }
    
    /**
     * 创建列族选项
     */
    private ColumnFamilyOptions createColumnFamilyOptions(String cfName) {
        ColumnFamilyOptions cfOptions = new ColumnFamilyOptions();
        
        // 基础配置
        cfOptions.setWriteBufferSize(config.getAdjustedWriteBufferSize());
        cfOptions.setMaxWriteBufferNumber(config.getAdjustedMaxWriteBufferNumber());
        cfOptions.setMinWriteBufferNumberToMerge(config.getMinWriteBufferNumberToMerge());
        
        // 压缩配置
        if (config.isCompressionEnabled()) {
            switch (config.getCompressionType().toUpperCase()) {
                case "SNAPPY":
                    cfOptions.setCompressionType(CompressionType.SNAPPY_COMPRESSION);
                    break;
                case "LZ4":
                    cfOptions.setCompressionType(CompressionType.LZ4_COMPRESSION);
                    break;
                case "ZSTD":
                    cfOptions.setCompressionType(CompressionType.ZSTD_COMPRESSION);
                    break;
                default:
                    cfOptions.setCompressionType(CompressionType.NO_COMPRESSION);
            }
        }
        
        // 块表配置
        BlockBasedTableConfig tableConfig = new BlockBasedTableConfig();
        tableConfig.setBlockSize(config.getBlockSize());
        
        // 为每个列族创建独立的缓存
        Cache cfCache = new LRUCache(config.getBlockCacheSize() / 7); // 平均分配给各列族
        tableConfig.setBlockCache(cfCache);
        
        if (config.isEnableBloomFilter()) {
            tableConfig.setFilterPolicy(new BloomFilter(config.getBloomFilterBitsPerKey()));
        }
        
        cfOptions.setTableFormatConfig(tableConfig);
        
        // 特定列族的自定义配置
        customizeColumnFamilyOptions(cfName, cfOptions);
        
        return cfOptions;
    }
    
    /**
     * 自定义列族配置
     */
    private void customizeColumnFamilyOptions(String cfName, ColumnFamilyOptions cfOptions) {
        switch (cfName) {
            case SESSION_CF:
                // 会话数据：中等写入频率，需要范围查询
                cfOptions.setTargetFileSizeBase(32L * 1024 * 1024); // 32MB
                break;
            case MESSAGE_CF:
                // 消息数据：高写入频率，大数据量
                cfOptions.setWriteBufferSize(config.getAdjustedWriteBufferSize() * 2);
                cfOptions.setTargetFileSizeBase(128L * 1024 * 1024); // 128MB
                break;
            case SUBSCRIPTION_CF:
                // 订阅数据：低写入频率，小数据量
                cfOptions.setWriteBufferSize(config.getAdjustedWriteBufferSize() / 2);
                cfOptions.setTargetFileSizeBase(16L * 1024 * 1024); // 16MB
                break;
            case RETAINED_CF:
                // 保留消息：低写入频率，长期存储
                cfOptions.setTargetFileSizeBase(64L * 1024 * 1024); // 64MB
                break;
            case QOS_STATE_CF:
                // QoS状态：高写入频率，小数据量，有TTL
                cfOptions.setWriteBufferSize(config.getAdjustedWriteBufferSize());
                cfOptions.setTargetFileSizeBase(32L * 1024 * 1024); // 32MB
                // 设置TTL（1小时）
                cfOptions.setTtl(3600);
                break;
            case METADATA_CF:
                // 元数据：低写入频率，小数据量
                cfOptions.setWriteBufferSize(config.getAdjustedWriteBufferSize() / 4);
                cfOptions.setTargetFileSizeBase(8L * 1024 * 1024); // 8MB
                break;
        }
    }
    
    /**
     * 打开数据库
     */
    private void openDatabase() throws RocksDBException {
        List<ColumnFamilyDescriptor> cfDescriptors = new ArrayList<>(columnFamilyDescriptors.values());
        List<ColumnFamilyHandle> cfHandles = new ArrayList<>();
        
        // 创建DBOptions和ColumnFamilyOptions
        try (DBOptions dbOptions = new DBOptions(options)) {
            db = RocksDB.open(dbOptions, config.getDataDir(), cfDescriptors, cfHandles);
        }
        
        // 建立列族名称到句柄的映射
        for (int i = 0; i < cfDescriptors.size(); i++) {
            String cfName = new String(cfDescriptors.get(i).getName());
            columnFamilyHandles.put(cfName, cfHandles.get(i));
        }
    }
    
    /**
     * 获取列族句柄
     */
    public ColumnFamilyHandle getColumnFamilyHandle(String cfName) {
        ColumnFamilyHandle handle = columnFamilyHandles.get(cfName);
        if (handle == null) {
            throw new IllegalArgumentException("Unknown column family: " + cfName);
        }
        return handle;
    }
    
    /**
     * 写入键值对
     */
    public void put(String cfName, String key, byte[] value) throws RocksDBException {
        checkInitialized();
        ColumnFamilyHandle cfHandle = getColumnFamilyHandle(cfName);
        db.put(cfHandle, key.getBytes(), value);
    }
    
    /**
     * 读取值
     */
    public byte[] get(String cfName, String key) throws RocksDBException {
        checkInitialized();
        ColumnFamilyHandle cfHandle = getColumnFamilyHandle(cfName);
        return db.get(cfHandle, key.getBytes());
    }
    
    /**
     * 删除键
     */
    public void delete(String cfName, String key) throws RocksDBException {
        checkInitialized();
        ColumnFamilyHandle cfHandle = getColumnFamilyHandle(cfName);
        db.delete(cfHandle, key.getBytes());
    }
    
    /**
     * 批量操作
     */
    public void writeBatch(WriteBatch batch) throws RocksDBException {
        checkInitialized();
        try (WriteOptions writeOptions = new WriteOptions()) {
            writeOptions.setSync(config.isSyncWrites());
            db.write(writeOptions, batch);
        }
    }
    
    /**
     * 异步写入
     */
    public CompletableFuture<Void> putAsync(String cfName, String key, byte[] value) {
        return CompletableFuture.runAsync(() -> {
            try {
                put(cfName, key, value);
            } catch (RocksDBException e) {
                throw new RuntimeException("Failed to put async", e);
            }
        });
    }
    
    /**
     * 范围查询
     */
    public List<KeyValue> scan(String cfName, String startKey, String endKey, int limit) throws RocksDBException {
        checkInitialized();
        List<KeyValue> results = new ArrayList<>();
        
        ColumnFamilyHandle cfHandle = getColumnFamilyHandle(cfName);
        
        try (RocksIterator iterator = db.newIterator(cfHandle)) {
            iterator.seek(startKey.getBytes());
            
            int count = 0;
            while (iterator.isValid() && count < limit) {
                String key = new String(iterator.key());
                
                // 检查是否超出结束范围
                if (endKey != null && key.compareTo(endKey) >= 0) {
                    break;
                }
                
                results.add(new KeyValue(key, iterator.value()));
                iterator.next();
                count++;
            }
        }
        
        return results;
    }
    
    /**
     * 前缀扫描
     */
    public List<KeyValue> scanByPrefix(String cfName, String prefix, int limit) throws RocksDBException {
        checkInitialized();
        List<KeyValue> results = new ArrayList<>();
        
        ColumnFamilyHandle cfHandle = getColumnFamilyHandle(cfName);
        
        try (RocksIterator iterator = db.newIterator(cfHandle)) {
            iterator.seek(prefix.getBytes());
            
            int count = 0;
            while (iterator.isValid() && count < limit) {
                String key = new String(iterator.key());
                
                // 检查前缀匹配
                if (!key.startsWith(prefix)) {
                    break;
                }
                
                results.add(new KeyValue(key, iterator.value()));
                iterator.next();
                count++;
            }
        }
        
        return results;
    }
    
    /**
     * 检查键是否存在
     */
    public boolean exists(String cfName, String key) throws RocksDBException {
        byte[] value = get(cfName, key);
        return value != null;
    }
    
    /**
     * 获取近似键数量
     */
    public long getApproximateKeyCount(String cfName) throws RocksDBException {
        checkInitialized();
        ColumnFamilyHandle cfHandle = getColumnFamilyHandle(cfName);
        return db.getLongProperty(cfHandle, "rocksdb.estimate-num-keys");
    }
    
    /**
     * 创建快照
     */
    public Snapshot createSnapshot() {
        checkInitialized();
        return db.getSnapshot();
    }
    
    /**
     * 释放快照
     */
    public void releaseSnapshot(Snapshot snapshot) {
        checkInitialized();
        db.releaseSnapshot(snapshot);
    }
    
    /**
     * 强制压缩
     */
    public void compactRange(String cfName) throws RocksDBException {
        checkInitialized();
        ColumnFamilyHandle cfHandle = getColumnFamilyHandle(cfName);
        db.compactRange(cfHandle);
    }
    
    /**
     * 刷新内存表到磁盘
     */
    public void flush(String cfName) throws RocksDBException {
        checkInitialized();
        ColumnFamilyHandle cfHandle = getColumnFamilyHandle(cfName);
        try (FlushOptions flushOptions = new FlushOptions()) {
            flushOptions.setWaitForFlush(true);
            db.flush(flushOptions, cfHandle);
        }
    }
    
    /**
     * 获取统计信息
     */
    public String getStatistics() {
        if (statistics != null) {
            return statistics.toString();
        }
        return "Statistics not enabled";
    }
    
    /**
     * 键值对内部类
     */
    public static class KeyValue {
        private final String key;
        private final byte[] value;
        
        public KeyValue(String key, byte[] value) {
            this.key = key;
            this.value = value;
        }
        
        public String getKey() { return key; }
        public byte[] getValue() { return value; }
    }
    
    /**
     * 检查初始化状态
     */
    private void checkInitialized() {
        if (!initialized.get()) {
            throw new IllegalStateException("RocksDB storage engine not initialized");
        }
    }
    
    /**
     * 记录数据库统计信息
     */
    private void logDatabaseStatistics() {
        if (statistics != null) {
            log.info("RocksDB initialized with statistics enabled");
        }
        
        try {
            for (String cfName : columnFamilyHandles.keySet()) {
                long keyCount = getApproximateKeyCount(cfName);
                log.info("Column family '{}' approximate key count: {}", cfName, keyCount);
            }
        } catch (Exception e) {
            log.warn("Failed to get initial statistics: {}", e.getMessage());
        }
    }
    
    /**
     * 获取数据库实例（用于高级操作）
     */
    public RocksDB getDatabase() {
        checkInitialized();
        return db;
    }
    
    /**
     * 检查存储引擎是否健康
     */
    public boolean isHealthy() {
        try {
            checkInitialized();
            // 尝试读取一个测试键
            get(DEFAULT_CF, "health_check");
            return true;
        } catch (Exception e) {
            log.warn("Health check failed: {}", e.getMessage());
            return false;
        }
    }
}