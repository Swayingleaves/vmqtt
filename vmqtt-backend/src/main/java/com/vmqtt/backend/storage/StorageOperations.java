/**
 * 存储操作助手类
 *
 * @author zhenglin
 * @date 2025/08/09
 */
package com.vmqtt.backend.storage;

import com.vmqtt.backend.storage.engine.RocksDBStorageEngine;
import com.vmqtt.backend.storage.serializer.JsonStorageSerializer;
import com.vmqtt.backend.storage.serializer.StorageSerializer;
import lombok.extern.slf4j.Slf4j;
import org.rocksdb.RocksDBException;
import org.rocksdb.WriteBatch;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

/**
 * 存储操作助手类
 * 提供类型安全的存储操作接口
 */
@Slf4j
@Component
public class StorageOperations {
    
    @Autowired
    private RocksDBStorageEngine storageEngine;
    
    /**
     * 存储对象
     *
     * @param cfName 列族名称
     * @param key 键
     * @param value 值对象
     * @param <T> 值类型
     * @throws StorageException 存储异常
     */
    public <T> void put(String cfName, String key, T value) throws StorageException {
        try {
            if (value == null) {
                delete(cfName, key);
                return;
            }
            
            byte[] serializedValue = JsonStorageSerializer.serializeToBytes(value);
            storageEngine.put(cfName, key, serializedValue);
            
            log.debug("Put object to {}: key={}, type={}", cfName, key, value.getClass().getSimpleName());
        } catch (Exception e) {
            log.error("Failed to put object: cfName={}, key={}", cfName, key, e);
            throw new StorageException("Failed to put object", e);
        }
    }
    
    /**
     * 获取对象
     *
     * @param cfName 列族名称
     * @param key 键
     * @param valueType 值类型
     * @param <T> 值类型
     * @return 对象Optional
     * @throws StorageException 存储异常
     */
    public <T> Optional<T> get(String cfName, String key, Class<T> valueType) throws StorageException {
        try {
            byte[] serializedValue = storageEngine.get(cfName, key);
            if (serializedValue == null) {
                return Optional.empty();
            }
            
            T value = JsonStorageSerializer.deserializeFromBytes(serializedValue, valueType);
            log.debug("Get object from {}: key={}, type={}, found={}", 
                     cfName, key, valueType.getSimpleName(), value != null);
            
            return Optional.ofNullable(value);
        } catch (Exception e) {
            log.error("Failed to get object: cfName={}, key={}, type={}", 
                     cfName, key, valueType.getSimpleName(), e);
            throw new StorageException("Failed to get object", e);
        }
    }
    
    /**
     * 删除对象
     *
     * @param cfName 列族名称
     * @param key 键
     * @throws StorageException 存储异常
     */
    public void delete(String cfName, String key) throws StorageException {
        try {
            storageEngine.delete(cfName, key);
            log.debug("Delete object from {}: key={}", cfName, key);
        } catch (RocksDBException e) {
            log.error("Failed to delete object: cfName={}, key={}", cfName, key, e);
            throw new StorageException("Failed to delete object", e);
        }
    }
    
    /**
     * 检查键是否存在
     *
     * @param cfName 列族名称
     * @param key 键
     * @return 是否存在
     * @throws StorageException 存储异常
     */
    public boolean exists(String cfName, String key) throws StorageException {
        try {
            boolean exists = storageEngine.exists(cfName, key);
            log.debug("Check existence in {}: key={}, exists={}", cfName, key, exists);
            return exists;
        } catch (RocksDBException e) {
            log.error("Failed to check existence: cfName={}, key={}", cfName, key, e);
            throw new StorageException("Failed to check existence", e);
        }
    }
    
    /**
     * 异步存储对象
     *
     * @param cfName 列族名称
     * @param key 键
     * @param value 值对象
     * @param <T> 值类型
     * @return CompletableFuture
     */
    public <T> CompletableFuture<Void> putAsync(String cfName, String key, T value) {
        return CompletableFuture.runAsync(() -> {
            try {
                put(cfName, key, value);
            } catch (StorageException e) {
                throw new RuntimeException(e);
            }
        });
    }
    
    /**
     * 异步获取对象
     *
     * @param cfName 列族名称
     * @param key 键
     * @param valueType 值类型
     * @param <T> 值类型
     * @return CompletableFuture<Optional<T>>
     */
    public <T> CompletableFuture<Optional<T>> getAsync(String cfName, String key, Class<T> valueType) {
        return CompletableFuture.supplyAsync(() -> {
            try {
                return get(cfName, key, valueType);
            } catch (StorageException e) {
                throw new RuntimeException(e);
            }
        });
    }
    
    /**
     * 扫描对象
     *
     * @param cfName 列族名称
     * @param startKey 开始键
     * @param endKey 结束键
     * @param limit 限制数量
     * @param valueType 值类型
     * @param <T> 值类型
     * @return 对象列表
     * @throws StorageException 存储异常
     */
    public <T> List<KeyValuePair<T>> scan(String cfName, String startKey, String endKey, 
                                         int limit, Class<T> valueType) throws StorageException {
        try {
            List<RocksDBStorageEngine.KeyValue> keyValues = 
                storageEngine.scan(cfName, startKey, endKey, limit);
            
            return keyValues.stream()
                    .map(kv -> {
                        try {
                            T value = JsonStorageSerializer.deserializeFromBytes(kv.getValue(), valueType);
                            return new KeyValuePair<>(kv.getKey(), value);
                        } catch (Exception e) {
                            log.warn("Failed to deserialize value for key {}: {}", kv.getKey(), e.getMessage());
                            return null;
                        }
                    })
                    .filter(kvp -> kvp != null && kvp.getValue() != null)
                    .collect(Collectors.toList());
        } catch (RocksDBException e) {
            log.error("Failed to scan: cfName={}, startKey={}, endKey={}", cfName, startKey, endKey, e);
            throw new StorageException("Failed to scan", e);
        }
    }
    
    /**
     * 前缀扫描
     *
     * @param cfName 列族名称
     * @param prefix 前缀
     * @param limit 限制数量
     * @param valueType 值类型
     * @param <T> 值类型
     * @return 对象列表
     * @throws StorageException 存储异常
     */
    public <T> List<KeyValuePair<T>> scanByPrefix(String cfName, String prefix, 
                                                 int limit, Class<T> valueType) throws StorageException {
        try {
            List<RocksDBStorageEngine.KeyValue> keyValues = 
                storageEngine.scanByPrefix(cfName, prefix, limit);
            
            return keyValues.stream()
                    .map(kv -> {
                        try {
                            T value = JsonStorageSerializer.deserializeFromBytes(kv.getValue(), valueType);
                            return new KeyValuePair<>(kv.getKey(), value);
                        } catch (Exception e) {
                            log.warn("Failed to deserialize value for key {}: {}", kv.getKey(), e.getMessage());
                            return null;
                        }
                    })
                    .filter(kvp -> kvp != null && kvp.getValue() != null)
                    .collect(Collectors.toList());
        } catch (RocksDBException e) {
            log.error("Failed to scan by prefix: cfName={}, prefix={}", cfName, prefix, e);
            throw new StorageException("Failed to scan by prefix", e);
        }
    }
    
    /**
     * 批量操作构建器
     *
     * @return 批量操作构建器
     */
    public BatchOperationBuilder batchOperation() {
        return new BatchOperationBuilder();
    }
    
    /**
     * 获取近似键数量
     *
     * @param cfName 列族名称
     * @return 近似键数量
     * @throws StorageException 存储异常
     */
    public long getApproximateKeyCount(String cfName) throws StorageException {
        try {
            return storageEngine.getApproximateKeyCount(cfName);
        } catch (RocksDBException e) {
            log.error("Failed to get key count: cfName={}", cfName, e);
            throw new StorageException("Failed to get key count", e);
        }
    }
    
    /**
     * 键值对类
     */
    public static class KeyValuePair<T> {
        private final String key;
        private final T value;
        
        public KeyValuePair(String key, T value) {
            this.key = key;
            this.value = value;
        }
        
        public String getKey() { return key; }
        public T getValue() { return value; }
        
        @Override
        public String toString() {
            return String.format("KeyValuePair{key='%s', value=%s}", key, value);
        }
    }
    
    /**
     * 批量操作构建器
     */
    public class BatchOperationBuilder {
        private final WriteBatch batch;
        
        public BatchOperationBuilder() {
            this.batch = new WriteBatch();
        }
        
        /**
         * 添加写入操作
         */
        public <T> BatchOperationBuilder put(String cfName, String key, T value) {
            try {
                if (value != null) {
                    byte[] serializedValue = JsonStorageSerializer.serializeToBytes(value);
                    batch.put(storageEngine.getColumnFamilyHandle(cfName), key.getBytes(), serializedValue);
                } else {
                    delete(cfName, key);
                }
            } catch (Exception e) {
                log.error("Failed to add put operation to batch: cfName={}, key={}", cfName, key, e);
                throw new RuntimeException("Failed to add put operation", e);
            }
            return this;
        }
        
        /**
         * 添加删除操作
         */
        public BatchOperationBuilder delete(String cfName, String key) {
            try {
                batch.delete(storageEngine.getColumnFamilyHandle(cfName), key.getBytes());
            } catch (Exception e) {
                log.error("Failed to add delete operation to batch: cfName={}, key={}", cfName, key, e);
                throw new RuntimeException("Failed to add delete operation", e);
            }
            return this;
        }
        
        /**
         * 执行批量操作
         */
        public void execute() throws StorageException {
            try {
                storageEngine.writeBatch(batch);
                log.debug("Executed batch operation with {} operations", batch.count());
            } catch (RocksDBException e) {
                log.error("Failed to execute batch operation", e);
                throw new StorageException("Failed to execute batch operation", e);
            } finally {
                batch.close();
            }
        }
        
        /**
         * 获取操作数量
         */
        public int getOperationCount() {
            return batch.count();
        }
    }
    
    /**
     * 存储异常
     */
    public static class StorageException extends Exception {
        private static final long serialVersionUID = 1L;
        
        public StorageException(String message) {
            super(message);
        }
        
        public StorageException(String message, Throwable cause) {
            super(message, cause);
        }
    }
}