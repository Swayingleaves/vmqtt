/**
 * 存储序列化器接口
 *
 * @author zhenglin
 * @date 2025/08/09
 */
package com.vmqtt.backend.storage.serializer;

/**
 * 通用存储序列化器接口
 * 用于将对象序列化为字节数组存储到RocksDB中
 * 
 * @param <T> 序列化的对象类型
 */
public interface StorageSerializer<T> {
    
    /**
     * 序列化对象为字节数组
     *
     * @param object 要序列化的对象
     * @return 序列化后的字节数组
     * @throws SerializationException 序列化异常
     */
    byte[] serialize(T object) throws SerializationException;
    
    /**
     * 从字节数组反序列化对象
     *
     * @param bytes 字节数组
     * @return 反序列化后的对象
     * @throws SerializationException 反序列化异常
     */
    T deserialize(byte[] bytes) throws SerializationException;
    
    /**
     * 获取序列化器类型
     *
     * @return 序列化器类型
     */
    String getType();
    
    /**
     * 序列化异常
     */
    class SerializationException extends Exception {
        private static final long serialVersionUID = 1L;
        
        public SerializationException(String message) {
            super(message);
        }
        
        public SerializationException(String message, Throwable cause) {
            super(message, cause);
        }
    }
}