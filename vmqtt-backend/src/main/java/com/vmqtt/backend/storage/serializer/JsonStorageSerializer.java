/**
 * JSON存储序列化器实现
 *
 * @author zhenglin
 * @date 2025/08/09
 */
package com.vmqtt.backend.storage.serializer;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

/**
 * 基于Jackson的JSON序列化器
 * 优化性能配置，支持高并发场景
 */
@Slf4j
@Component
public class JsonStorageSerializer<T> implements StorageSerializer<T> {
    
    private static final ObjectMapper OBJECT_MAPPER;
    
    static {
        OBJECT_MAPPER = new ObjectMapper();
        
        // 性能优化配置
        OBJECT_MAPPER.configure(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS, false);
        OBJECT_MAPPER.configure(SerializationFeature.FAIL_ON_EMPTY_BEANS, false);
        OBJECT_MAPPER.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        OBJECT_MAPPER.configure(DeserializationFeature.ACCEPT_EMPTY_ARRAY_AS_NULL_OBJECT, true);
        OBJECT_MAPPER.configure(DeserializationFeature.ACCEPT_EMPTY_STRING_AS_NULL_OBJECT, true);
        
        // 只序列化非空字段，减少存储空间
        OBJECT_MAPPER.setSerializationInclusion(JsonInclude.Include.NON_NULL);
        
        // 支持Java 8时间类型
        OBJECT_MAPPER.registerModule(new JavaTimeModule());
        
        // 禁用自动检测，提高性能
        OBJECT_MAPPER.configure(SerializationFeature.FAIL_ON_SELF_REFERENCES, false);
    }
    
    private final Class<T> targetType;
    
    /**
     * 构造函数
     *
     * @param targetType 目标类型
     */
    public JsonStorageSerializer(Class<T> targetType) {
        this.targetType = targetType;
    }
    
    @Override
    public byte[] serialize(T object) throws SerializationException {
        if (object == null) {
            return null;
        }
        
        try {
            String json = OBJECT_MAPPER.writeValueAsString(object);
            return json.getBytes(StandardCharsets.UTF_8);
        } catch (JsonProcessingException e) {
            log.error("Failed to serialize object of type {}: {}", 
                     object.getClass().getSimpleName(), e.getMessage());
            throw new SerializationException("Failed to serialize object", e);
        }
    }
    
    @Override
    public T deserialize(byte[] bytes) throws SerializationException {
        if (bytes == null || bytes.length == 0) {
            return null;
        }
        
        try {
            String json = new String(bytes, StandardCharsets.UTF_8);
            return OBJECT_MAPPER.readValue(json, targetType);
        } catch (IOException e) {
            log.error("Failed to deserialize object to type {}: {}", 
                     targetType.getSimpleName(), e.getMessage());
            throw new SerializationException("Failed to deserialize object", e);
        }
    }
    
    @Override
    public String getType() {
        return "JSON";
    }
    
    /**
     * 获取ObjectMapper实例（用于其他需要JSON序列化的场景）
     *
     * @return ObjectMapper实例
     */
    public static ObjectMapper getObjectMapper() {
        return OBJECT_MAPPER;
    }
    
    /**
     * 创建特定类型的序列化器
     *
     * @param targetType 目标类型
     * @param <U> 目标类型参数
     * @return 序列化器实例
     */
    public static <U> JsonStorageSerializer<U> forType(Class<U> targetType) {
        return new JsonStorageSerializer<>(targetType);
    }
    
    /**
     * 直接序列化对象为字节数组（静态方法）
     *
     * @param object 要序列化的对象
     * @return 序列化后的字节数组
     * @throws SerializationException 序列化异常
     */
    public static byte[] serializeToBytes(Object object) throws SerializationException {
        if (object == null) {
            return null;
        }
        
        try {
            String json = OBJECT_MAPPER.writeValueAsString(object);
            return json.getBytes(StandardCharsets.UTF_8);
        } catch (JsonProcessingException e) {
            throw new SerializationException("Failed to serialize object", e);
        }
    }
    
    /**
     * 直接从字节数组反序列化对象（静态方法）
     *
     * @param bytes 字节数组
     * @param targetType 目标类型
     * @param <U> 目标类型参数
     * @return 反序列化后的对象
     * @throws SerializationException 反序列化异常
     */
    public static <U> U deserializeFromBytes(byte[] bytes, Class<U> targetType) throws SerializationException {
        if (bytes == null || bytes.length == 0) {
            return null;
        }
        
        try {
            String json = new String(bytes, StandardCharsets.UTF_8);
            return OBJECT_MAPPER.readValue(json, targetType);
        } catch (IOException e) {
            throw new SerializationException("Failed to deserialize object", e);
        }
    }
    
    /**
     * 计算序列化后的大小（估算）
     *
     * @param object 要序列化的对象
     * @return 序列化后的大小（字节）
     */
    public static int estimateSerializedSize(Object object) {
        if (object == null) {
            return 0;
        }
        
        try {
            byte[] serialized = serializeToBytes(object);
            return serialized != null ? serialized.length : 0;
        } catch (SerializationException e) {
            log.warn("Failed to estimate serialized size for object: {}", e.getMessage());
            return 0;
        }
    }
    
    /**
     * 验证对象是否可以被序列化
     *
     * @param object 要验证的对象
     * @return 是否可以被序列化
     */
    public static boolean isSerializable(Object object) {
        try {
            serializeToBytes(object);
            return true;
        } catch (SerializationException e) {
            return false;
        }
    }
}