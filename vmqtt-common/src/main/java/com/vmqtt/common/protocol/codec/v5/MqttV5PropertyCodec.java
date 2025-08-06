/**
 * MQTT 5.0属性编解码器
 *
 * @author zhenglin
 * @date 2025/08/06
 */
package com.vmqtt.common.protocol.codec.v5;

import com.vmqtt.common.protocol.codec.MqttCodecException;
import com.vmqtt.common.protocol.codec.MqttCodecUtil;
import com.vmqtt.common.protocol.property.MqttProperty;
import com.vmqtt.common.protocol.property.MqttPropertyType;
import io.netty.buffer.ByteBuf;

import java.util.*;

/**
 * MQTT 5.0属性编解码器
 * 
 * 负责MQTT 5.0属性的编码和解码
 */
public class MqttV5PropertyCodec {
    
    /**
     * 解码属性
     *
     * @param buffer 字节缓冲区
     * @return 属性映射，如果数据不足返回null
     */
    public static Map<MqttProperty, Object> decodeProperties(ByteBuf buffer) {
        if (!buffer.isReadable()) {
            return Collections.emptyMap();
        }
        
        // 读取属性总长度
        int propertiesLength = MqttCodecUtil.decodeRemainingLength(buffer);
        if (propertiesLength == -1) {
            return null; // 数据不足
        }
        
        if (propertiesLength == 0) {
            return Collections.emptyMap();
        }
        
        if (!MqttCodecUtil.hasEnoughBytes(buffer, propertiesLength)) {
            // 重置读取位置
            buffer.readerIndex(buffer.readerIndex() - getRemainingLengthSize(propertiesLength));
            return null;
        }
        
        Map<MqttProperty, Object> properties = new LinkedHashMap<>();
        int endIndex = buffer.readerIndex() + propertiesLength;
        
        while (buffer.readerIndex() < endIndex) {
            // 读取属性标识符
            int propertyIdentifier = MqttCodecUtil.decodeRemainingLength(buffer);
            if (propertyIdentifier == -1) {
                throw new MqttCodecException("Failed to decode property identifier");
            }
            
            MqttProperty property = MqttProperty.fromIdentifier(propertyIdentifier);
            Object value = decodePropertyValue(buffer, property);
            
            // 处理用户属性（可以有多个）
            if (property == MqttProperty.USER_PROPERTY) {
                @SuppressWarnings("unchecked")
                List<MqttUserProperty> userProperties = (List<MqttUserProperty>) properties.get(property);
                if (userProperties == null) {
                    userProperties = new ArrayList<>();
                    properties.put(property, userProperties);
                }
                userProperties.add((MqttUserProperty) value);
            } else {
                properties.put(property, value);
            }
        }
        
        return properties;
    }
    
    /**
     * 编码属性
     *
     * @param buffer 字节缓冲区
     * @param properties 属性映射
     */
    public static void encodeProperties(ByteBuf buffer, Map<MqttProperty, Object> properties) {
        if (properties == null || properties.isEmpty()) {
            buffer.writeByte(0); // 属性长度为0
            return;
        }
        
        // 先计算属性的总长度
        int propertiesLength = calculatePropertiesLength(properties);
        
        // 编码属性长度
        MqttCodecUtil.encodeRemainingLength(buffer, propertiesLength);
        
        // 编码各个属性
        for (Map.Entry<MqttProperty, Object> entry : properties.entrySet()) {
            MqttProperty property = entry.getKey();
            Object value = entry.getValue();
            
            if (property == MqttProperty.USER_PROPERTY) {
                // 用户属性可能有多个
                @SuppressWarnings("unchecked")
                List<MqttUserProperty> userProperties = (List<MqttUserProperty>) value;
                for (MqttUserProperty userProperty : userProperties) {
                    MqttCodecUtil.encodeRemainingLength(buffer, property.getIdentifier());
                    encodePropertyValue(buffer, property, userProperty);
                }
            } else {
                MqttCodecUtil.encodeRemainingLength(buffer, property.getIdentifier());
                encodePropertyValue(buffer, property, value);
            }
        }
    }
    
    /**
     * 解码属性值
     */
    private static Object decodePropertyValue(ByteBuf buffer, MqttProperty property) {
        return switch (property.getType()) {
            case BYTE -> buffer.readUnsignedByte();
            case TWO_BYTE_INTEGER -> buffer.readUnsignedShort();
            case FOUR_BYTE_INTEGER -> buffer.readUnsignedInt();
            case VARIABLE_BYTE_INTEGER -> (long) MqttCodecUtil.decodeRemainingLength(buffer);
            case BINARY_DATA -> MqttCodecUtil.decodeBinaryData(buffer);
            case UTF8_STRING -> MqttCodecUtil.decodeString(buffer);
            case UTF8_STRING_PAIR -> {
                String name = MqttCodecUtil.decodeString(buffer);
                String value = MqttCodecUtil.decodeString(buffer);
                yield new MqttUserProperty(name, value);
            }
        };
    }
    
    /**
     * 编码属性值
     */
    private static void encodePropertyValue(ByteBuf buffer, MqttProperty property, Object value) {
        switch (property.getType()) {
            case BYTE -> {
                if (value instanceof Number number) {
                    buffer.writeByte(number.intValue());
                } else {
                    throw new MqttCodecException("Expected Number for byte property, got: " + value.getClass());
                }
            }
            case TWO_BYTE_INTEGER -> {
                if (value instanceof Number number) {
                    buffer.writeShort(number.intValue());
                } else {
                    throw new MqttCodecException("Expected Number for two byte integer property, got: " + value.getClass());
                }
            }
            case FOUR_BYTE_INTEGER -> {
                if (value instanceof Number number) {
                    buffer.writeInt((int) number.longValue());
                } else {
                    throw new MqttCodecException("Expected Number for four byte integer property, got: " + value.getClass());
                }
            }
            case VARIABLE_BYTE_INTEGER -> {
                if (value instanceof Number number) {
                    MqttCodecUtil.encodeRemainingLength(buffer, number.intValue());
                } else {
                    throw new MqttCodecException("Expected Number for variable byte integer property, got: " + value.getClass());
                }
            }
            case BINARY_DATA -> {
                if (value instanceof byte[] data) {
                    MqttCodecUtil.encodeBinaryData(buffer, data);
                } else {
                    throw new MqttCodecException("Expected byte[] for binary data property, got: " + value.getClass());
                }
            }
            case UTF8_STRING -> {
                if (value instanceof String string) {
                    MqttCodecUtil.encodeString(buffer, string);
                } else {
                    throw new MqttCodecException("Expected String for UTF-8 string property, got: " + value.getClass());
                }
            }
            case UTF8_STRING_PAIR -> {
                if (value instanceof MqttUserProperty userProperty) {
                    MqttCodecUtil.encodeString(buffer, userProperty.name());
                    MqttCodecUtil.encodeString(buffer, userProperty.value());
                } else {
                    throw new MqttCodecException("Expected MqttUserProperty for UTF-8 string pair property, got: " + value.getClass());
                }
            }
        }
    }
    
    /**
     * 计算属性的总长度
     */
    private static int calculatePropertiesLength(Map<MqttProperty, Object> properties) {
        int length = 0;
        
        for (Map.Entry<MqttProperty, Object> entry : properties.entrySet()) {
            MqttProperty property = entry.getKey();
            Object value = entry.getValue();
            
            if (property == MqttProperty.USER_PROPERTY) {
                @SuppressWarnings("unchecked")
                List<MqttUserProperty> userProperties = (List<MqttUserProperty>) value;
                for (MqttUserProperty userProperty : userProperties) {
                    length += MqttCodecUtil.getRemainingLengthEncodedSize(property.getIdentifier());
                    length += calculatePropertyValueLength(property, userProperty);
                }
            } else {
                length += MqttCodecUtil.getRemainingLengthEncodedSize(property.getIdentifier());
                length += calculatePropertyValueLength(property, value);
            }
        }
        
        return length;
    }
    
    /**
     * 计算属性值的长度
     */
    private static int calculatePropertyValueLength(MqttProperty property, Object value) {
        return switch (property.getType()) {
            case BYTE -> 1;
            case TWO_BYTE_INTEGER -> 2;
            case FOUR_BYTE_INTEGER -> 4;
            case VARIABLE_BYTE_INTEGER -> {
                if (value instanceof Number number) {
                    yield MqttCodecUtil.getRemainingLengthEncodedSize(number.intValue());
                } else {
                    throw new MqttCodecException("Expected Number for variable byte integer property");
                }
            }
            case BINARY_DATA -> {
                if (value instanceof byte[] data) {
                    yield MqttCodecUtil.getBinaryDataEncodedLength(data);
                } else {
                    throw new MqttCodecException("Expected byte[] for binary data property");
                }
            }
            case UTF8_STRING -> {
                if (value instanceof String string) {
                    yield MqttCodecUtil.getStringEncodedLength(string);
                } else {
                    throw new MqttCodecException("Expected String for UTF-8 string property");
                }
            }
            case UTF8_STRING_PAIR -> {
                if (value instanceof MqttUserProperty userProperty) {
                    yield MqttCodecUtil.getStringEncodedLength(userProperty.name()) +
                          MqttCodecUtil.getStringEncodedLength(userProperty.value());
                } else {
                    throw new MqttCodecException("Expected MqttUserProperty for UTF-8 string pair property");
                }
            }
        };
    }
    
    /**
     * 获取剩余长度字段的字节数
     */
    private static int getRemainingLengthSize(int value) {
        return MqttCodecUtil.getRemainingLengthEncodedSize(value);
    }
    
    /**
     * 用户属性记录
     */
    public record MqttUserProperty(String name, String value) {
        public MqttUserProperty {
            if (name == null) {
                throw new IllegalArgumentException("User property name cannot be null");
            }
            if (value == null) {
                throw new IllegalArgumentException("User property value cannot be null");
            }
        }
        
        @Override
        public String toString() {
            return String.format("UserProperty{name='%s', value='%s'}", name, value);
        }
    }
}