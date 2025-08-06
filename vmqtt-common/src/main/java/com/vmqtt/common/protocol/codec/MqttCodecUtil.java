/**
 * MQTT编解码工具类
 *
 * @author zhenglin
 * @date 2025/08/06
 */
package com.vmqtt.common.protocol.codec;

import com.vmqtt.common.protocol.MqttQos;
import com.vmqtt.common.protocol.MqttVersion;
import com.vmqtt.common.protocol.packet.MqttFixedHeader;
import com.vmqtt.common.protocol.packet.MqttPacketType;
import io.netty.buffer.ByteBuf;

import java.nio.charset.StandardCharsets;

/**
 * MQTT协议编解码工具类
 * 
 * 提供MQTT协议编解码的通用功能
 */
public final class MqttCodecUtil {
    
    /**
     * 最大剩余长度值（2^28 - 1）
     */
    public static final int MAX_REMAINING_LENGTH = 268435455;
    
    /**
     * 最大客户端ID长度
     */
    public static final int MAX_CLIENT_ID_LENGTH = 65535;
    
    /**
     * 最大主题名长度
     */
    public static final int MAX_TOPIC_NAME_LENGTH = 65535;
    
    /**
     * UTF-8字符集
     */
    public static final java.nio.charset.Charset UTF8_CHARSET = StandardCharsets.UTF_8;
    
    private MqttCodecUtil() {
        // 工具类，防止实例化
    }
    
    /**
     * 解码固定头部
     *
     * @param buffer 字节缓冲区
     * @return 固定头部，如果数据不足返回null
     */
    public static MqttFixedHeader decodeFixedHeader(ByteBuf buffer) {
        if (!buffer.isReadable()) {
            return null;
        }
        
        // 标记当前读取位置
        buffer.markReaderIndex();
        
        try {
            // 读取第一个字节（包类型和标志）
            byte firstByte = buffer.readByte();
            
            // 解析包类型和标志
            int packetTypeValue = (firstByte & 0xF0) >> 4;
            MqttPacketType packetType = MqttPacketType.fromValue(packetTypeValue);
            
            boolean dupFlag = (firstByte & 0x08) != 0;
            int qosValue = (firstByte & 0x06) >> 1;
            MqttQos qos = MqttQos.fromValue(qosValue);
            boolean retainFlag = (firstByte & 0x01) != 0;
            
            // 解码剩余长度
            int remainingLength = decodeRemainingLength(buffer);
            if (remainingLength == -1) {
                // 数据不足，重置读取位置
                buffer.resetReaderIndex();
                return null;
            }
            
            return new MqttFixedHeader(packetType, dupFlag, qos, retainFlag, remainingLength);
            
        } catch (Exception e) {
            // 发生错误，重置读取位置
            buffer.resetReaderIndex();
            throw new MqttCodecException("Failed to decode fixed header", e);
        }
    }
    
    /**
     * 编码固定头部
     *
     * @param buffer 字节缓冲区
     * @param fixedHeader 固定头部
     */
    public static void encodeFixedHeader(ByteBuf buffer, MqttFixedHeader fixedHeader) {
        // 编码第一个字节
        buffer.writeByte(fixedHeader.getFirstByte());
        
        // 编码剩余长度
        encodeRemainingLength(buffer, fixedHeader.remainingLength());
    }
    
    /**
     * 解码剩余长度
     *
     * @param buffer 字节缓冲区
     * @return 剩余长度，如果数据不足返回-1
     */
    public static int decodeRemainingLength(ByteBuf buffer) {
        int remainingLength = 0;
        int multiplier = 1;
        int bytesRead = 0;
        
        do {
            if (!buffer.isReadable()) {
                return -1; // 数据不足
            }
            
            byte encodedByte = buffer.readByte();
            remainingLength += (encodedByte & 0x7F) * multiplier;
            
            if (multiplier > 128 * 128 * 128) {
                throw new MqttCodecException("Remaining length exceeds maximum of 4 bytes");
            }
            
            multiplier *= 128;
            bytesRead++;
            
            if (bytesRead > 4) {
                throw new MqttCodecException("Remaining length field too long");
            }
            
            if ((encodedByte & 0x80) == 0) {
                break;
            }
            
        } while (true);
        
        if (remainingLength < 0) {
            throw new MqttCodecException("Remaining length is negative");
        }
        
        if (remainingLength > MAX_REMAINING_LENGTH) {
            throw new MqttCodecException("Remaining length exceeds maximum value");
        }
        
        return remainingLength;
    }
    
    /**
     * 编码剩余长度
     *
     * @param buffer 字节缓冲区
     * @param remainingLength 剩余长度
     */
    public static void encodeRemainingLength(ByteBuf buffer, int remainingLength) {
        if (remainingLength < 0) {
            throw new IllegalArgumentException("Remaining length cannot be negative");
        }
        
        if (remainingLength > MAX_REMAINING_LENGTH) {
            throw new IllegalArgumentException("Remaining length exceeds maximum value");
        }
        
        do {
            int digit = remainingLength % 128;
            remainingLength /= 128;
            
            if (remainingLength > 0) {
                digit |= 0x80;
            }
            
            buffer.writeByte(digit);
            
        } while (remainingLength > 0);
    }
    
    /**
     * 计算剩余长度编码后的字节数
     *
     * @param remainingLength 剩余长度
     * @return 编码后的字节数
     */
    public static int getRemainingLengthEncodedSize(int remainingLength) {
        if (remainingLength < 0) {
            throw new IllegalArgumentException("Remaining length cannot be negative");
        }
        
        if (remainingLength == 0) {
            return 1;
        }
        
        int size = 0;
        do {
            remainingLength /= 128;
            size++;
        } while (remainingLength > 0);
        
        return size;
    }
    
    /**
     * 解码UTF-8字符串
     *
     * @param buffer 字节缓冲区
     * @return UTF-8字符串，如果数据不足返回null
     */
    public static String decodeString(ByteBuf buffer) {
        if (buffer.readableBytes() < 2) {
            return null;
        }
        
        int length = buffer.readUnsignedShort();
        if (buffer.readableBytes() < length) {
            buffer.readerIndex(buffer.readerIndex() - 2); // 回退长度字段
            return null;
        }
        
        byte[] bytes = new byte[length];
        buffer.readBytes(bytes);
        
        return new String(bytes, UTF8_CHARSET);
    }
    
    /**
     * 编码UTF-8字符串
     *
     * @param buffer 字节缓冲区
     * @param string 字符串
     */
    public static void encodeString(ByteBuf buffer, String string) {
        if (string == null) {
            buffer.writeShort(0);
            return;
        }
        
        byte[] bytes = string.getBytes(UTF8_CHARSET);
        if (bytes.length > 65535) {
            throw new MqttCodecException("String length exceeds maximum of 65535 bytes");
        }
        
        buffer.writeShort(bytes.length);
        buffer.writeBytes(bytes);
    }
    
    /**
     * 解码二进制数据
     *
     * @param buffer 字节缓冲区
     * @return 二进制数据，如果数据不足返回null
     */
    public static byte[] decodeBinaryData(ByteBuf buffer) {
        if (buffer.readableBytes() < 2) {
            return null;
        }
        
        int length = buffer.readUnsignedShort();
        if (buffer.readableBytes() < length) {
            buffer.readerIndex(buffer.readerIndex() - 2); // 回退长度字段
            return null;
        }
        
        byte[] data = new byte[length];
        buffer.readBytes(data);
        
        return data;
    }
    
    /**
     * 编码二进制数据
     *
     * @param buffer 字节缓冲区
     * @param data 二进制数据
     */
    public static void encodeBinaryData(ByteBuf buffer, byte[] data) {
        if (data == null) {
            buffer.writeShort(0);
            return;
        }
        
        if (data.length > 65535) {
            throw new MqttCodecException("Binary data length exceeds maximum of 65535 bytes");
        }
        
        buffer.writeShort(data.length);
        buffer.writeBytes(data);
    }
    
    /**
     * 解码包标识符
     *
     * @param buffer 字节缓冲区
     * @return 包标识符，如果数据不足返回-1
     */
    public static int decodePacketId(ByteBuf buffer) {
        if (buffer.readableBytes() < 2) {
            return -1;
        }
        
        return buffer.readUnsignedShort();
    }
    
    /**
     * 编码包标识符
     *
     * @param buffer 字节缓冲区
     * @param packetId 包标识符
     */
    public static void encodePacketId(ByteBuf buffer, int packetId) {
        if (packetId < 1 || packetId > 65535) {
            throw new IllegalArgumentException("Packet ID must be between 1 and 65535");
        }
        
        buffer.writeShort(packetId);
    }
    
    /**
     * 检查是否有足够的数据读取指定长度
     *
     * @param buffer 字节缓冲区
     * @param length 所需长度
     * @return 如果有足够数据返回true
     */
    public static boolean hasEnoughBytes(ByteBuf buffer, int length) {
        return buffer.readableBytes() >= length;
    }
    
    /**
     * 验证MQTT版本兼容性
     *
     * @param version MQTT版本
     * @param packetType 包类型
     * @throws MqttCodecException 如果版本不兼容
     */
    public static void validateVersionCompatibility(MqttVersion version, MqttPacketType packetType) {
        if (packetType.isMqtt5Only() && !version.isMqtt5()) {
            throw new MqttCodecException(
                String.format("Packet type %s is only supported in MQTT 5.0", packetType.getName()));
        }
    }
    
    /**
     * 计算字符串编码后的字节长度
     *
     * @param string 字符串
     * @return 编码后的字节长度（包括2字节长度前缀）
     */
    public static int getStringEncodedLength(String string) {
        if (string == null) {
            return 2; // 仅长度前缀
        }
        return 2 + string.getBytes(UTF8_CHARSET).length;
    }
    
    /**
     * 计算二进制数据编码后的字节长度
     *
     * @param data 二进制数据
     * @return 编码后的字节长度（包括2字节长度前缀）
     */
    public static int getBinaryDataEncodedLength(byte[] data) {
        if (data == null) {
            return 2; // 仅长度前缀
        }
        return 2 + data.length;
    }
}