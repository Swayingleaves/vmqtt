/**
 * MQTT协议版本检测器
 *
 * @author zhenglin
 * @date 2025/08/13
 */
package com.vmqtt.common.protocol.codec;

import com.vmqtt.common.protocol.MqttVersion;
import io.netty.buffer.ByteBuf;
import lombok.extern.slf4j.Slf4j;

/**
 * MQTT协议版本检测器
 * 
 * 从CONNECT消息中检测MQTT协议版本
 */
@Slf4j
public class MqttProtocolVersionDetector {
    
    // MQTT固定头部最小长度 (固定头部字节 + 剩余长度字节)
    private static final int MIN_FIXED_HEADER_LENGTH = 2;
    
    // CONNECT消息类型
    private static final byte CONNECT_MESSAGE_TYPE = 0x10;
    
    /**
     * 从字节流中检测MQTT协议版本
     *
     * @param buffer 字节缓冲区
     * @return 检测到的协议版本，如果无法检测返回null
     */
    public static MqttVersion detectVersion(ByteBuf buffer) {
        if (buffer == null || buffer.readableBytes() < MIN_FIXED_HEADER_LENGTH) {
            return null;
        }
        
        // 保存原始读取位置
        int originalReaderIndex = buffer.readerIndex();
        
        try {
            // 检查是否为CONNECT消息
            byte firstByte = buffer.getByte(originalReaderIndex);
            if ((firstByte & 0xF0) != CONNECT_MESSAGE_TYPE) {
                log.debug("非CONNECT消息，无法检测协议版本，消息类型: 0x{}", 
                    Integer.toHexString(firstByte & 0xFF));
                return null;
            }
            
            // 解析剩余长度
            int remainingLength = decodeRemainingLength(buffer, originalReaderIndex + 1);
            if (remainingLength < 0) {
                log.debug("无法解析剩余长度");
                return null;
            }
            
            // 检查缓冲区是否包含足够的数据
            int totalMessageLength = getFixedHeaderLength(buffer, originalReaderIndex + 1) + remainingLength;
            if (buffer.readableBytes() < totalMessageLength) {
                log.debug("数据不完整，需要 {} 字节，但只有 {} 字节", 
                    totalMessageLength, buffer.readableBytes());
                return null;
            }
            
            // 跳过固定头部，读取可变头部
            int currentIndex = originalReaderIndex + getFixedHeaderLength(buffer, originalReaderIndex + 1);
            
            // 读取协议名称长度
            if (currentIndex + 2 > buffer.writerIndex()) {
                return null;
            }
            
            int protocolNameLength = buffer.getUnsignedShort(currentIndex);
            currentIndex += 2;
            
            // 读取协议名称
            if (currentIndex + protocolNameLength > buffer.writerIndex()) {
                return null;
            }
            
            String protocolName = buffer.toString(currentIndex, protocolNameLength, java.nio.charset.StandardCharsets.UTF_8);
            currentIndex += protocolNameLength;
            
            // 读取协议级别
            if (currentIndex >= buffer.writerIndex()) {
                return null;
            }
            
            byte protocolLevel = buffer.getByte(currentIndex);
            
            // 根据协议名称和级别确定版本
            MqttVersion version = determineVersion(protocolName, protocolLevel);
            
            log.debug("检测到MQTT协议版本: {} (协议名称: {}, 协议级别: {})", 
                version, protocolName, protocolLevel & 0xFF);
            
            return version;
            
        } catch (Exception e) {
            log.warn("检测MQTT协议版本时发生异常", e);
            return null;
        }
    }
    
    /**
     * 解码剩余长度
     */
    private static int decodeRemainingLength(ByteBuf buffer, int startIndex) {
        int multiplier = 1;
        int length = 0;
        int currentIndex = startIndex;
        
        do {
            if (currentIndex >= buffer.writerIndex()) {
                return -1; // 数据不足
            }
            
            byte currentByte = buffer.getByte(currentIndex++);
            length += (currentByte & 0x7F) * multiplier;
            multiplier *= 128;
            
            if (multiplier > 128 * 128 * 128) {
                return -1; // 剩余长度过大
            }
            
            if ((currentByte & 0x80) == 0) {
                break;
            }
        } while (true);
        
        return length;
    }
    
    /**
     * 获取固定头部长度
     */
    private static int getFixedHeaderLength(ByteBuf buffer, int remainingLengthStartIndex) {
        int length = 1; // 第一个字节
        int currentIndex = remainingLengthStartIndex;
        
        do {
            if (currentIndex >= buffer.writerIndex()) {
                return -1;
            }
            
            byte currentByte = buffer.getByte(currentIndex++);
            length++;
            
            if ((currentByte & 0x80) == 0) {
                break;
            }
        } while (length <= 4);
        
        return length;
    }
    
    /**
     * 根据协议名称和级别确定MQTT版本
     */
    private static MqttVersion determineVersion(String protocolName, byte protocolLevel) {
        if ("MQIsdp".equals(protocolName) && protocolLevel == 3) {
            return MqttVersion.MQTT_3_1;
        } else if ("MQTT".equals(protocolName)) {
            return switch (protocolLevel & 0xFF) {
                case 4 -> MqttVersion.MQTT_3_1_1;
                case 5 -> MqttVersion.MQTT_5_0;
                default -> {
                    log.warn("不支持的MQTT协议级别: {}", protocolLevel & 0xFF);
                    yield MqttVersion.MQTT_3_1_1; // 默认回退到3.1.1
                }
            };
        } else {
            log.warn("未知的MQTT协议名称: {}", protocolName);
            return MqttVersion.MQTT_3_1_1; // 默认回退到3.1.1
        }
    }
    
    /**
     * 检查是否为CONNECT消息
     *
     * @param buffer 字节缓冲区
     * @return 如果是CONNECT消息返回true
     */
    public static boolean isConnectMessage(ByteBuf buffer) {
        if (buffer == null || buffer.readableBytes() < 1) {
            return false;
        }
        
        byte firstByte = buffer.getByte(buffer.readerIndex());
        return (firstByte & 0xF0) == CONNECT_MESSAGE_TYPE;
    }
    
    /**
     * 获取支持的默认版本
     *
     * @return 默认的MQTT版本
     */
    public static MqttVersion getDefaultVersion() {
        return MqttVersion.MQTT_3_1_1;
    }
    
    /**
     * 检查版本是否受支持
     *
     * @param version 要检查的版本
     * @return 如果支持返回true
     */
    public static boolean isVersionSupported(MqttVersion version) {
        return version != null && MqttCodecFactory.isVersionSupported(version);
    }
}