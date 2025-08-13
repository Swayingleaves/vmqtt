/**
 * MQTT协议错误处理器
 *
 * @author zhenglin
 * @date 2025/08/13
 */
package com.vmqtt.common.protocol.error;

import com.vmqtt.common.protocol.MqttVersion;
import com.vmqtt.common.protocol.codec.MqttProtocolVersionManager;
import com.vmqtt.common.protocol.codec.MqttProtocolVersionDetector;
import com.vmqtt.common.protocol.packet.connack.MqttConnackPacket;
import com.vmqtt.common.protocol.packet.connack.MqttConnectReturnCode;
import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.handler.codec.DecoderException;
import io.netty.handler.codec.EncoderException;
import io.netty.handler.codec.TooLongFrameException;
import lombok.extern.slf4j.Slf4j;

/**
 * MQTT协议错误处理器
 * 
 * 处理各种MQTT协议错误和异常情况
 */
@Slf4j
public class MqttProtocolErrorHandler {
    
    // 错误类型枚举
    public enum ErrorType {
        PROTOCOL_VERSION_NOT_SUPPORTED,
        MALFORMED_PACKET,
        PACKET_TOO_LARGE,
        DECODE_ERROR,
        ENCODE_ERROR,
        CONNECTION_TIMEOUT,
        AUTHENTICATION_FAILED,
        AUTHORIZATION_FAILED,
        INVALID_CLIENT_ID,
        SERVER_UNAVAILABLE,
        UNKNOWN_ERROR
    }
    
    /**
     * 处理协议错误
     *
     * @param channel 网络通道
     * @param error 错误信息
     * @param cause 异常原因
     * @return 是否应该关闭连接
     */
    public static boolean handleProtocolError(Channel channel, ErrorType error, Throwable cause) {
        if (channel == null || !channel.isActive()) {
            return false;
        }
        
        log.warn("处理MQTT协议错误: {} for channel: {}", error, channel.remoteAddress(), cause);
        
        switch (error) {
            case PROTOCOL_VERSION_NOT_SUPPORTED:
                return handleVersionNotSupported(channel, cause);
                
            case MALFORMED_PACKET:
                return handleMalformedPacket(channel, cause);
                
            case PACKET_TOO_LARGE:
                return handlePacketTooLarge(channel, cause);
                
            case DECODE_ERROR:
                return handleDecodeError(channel, cause);
                
            case ENCODE_ERROR:
                return handleEncodeError(channel, cause);
                
            case CONNECTION_TIMEOUT:
                return handleConnectionTimeout(channel, cause);
                
            case AUTHENTICATION_FAILED:
                return handleAuthenticationFailed(channel, cause);
                
            case AUTHORIZATION_FAILED:
                return handleAuthorizationFailed(channel, cause);
                
            case INVALID_CLIENT_ID:
                return handleInvalidClientId(channel, cause);
                
            case SERVER_UNAVAILABLE:
                return handleServerUnavailable(channel, cause);
                
            default:
                return handleUnknownError(channel, cause);
        }
    }
    
    /**
     * 根据异常类型自动确定错误类型并处理
     */
    public static boolean handleException(Channel channel, Throwable cause) {
        ErrorType errorType = determineErrorType(cause);
        return handleProtocolError(channel, errorType, cause);
    }
    
    /**
     * 确定异常对应的错误类型
     */
    private static ErrorType determineErrorType(Throwable cause) {
        if (cause instanceof DecoderException) {
            if (cause.getMessage() != null && cause.getMessage().contains("version")) {
                return ErrorType.PROTOCOL_VERSION_NOT_SUPPORTED;
            } else if (cause.getCause() instanceof TooLongFrameException) {
                return ErrorType.PACKET_TOO_LARGE;
            } else {
                return ErrorType.DECODE_ERROR;
            }
        } else if (cause instanceof EncoderException) {
            return ErrorType.ENCODE_ERROR;
        } else if (cause instanceof TooLongFrameException) {
            return ErrorType.PACKET_TOO_LARGE;
        } else {
            return ErrorType.UNKNOWN_ERROR;
        }
    }
    
    /**
     * 处理协议版本不支持错误
     */
    private static boolean handleVersionNotSupported(Channel channel, Throwable cause) {
        log.error("不支持的MQTT协议版本: {}", channel.remoteAddress());
        
        try {
            // 尝试发送CONNACK响应（如果可能的话）
            sendConnackResponse(channel, MqttConnectReturnCode.CONNECTION_REFUSED_UNACCEPTABLE_PROTOCOL_VERSION);
        } catch (Exception e) {
            log.warn("发送CONNACK失败", e);
        }
        
        return true; // 关闭连接
    }
    
    /**
     * 处理畸形数据包错误
     */
    private static boolean handleMalformedPacket(Channel channel, Throwable cause) {
        log.error("接收到畸形MQTT数据包: {}", channel.remoteAddress());
        
        // 畸形数据包通常需要关闭连接
        return true;
    }
    
    /**
     * 处理数据包过大错误
     */
    private static boolean handlePacketTooLarge(Channel channel, Throwable cause) {
        log.error("MQTT数据包过大: {}", channel.remoteAddress());
        
        // 数据包过大，关闭连接防止内存溢出
        return true;
    }
    
    /**
     * 处理解码错误
     */
    private static boolean handleDecodeError(Channel channel, Throwable cause) {
        log.error("MQTT数据包解码失败: {}", channel.remoteAddress(), cause);
        
        // 解码错误通常表示数据损坏或协议不匹配
        return true;
    }
    
    /**
     * 处理编码错误
     */
    private static boolean handleEncodeError(Channel channel, Throwable cause) {
        log.error("MQTT数据包编码失败: {}", channel.remoteAddress(), cause);
        
        // 编码错误可能是临时问题，可以考虑不关闭连接
        return false;
    }
    
    /**
     * 处理连接超时
     */
    private static boolean handleConnectionTimeout(Channel channel, Throwable cause) {
        log.warn("MQTT连接超时: {}", channel.remoteAddress());
        
        return true; // 超时关闭连接
    }
    
    /**
     * 处理认证失败
     */
    private static boolean handleAuthenticationFailed(Channel channel, Throwable cause) {
        log.warn("MQTT认证失败: {}", channel.remoteAddress());
        
        try {
            sendConnackResponse(channel, MqttConnectReturnCode.CONNECTION_REFUSED_BAD_USER_NAME_OR_PASSWORD);
        } catch (Exception e) {
            log.warn("发送CONNACK失败", e);
        }
        
        return true;
    }
    
    /**
     * 处理授权失败
     */
    private static boolean handleAuthorizationFailed(Channel channel, Throwable cause) {
        log.warn("MQTT授权失败: {}", channel.remoteAddress());
        
        try {
            sendConnackResponse(channel, MqttConnectReturnCode.CONNECTION_REFUSED_NOT_AUTHORIZED);
        } catch (Exception e) {
            log.warn("发送CONNACK失败", e);
        }
        
        return true;
    }
    
    /**
     * 处理无效客户端ID
     */
    private static boolean handleInvalidClientId(Channel channel, Throwable cause) {
        log.warn("无效的MQTT客户端ID: {}", channel.remoteAddress());
        
        try {
            sendConnackResponse(channel, MqttConnectReturnCode.CONNECTION_REFUSED_IDENTIFIER_REJECTED);
        } catch (Exception e) {
            log.warn("发送CONNACK失败", e);
        }
        
        return true;
    }
    
    /**
     * 处理服务器不可用
     */
    private static boolean handleServerUnavailable(Channel channel, Throwable cause) {
        log.error("MQTT服务器不可用: {}", channel.remoteAddress());
        
        try {
            sendConnackResponse(channel, MqttConnectReturnCode.CONNECTION_REFUSED_SERVER_UNAVAILABLE);
        } catch (Exception e) {
            log.warn("发送CONNACK失败", e);
        }
        
        return true;
    }
    
    /**
     * 处理未知错误
     */
    private static boolean handleUnknownError(Channel channel, Throwable cause) {
        log.error("未知MQTT协议错误: {}", channel.remoteAddress(), cause);
        
        // 对于未知错误，采用保守策略关闭连接
        return true;
    }
    
    /**
     * 发送CONNACK响应
     */
    private static void sendConnackResponse(Channel channel, MqttConnectReturnCode returnCode) {
        try {
            MqttVersion version = MqttProtocolVersionManager.getProtocolVersion(channel);
            if (version == null) {
                version = MqttProtocolVersionDetector.getDefaultVersion();
            }
            
            // 这里简化处理，实际实现需要根据具体的MqttConnackPacket构造方法调整
            // MqttConnackPacket connackPacket = createConnackPacket(version, returnCode);
            // channel.writeAndFlush(connackPacket);
            
            log.debug("尝试发送CONNACK响应: {} to {}", returnCode, channel.remoteAddress());
            
        } catch (Exception e) {
            log.warn("创建CONNACK响应失败", e);
        }
    }
    
    /**
     * 检查错误是否可恢复
     */
    public static boolean isRecoverableError(ErrorType error) {
        return switch (error) {
            case ENCODE_ERROR, CONNECTION_TIMEOUT -> true;
            default -> false;
        };
    }
    
    /**
     * 获取错误描述
     */
    public static String getErrorDescription(ErrorType error) {
        return switch (error) {
            case PROTOCOL_VERSION_NOT_SUPPORTED -> "不支持的MQTT协议版本";
            case MALFORMED_PACKET -> "畸形数据包";
            case PACKET_TOO_LARGE -> "数据包过大";
            case DECODE_ERROR -> "解码错误";
            case ENCODE_ERROR -> "编码错误";
            case CONNECTION_TIMEOUT -> "连接超时";
            case AUTHENTICATION_FAILED -> "认证失败";
            case AUTHORIZATION_FAILED -> "授权失败";
            case INVALID_CLIENT_ID -> "无效客户端ID";
            case SERVER_UNAVAILABLE -> "服务器不可用";
            case UNKNOWN_ERROR -> "未知错误";
        };
    }
}