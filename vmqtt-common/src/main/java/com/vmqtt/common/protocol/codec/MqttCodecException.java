/**
 * MQTT编解码异常
 *
 * @author zhenglin
 * @date 2025/08/06
 */
package com.vmqtt.common.protocol.codec;

/**
 * MQTT协议编解码异常
 * 
 * 在MQTT数据包编码或解码过程中发生错误时抛出
 */
public class MqttCodecException extends RuntimeException {
    
    /**
     * 序列化版本号
     */
    private static final long serialVersionUID = 1L;
    
    /**
     * 创建编解码异常
     *
     * @param message 异常消息
     */
    public MqttCodecException(String message) {
        super(message);
    }
    
    /**
     * 创建编解码异常
     *
     * @param message 异常消息
     * @param cause 原因异常
     */
    public MqttCodecException(String message, Throwable cause) {
        super(message, cause);
    }
    
    /**
     * 创建编解码异常
     *
     * @param cause 原因异常
     */
    public MqttCodecException(Throwable cause) {
        super(cause);
    }
}