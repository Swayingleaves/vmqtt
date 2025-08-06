/**
 * MQTT解码器接口
 *
 * @author zhenglin
 * @date 2025/08/06
 */
package com.vmqtt.common.protocol.codec;

import com.vmqtt.common.protocol.MqttVersion;
import com.vmqtt.common.protocol.packet.MqttPacket;
import io.netty.buffer.ByteBuf;

/**
 * MQTT解码器接口
 * 
 * 定义MQTT数据包解码的统一接口
 */
public interface MqttDecoder {
    
    /**
     * 解码MQTT数据包
     *
     * @param buffer 字节缓冲区
     * @return MQTT数据包，如果数据不足返回null
     * @throws MqttCodecException 解码失败时抛出
     */
    MqttPacket decode(ByteBuf buffer) throws MqttCodecException;
    
    /**
     * 获取支持的协议版本
     *
     * @return 协议版本
     */
    MqttVersion getVersion();
}