/**
 * MQTT编码器接口
 *
 * @author zhenglin
 * @date 2025/08/06
 */
package com.vmqtt.common.protocol.codec;

import com.vmqtt.common.protocol.MqttVersion;
import com.vmqtt.common.protocol.packet.MqttPacket;
import io.netty.buffer.ByteBuf;

/**
 * MQTT编码器接口
 * 
 * 定义MQTT数据包编码的统一接口
 */
public interface MqttEncoder {
    
    /**
     * 编码MQTT数据包
     *
     * @param packet MQTT数据包
     * @param buffer 字节缓冲区
     * @throws MqttCodecException 编码失败时抛出
     */
    void encode(MqttPacket packet, ByteBuf buffer) throws MqttCodecException;
    
    /**
     * 获取支持的协议版本
     *
     * @return 协议版本
     */
    MqttVersion getVersion();
}