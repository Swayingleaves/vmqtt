/**
 * MQTT编解码器工厂
 *
 * @author zhenglin
 * @date 2025/08/06
 */
package com.vmqtt.common.protocol.codec;

import com.vmqtt.common.protocol.MqttVersion;
import com.vmqtt.common.protocol.codec.v3.MqttV3Decoder;
import com.vmqtt.common.protocol.codec.v3.MqttV3Encoder;

/**
 * MQTT编解码器工厂
 * 
 * 根据协议版本创建对应的编解码器
 */
public class MqttCodecFactory {
    
    /**
     * 创建解码器
     *
     * @param version MQTT协议版本
     * @return MQTT解码器
     */
    public static MqttDecoder createDecoder(MqttVersion version) {
        return switch (version) {
            case MQTT_3_1, MQTT_3_1_1 -> new MqttV3DecoderWrapper(new MqttV3Decoder(version));
            case MQTT_5_0 -> throw new UnsupportedOperationException("MQTT 5.0 decoder not fully implemented yet");
        };
    }
    
    /**
     * 创建编码器
     *
     * @param version MQTT协议版本
     * @return MQTT编码器
     */
    public static MqttEncoder createEncoder(MqttVersion version) {
        return switch (version) {
            case MQTT_3_1, MQTT_3_1_1 -> new MqttV3EncoderWrapper(new MqttV3Encoder(version));
            case MQTT_5_0 -> throw new UnsupportedOperationException("MQTT 5.0 encoder not fully implemented yet");
        };
    }
    
    /**
     * 创建编解码器对
     *
     * @param version MQTT协议版本
     * @return 编解码器对
     */
    public static MqttCodecPair createCodecPair(MqttVersion version) {
        return new MqttCodecPair(createDecoder(version), createEncoder(version));
    }
    
    /**
     * 检查是否支持指定版本
     *
     * @param version MQTT协议版本
     * @return 如果支持返回true
     */
    public static boolean isVersionSupported(MqttVersion version) {
        return switch (version) {
            case MQTT_3_1, MQTT_3_1_1 -> true;
            case MQTT_5_0 -> false; // 暂时不支持
        };
    }
    
    /**
     * 获取支持的版本
     *
     * @return 支持的版本数组
     */
    public static MqttVersion[] getSupportedVersions() {
        return new MqttVersion[] {
            MqttVersion.MQTT_3_1,
            MqttVersion.MQTT_3_1_1
        };
    }
    
    /**
     * MQTT编解码器对
     */
    public record MqttCodecPair(MqttDecoder decoder, MqttEncoder encoder) {
        
        /**
         * 获取协议版本
         *
         * @return 协议版本
         */
        public MqttVersion getVersion() {
            return decoder.getVersion();
        }
        
        @Override
        public String toString() {
            return String.format("MqttCodecPair{version=%s}", getVersion());
        }
    }
    
    /**
     * MQTT 3.x解码器包装器
     */
    private static class MqttV3DecoderWrapper implements MqttDecoder {
        private final MqttV3Decoder decoder;
        
        private MqttV3DecoderWrapper(MqttV3Decoder decoder) {
            this.decoder = decoder;
        }
        
        @Override
        public com.vmqtt.common.protocol.packet.MqttPacket decode(io.netty.buffer.ByteBuf buffer) {
            return decoder.decode(buffer);
        }
        
        @Override
        public MqttVersion getVersion() {
            return decoder.getProtocolVersion();
        }
    }
    
    /**
     * MQTT 3.x编码器包装器
     */
    private static class MqttV3EncoderWrapper implements MqttEncoder {
        private final MqttV3Encoder encoder;
        
        private MqttV3EncoderWrapper(MqttV3Encoder encoder) {
            this.encoder = encoder;
        }
        
        @Override
        public void encode(com.vmqtt.common.protocol.packet.MqttPacket packet, io.netty.buffer.ByteBuf buffer) {
            encoder.encode(packet, buffer);
        }
        
        @Override
        public MqttVersion getVersion() {
            return encoder.getProtocolVersion();
        }
    }
}