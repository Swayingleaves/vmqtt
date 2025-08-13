/**
 * MQTT协议版本管理器
 *
 * @author zhenglin
 * @date 2025/08/13
 */
package com.vmqtt.common.protocol.codec;

import com.vmqtt.common.protocol.MqttVersion;
import io.netty.channel.Channel;
import io.netty.util.AttributeKey;
import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * MQTT协议版本管理器
 * 
 * 管理每个连接的MQTT协议版本和编解码器
 */
@Slf4j
public class MqttProtocolVersionManager {
    
    // Channel属性键，用于存储协议版本
    public static final AttributeKey<MqttVersion> PROTOCOL_VERSION_KEY = 
        AttributeKey.valueOf("mqtt_protocol_version");
    
    // Channel属性键，用于存储编解码器对
    public static final AttributeKey<MqttCodecFactory.MqttCodecPair> CODEC_PAIR_KEY = 
        AttributeKey.valueOf("mqtt_codec_pair");
    
    // Channel属性键，用于标记版本是否已协商
    public static final AttributeKey<Boolean> VERSION_NEGOTIATED_KEY = 
        AttributeKey.valueOf("mqtt_version_negotiated");
    
    // 全局编解码器缓存，避免重复创建
    private static final ConcurrentMap<MqttVersion, MqttCodecFactory.MqttCodecPair> codecCache = 
        new ConcurrentHashMap<>();
    
    /**
     * 设置连接的MQTT协议版本
     *
     * @param channel 网络通道
     * @param version 协议版本
     * @return 是否设置成功
     */
    public static boolean setProtocolVersion(Channel channel, MqttVersion version) {
        if (channel == null || version == null) {
            log.warn("无效的参数: channel={}, version={}", channel, version);
            return false;
        }
        
        if (!MqttCodecFactory.isVersionSupported(version)) {
            log.warn("不支持的MQTT协议版本: {} for channel: {}", version, channel.remoteAddress());
            return false;
        }
        
        try {
            // 获取或创建编解码器对
            MqttCodecFactory.MqttCodecPair codecPair = codecCache.computeIfAbsent(version, 
                MqttCodecFactory::createCodecPair);
            
            // 设置Channel属性
            channel.attr(PROTOCOL_VERSION_KEY).set(version);
            channel.attr(CODEC_PAIR_KEY).set(codecPair);
            channel.attr(VERSION_NEGOTIATED_KEY).set(true);
            
            log.debug("为连接 {} 设置MQTT协议版本: {}", channel.remoteAddress(), version);
            return true;
            
        } catch (Exception e) {
            log.error("设置MQTT协议版本失败: channel={}, version={}", channel.remoteAddress(), version, e);
            return false;
        }
    }
    
    /**
     * 获取连接的MQTT协议版本
     *
     * @param channel 网络通道
     * @return 协议版本，如果未设置返回null
     */
    public static MqttVersion getProtocolVersion(Channel channel) {
        if (channel == null) {
            return null;
        }
        
        return channel.attr(PROTOCOL_VERSION_KEY).get();
    }
    
    /**
     * 获取连接的编解码器对
     *
     * @param channel 网络通道
     * @return 编解码器对，如果未设置返回null
     */
    public static MqttCodecFactory.MqttCodecPair getCodecPair(Channel channel) {
        if (channel == null) {
            return null;
        }
        
        return channel.attr(CODEC_PAIR_KEY).get();
    }
    
    /**
     * 检查连接的协议版本是否已协商
     *
     * @param channel 网络通道
     * @return 如果已协商返回true
     */
    public static boolean isVersionNegotiated(Channel channel) {
        if (channel == null) {
            return false;
        }
        
        Boolean negotiated = channel.attr(VERSION_NEGOTIATED_KEY).get();
        return Boolean.TRUE.equals(negotiated);
    }
    
    /**
     * 重置连接的协议版本
     *
     * @param channel 网络通道
     */
    public static void resetProtocolVersion(Channel channel) {
        if (channel == null) {
            return;
        }
        
        try {
            channel.attr(PROTOCOL_VERSION_KEY).set(null);
            channel.attr(CODEC_PAIR_KEY).set(null);
            channel.attr(VERSION_NEGOTIATED_KEY).set(false);
            
            log.debug("重置连接 {} 的MQTT协议版本", channel.remoteAddress());
            
        } catch (Exception e) {
            log.warn("重置MQTT协议版本时发生异常: {}", channel.remoteAddress(), e);
        }
    }
    
    /**
     * 协商MQTT协议版本
     *
     * @param channel 网络通道
     * @param requestedVersion 客户端请求的版本
     * @return 协商后的版本，如果协商失败返回null
     */
    public static MqttVersion negotiateVersion(Channel channel, MqttVersion requestedVersion) {
        if (channel == null) {
            log.warn("无效的channel");
            return null;
        }
        
        log.debug("开始协商MQTT协议版本: channel={}, requestedVersion={}", 
            channel.remoteAddress(), requestedVersion);
        
        // 如果客户端请求的版本受支持，直接使用
        if (MqttCodecFactory.isVersionSupported(requestedVersion)) {
            if (setProtocolVersion(channel, requestedVersion)) {
                log.info("协商成功，使用客户端请求的版本: {} for channel: {}", 
                    requestedVersion, channel.remoteAddress());
                return requestedVersion;
            }
        }
        
        // 尝试回退到支持的版本
        MqttVersion fallbackVersion = findSupportedFallbackVersion(requestedVersion);
        if (fallbackVersion != null && setProtocolVersion(channel, fallbackVersion)) {
            log.info("协商成功，回退到支持的版本: {} for channel: {} (原请求版本: {})", 
                fallbackVersion, channel.remoteAddress(), requestedVersion);
            return fallbackVersion;
        }
        
        log.warn("MQTT协议版本协商失败: channel={}, requestedVersion={}", 
            channel.remoteAddress(), requestedVersion);
        return null;
    }
    
    /**
     * 查找支持的回退版本
     */
    private static MqttVersion findSupportedFallbackVersion(MqttVersion requestedVersion) {
        MqttVersion[] supportedVersions = MqttCodecFactory.getSupportedVersions();
        
        // 如果请求的是MQTT 5.0，回退到3.1.1
        if (requestedVersion == MqttVersion.MQTT_5_0) {
            for (MqttVersion version : supportedVersions) {
                if (version == MqttVersion.MQTT_3_1_1) {
                    return version;
                }
            }
        }
        
        // 默认返回第一个支持的版本
        if (supportedVersions.length > 0) {
            return supportedVersions[0];
        }
        
        return null;
    }
    
    /**
     * 获取编解码器统计信息
     *
     * @return 统计信息字符串
     */
    public static String getCodecStats() {
        return String.format("编解码器缓存大小: %d, 支持的版本: %d", 
            codecCache.size(), MqttCodecFactory.getSupportedVersions().length);
    }
    
    /**
     * 清理编解码器缓存
     */
    public static void clearCodecCache() {
        codecCache.clear();
        log.info("已清理MQTT编解码器缓存");
    }
    
    /**
     * 预热编解码器缓存
     */
    public static void warmupCodecCache() {
        log.info("开始预热MQTT编解码器缓存");
        
        for (MqttVersion version : MqttCodecFactory.getSupportedVersions()) {
            try {
                codecCache.computeIfAbsent(version, MqttCodecFactory::createCodecPair);
                log.debug("预热编解码器: {}", version);
            } catch (Exception e) {
                log.warn("预热编解码器失败: {}", version, e);
            }
        }
        
        log.info("MQTT编解码器缓存预热完成，缓存大小: {}", codecCache.size());
    }
}