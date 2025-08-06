/**
 * MQTT协议版本协商器
 *
 * @author zhenglin
 * @date 2025/08/06
 */
package com.vmqtt.common.protocol.version;

import com.vmqtt.common.protocol.MqttVersion;
import com.vmqtt.common.protocol.packet.connack.MqttConnectReturnCode;

import java.util.EnumSet;
import java.util.Set;

/**
 * MQTT协议版本协商器
 * 
 * 负责处理客户端和服务器之间的协议版本协商
 */
public class MqttVersionNegotiator {
    
    /**
     * 服务器支持的协议版本
     */
    private final Set<MqttVersion> supportedVersions;
    
    /**
     * 默认协议版本
     */
    private final MqttVersion defaultVersion;
    
    /**
     * 构造函数
     *
     * @param supportedVersions 支持的协议版本
     * @param defaultVersion 默认协议版本
     */
    public MqttVersionNegotiator(Set<MqttVersion> supportedVersions, MqttVersion defaultVersion) {
        if (supportedVersions == null || supportedVersions.isEmpty()) {
            throw new IllegalArgumentException("Supported versions cannot be null or empty");
        }
        
        if (defaultVersion == null || !supportedVersions.contains(defaultVersion)) {
            throw new IllegalArgumentException("Default version must be one of the supported versions");
        }
        
        this.supportedVersions = EnumSet.copyOf(supportedVersions);
        this.defaultVersion = defaultVersion;
    }
    
    /**
     * 协商协议版本
     *
     * @param clientRequestedVersion 客户端请求的协议版本
     * @return 协商结果
     */
    public NegotiationResult negotiate(MqttVersion clientRequestedVersion) {
        if (clientRequestedVersion == null) {
            return NegotiationResult.failure(
                MqttConnectReturnCode.CONNECTION_REFUSED_UNACCEPTABLE_PROTOCOL_VERSION,
                "Client requested version is null"
            );
        }
        
        // 检查是否支持客户端请求的版本
        if (supportedVersions.contains(clientRequestedVersion)) {
            return NegotiationResult.success(clientRequestedVersion);
        }
        
        // 尝试版本降级
        MqttVersion negotiatedVersion = attemptVersionDowngrade(clientRequestedVersion);
        if (negotiatedVersion != null) {
            return NegotiationResult.success(negotiatedVersion);
        }
        
        // 协商失败
        return NegotiationResult.failure(
            MqttConnectReturnCode.CONNECTION_REFUSED_UNACCEPTABLE_PROTOCOL_VERSION,
            String.format("Unsupported protocol version: %s, supported versions: %s", 
                         clientRequestedVersion, supportedVersions)
        );
    }
    
    /**
     * 尝试版本降级
     *
     * @param clientVersion 客户端版本
     * @return 降级后的版本，如果无法降级返回null
     */
    private MqttVersion attemptVersionDowngrade(MqttVersion clientVersion) {
        // 如果客户端请求MQTT 5.0，尝试降级到3.1.1
        if (clientVersion == MqttVersion.MQTT_5_0 && supportedVersions.contains(MqttVersion.MQTT_3_1_1)) {
            return MqttVersion.MQTT_3_1_1;
        }
        
        // 如果客户端请求MQTT 3.1.1，尝试降级到3.1
        if (clientVersion == MqttVersion.MQTT_3_1_1 && supportedVersions.contains(MqttVersion.MQTT_3_1)) {
            return MqttVersion.MQTT_3_1;
        }
        
        // 无法降级，返回默认版本（如果支持）
        if (supportedVersions.contains(defaultVersion)) {
            return defaultVersion;
        }
        
        return null;
    }
    
    /**
     * 检查是否支持指定版本
     *
     * @param version 协议版本
     * @return 如果支持返回true
     */
    public boolean isSupported(MqttVersion version) {
        return supportedVersions.contains(version);
    }
    
    /**
     * 获取支持的协议版本
     *
     * @return 支持的协议版本集合
     */
    public Set<MqttVersion> getSupportedVersions() {
        return EnumSet.copyOf(supportedVersions);
    }
    
    /**
     * 获取默认协议版本
     *
     * @return 默认协议版本
     */
    public MqttVersion getDefaultVersion() {
        return defaultVersion;
    }
    
    /**
     * 获取最高支持的协议版本
     *
     * @return 最高协议版本
     */
    public MqttVersion getHighestSupportedVersion() {
        if (supportedVersions.contains(MqttVersion.MQTT_5_0)) {
            return MqttVersion.MQTT_5_0;
        } else if (supportedVersions.contains(MqttVersion.MQTT_3_1_1)) {
            return MqttVersion.MQTT_3_1_1;
        } else {
            return MqttVersion.MQTT_3_1;
        }
    }
    
    /**
     * 获取最低支持的协议版本
     *
     * @return 最低协议版本
     */
    public MqttVersion getLowestSupportedVersion() {
        if (supportedVersions.contains(MqttVersion.MQTT_3_1)) {
            return MqttVersion.MQTT_3_1;
        } else if (supportedVersions.contains(MqttVersion.MQTT_3_1_1)) {
            return MqttVersion.MQTT_3_1_1;
        } else {
            return MqttVersion.MQTT_5_0;
        }
    }
    
    /**
     * 创建支持所有版本的协商器
     *
     * @return 协商器实例
     */
    public static MqttVersionNegotiator createFullSupport() {
        return new MqttVersionNegotiator(
            EnumSet.allOf(MqttVersion.class),
            MqttVersion.MQTT_3_1_1
        );
    }
    
    /**
     * 创建仅支持MQTT 3.x的协商器
     *
     * @return 协商器实例
     */
    public static MqttVersionNegotiator createMqtt3xOnly() {
        return new MqttVersionNegotiator(
            EnumSet.of(MqttVersion.MQTT_3_1, MqttVersion.MQTT_3_1_1),
            MqttVersion.MQTT_3_1_1
        );
    }
    
    /**
     * 创建仅支持MQTT 5.0的协商器
     *
     * @return 协商器实例
     */
    public static MqttVersionNegotiator createMqtt5Only() {
        return new MqttVersionNegotiator(
            EnumSet.of(MqttVersion.MQTT_5_0),
            MqttVersion.MQTT_5_0
        );
    }
    
    /**
     * 协商结果
     */
    public static class NegotiationResult {
        private final boolean success;
        private final MqttVersion negotiatedVersion;
        private final MqttConnectReturnCode returnCode;
        private final String message;
        
        private NegotiationResult(boolean success, MqttVersion negotiatedVersion, 
                                 MqttConnectReturnCode returnCode, String message) {
            this.success = success;
            this.negotiatedVersion = negotiatedVersion;
            this.returnCode = returnCode;
            this.message = message;
        }
        
        /**
         * 创建成功的协商结果
         *
         * @param negotiatedVersion 协商后的版本
         * @return 协商结果
         */
        public static NegotiationResult success(MqttVersion negotiatedVersion) {
            return new NegotiationResult(
                true, 
                negotiatedVersion, 
                MqttConnectReturnCode.CONNECTION_ACCEPTED, 
                "Protocol version negotiated successfully"
            );
        }
        
        /**
         * 创建失败的协商结果
         *
         * @param returnCode 返回码
         * @param message 错误消息
         * @return 协商结果
         */
        public static NegotiationResult failure(MqttConnectReturnCode returnCode, String message) {
            return new NegotiationResult(false, null, returnCode, message);
        }
        
        /**
         * 检查协商是否成功
         *
         * @return 如果成功返回true
         */
        public boolean isSuccess() {
            return success;
        }
        
        /**
         * 获取协商后的版本
         *
         * @return 协商后的版本，如果协商失败返回null
         */
        public MqttVersion getNegotiatedVersion() {
            return negotiatedVersion;
        }
        
        /**
         * 获取返回码
         *
         * @return 返回码
         */
        public MqttConnectReturnCode getReturnCode() {
            return returnCode;
        }
        
        /**
         * 获取消息
         *
         * @return 消息
         */
        public String getMessage() {
            return message;
        }
        
        @Override
        public String toString() {
            return String.format("NegotiationResult{success=%s, version=%s, returnCode=%s, message='%s'}",
                               success, negotiatedVersion, returnCode, message);
        }
    }
    
    @Override
    public String toString() {
        return String.format("MqttVersionNegotiator{supportedVersions=%s, defaultVersion=%s}",
                           supportedVersions, defaultVersion);
    }
}