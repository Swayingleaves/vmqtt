/**
 * MQTT连接包
 *
 * @author zhenglin
 * @date 2025/08/06
 */
package com.vmqtt.common.protocol.packet.connect;

import com.vmqtt.common.protocol.MqttVersion;
import com.vmqtt.common.protocol.packet.MqttFixedHeader;
import com.vmqtt.common.protocol.packet.MqttPacket;
import com.vmqtt.common.protocol.packet.MqttPacketType;

/**
 * MQTT CONNECT包定义
 * 
 * 客户端连接到服务器时发送的第一个包
 */
public record MqttConnectPacket(
        MqttFixedHeader fixedHeader,
        MqttConnectVariableHeader variableHeader,
        MqttConnectPayload payload) implements MqttPacket {
    
    @Override
    public MqttFixedHeader getFixedHeader() {
        return fixedHeader;
    }
    
    /**
     * 构造函数验证
     */
    public MqttConnectPacket {
        if (fixedHeader.packetType() != MqttPacketType.CONNECT) {
            throw new IllegalArgumentException("Invalid packet type for CONNECT packet");
        }
        
        if (variableHeader == null) {
            throw new IllegalArgumentException("Variable header cannot be null for CONNECT packet");
        }
        
        if (payload == null) {
            throw new IllegalArgumentException("Payload cannot be null for CONNECT packet");
        }
        
        // 验证客户端ID
        if (payload.clientId() == null || payload.clientId().isEmpty()) {
            if (!variableHeader.connectFlags().cleanSession()) {
                throw new IllegalArgumentException("Clean session must be true if client ID is empty");
            }
        }
        
        // 验证遗嘱消息
        if (variableHeader.connectFlags().willFlag()) {
            if (payload.willTopic() == null || payload.willTopic().isEmpty()) {
                throw new IllegalArgumentException("Will topic cannot be null or empty when will flag is set");
            }
        } else {
            if (payload.willTopic() != null || payload.willMessage() != null) {
                throw new IllegalArgumentException("Will topic and message must be null when will flag is not set");
            }
        }
        
        // 验证用户名和密码
        if (variableHeader.connectFlags().usernameFlag()) {
            if (payload.username() == null) {
                throw new IllegalArgumentException("Username cannot be null when username flag is set");
            }
        } else {
            if (payload.username() != null) {
                throw new IllegalArgumentException("Username must be null when username flag is not set");
            }
        }
        
        if (variableHeader.connectFlags().passwordFlag()) {
            if (payload.password() == null) {
                throw new IllegalArgumentException("Password cannot be null when password flag is set");
            }
            if (!variableHeader.connectFlags().usernameFlag()) {
                throw new IllegalArgumentException("Username flag must be set when password flag is set");
            }
        } else {
            if (payload.password() != null) {
                throw new IllegalArgumentException("Password must be null when password flag is not set");
            }
        }
    }
    
    /**
     * 获取MQTT版本
     *
     * @return MQTT版本
     */
    public MqttVersion getMqttVersion() {
        return variableHeader.mqttVersion();
    }
    
    /**
     * 获取客户端ID
     *
     * @return 客户端ID
     */
    public String getClientId() {
        return payload.clientId();
    }
    
    /**
     * 获取保活时间
     *
     * @return 保活时间（秒）
     */
    public int getKeepAlive() {
        return variableHeader.keepAlive();
    }
    
    /**
     * 检查是否为清除会话
     *
     * @return 如果是清除会话返回true
     */
    public boolean isCleanSession() {
        return variableHeader.connectFlags().cleanSession();
    }
    
    /**
     * 检查是否有遗嘱消息
     *
     * @return 如果有遗嘱消息返回true
     */
    public boolean hasWill() {
        return variableHeader.connectFlags().willFlag();
    }
    
    /**
     * 检查是否有用户名
     *
     * @return 如果有用户名返回true
     */
    public boolean hasUsername() {
        return variableHeader.connectFlags().usernameFlag();
    }
    
    /**
     * 检查是否有密码
     *
     * @return 如果有密码返回true
     */
    public boolean hasPassword() {
        return variableHeader.connectFlags().passwordFlag();
    }
    
    /**
     * 创建CONNECT包构建器
     *
     * @param clientId 客户端ID
     * @return CONNECT包构建器
     */
    public static Builder builder(String clientId) {
        return new Builder(clientId);
    }
    
    /**
     * CONNECT包构建器
     */
    public static class Builder {
        private String clientId;
        private MqttVersion mqttVersion = MqttVersion.MQTT_3_1_1;
        private boolean cleanSession = true;
        private int keepAlive = 60;
        private String willTopic;
        private byte[] willMessage;
        private boolean willRetain = false;
        private int willQos = 0;
        private String username;
        private byte[] password;
        private Object properties; // MQTT 5.0属性
        
        private Builder(String clientId) {
            this.clientId = clientId;
        }
        
        public Builder mqttVersion(MqttVersion mqttVersion) {
            this.mqttVersion = mqttVersion;
            return this;
        }
        
        public Builder cleanSession(boolean cleanSession) {
            this.cleanSession = cleanSession;
            return this;
        }
        
        public Builder keepAlive(int keepAlive) {
            this.keepAlive = keepAlive;
            return this;
        }
        
        public Builder will(String topic, byte[] message, boolean retain, int qos) {
            this.willTopic = topic;
            this.willMessage = message;
            this.willRetain = retain;
            this.willQos = qos;
            return this;
        }
        
        public Builder credentials(String username, byte[] password) {
            this.username = username;
            this.password = password;
            return this;
        }
        
        public Builder properties(Object properties) {
            this.properties = properties;
            return this;
        }
        
        public MqttConnectPacket build() {
            // 计算剩余长度
            int remainingLength = calculateRemainingLength();
            
            // 创建固定头部
            MqttFixedHeader fixedHeader = new MqttFixedHeader(MqttPacketType.CONNECT, remainingLength);
            
            // 创建连接标志
            MqttConnectFlags connectFlags = new MqttConnectFlags(
                cleanSession,
                willTopic != null,
                willQos,
                willRetain,
                username != null,
                password != null
            );
            
            // 创建可变头部
            MqttConnectVariableHeader variableHeader = new MqttConnectVariableHeader(
                mqttVersion,
                connectFlags,
                keepAlive,
                properties
            );
            
            // 创建负载
            MqttConnectPayload payload = new MqttConnectPayload(
                clientId,
                willTopic,
                willMessage,
                username,
                password
            );
            
            return new MqttConnectPacket(fixedHeader, variableHeader, payload);
        }
        
        private int calculateRemainingLength() {
            int length = 0;
            
            // 协议名长度
            length += 2 + mqttVersion.getProtocolName().getBytes().length;
            // 协议级别
            length += 1;
            // 连接标志
            length += 1;
            // 保活时间
            length += 2;
            
            // MQTT 5.0属性长度
            if (mqttVersion.isMqtt5() && properties != null) {
                // 这里需要根据实际属性实现计算长度
                length += 1; // 暂时占位
            } else if (mqttVersion.isMqtt5()) {
                length += 1; // 空属性长度
            }
            
            // 客户端ID
            length += 2 + (clientId != null ? clientId.getBytes().length : 0);
            
            // 遗嘱属性（MQTT 5.0）
            if (mqttVersion.isMqtt5() && willTopic != null) {
                length += 1; // 遗嘱属性长度占位
            }
            
            // 遗嘱主题
            if (willTopic != null) {
                length += 2 + willTopic.getBytes().length;
            }
            
            // 遗嘱消息
            if (willMessage != null) {
                length += 2 + willMessage.length;
            }
            
            // 用户名
            if (username != null) {
                length += 2 + username.getBytes().length;
            }
            
            // 密码
            if (password != null) {
                length += 2 + password.length;
            }
            
            return length;
        }
    }
    
    @Override
    public String toString() {
        return String.format("MqttConnectPacket{clientId='%s', version=%s, cleanSession=%s, keepAlive=%d}",
                           payload.clientId(), variableHeader.mqttVersion(), 
                           variableHeader.connectFlags().cleanSession(), variableHeader.keepAlive());
    }
}