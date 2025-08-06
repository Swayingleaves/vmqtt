/**
 * MQTT连接标志
 *
 * @author zhenglin
 * @date 2025/08/06
 */
package com.vmqtt.common.protocol.packet.connect;

import com.vmqtt.common.protocol.MqttQos;

/**
 * MQTT CONNECT包连接标志定义
 * 
 * 连接标志字节的各个位定义：
 * - bit 7: Username Flag
 * - bit 6: Password Flag  
 * - bit 5: Will Retain
 * - bit 4-3: Will QoS
 * - bit 2: Will Flag
 * - bit 1: Clean Session
 * - bit 0: Reserved (必须为0)
 */
public record MqttConnectFlags(
        boolean cleanSession,
        boolean willFlag,
        int willQos,
        boolean willRetain,
        boolean usernameFlag,
        boolean passwordFlag) {
    
    /**
     * 构造函数验证
     */
    public MqttConnectFlags {
        // 验证Will QoS
        if (willQos < 0 || willQos > 2) {
            throw new IllegalArgumentException("Will QoS must be 0, 1, or 2");
        }
        
        // 如果没有Will Flag，则Will QoS和Will Retain必须为0/false
        if (!willFlag) {
            if (willQos != 0) {
                throw new IllegalArgumentException("Will QoS must be 0 when Will Flag is not set");
            }
            if (willRetain) {
                throw new IllegalArgumentException("Will Retain must be false when Will Flag is not set");
            }
        }
        
        // 如果Password Flag设置，则Username Flag也必须设置
        if (passwordFlag && !usernameFlag) {
            throw new IllegalArgumentException("Username Flag must be set when Password Flag is set");
        }
    }
    
    /**
     * 从字节值创建连接标志
     *
     * @param flags 标志字节
     * @return MQTT连接标志
     * @throws IllegalArgumentException 如果保留位不为0
     */
    public static MqttConnectFlags fromByte(byte flags) {
        // 检查保留位（bit 0）
        if ((flags & 0x01) != 0) {
            throw new IllegalArgumentException("Reserved flag bit must be 0");
        }
        
        boolean cleanSession = (flags & 0x02) != 0;
        boolean willFlag = (flags & 0x04) != 0;
        int willQos = (flags & 0x18) >> 3;
        boolean willRetain = (flags & 0x20) != 0;
        boolean passwordFlag = (flags & 0x40) != 0;
        boolean usernameFlag = (flags & 0x80) != 0;
        
        return new MqttConnectFlags(cleanSession, willFlag, willQos, willRetain, usernameFlag, passwordFlag);
    }
    
    /**
     * 转换为字节值
     *
     * @return 标志字节
     */
    public byte toByte() {
        byte flags = 0;
        
        if (cleanSession) {
            flags |= 0x02;
        }
        
        if (willFlag) {
            flags |= 0x04;
        }
        
        flags |= (willQos << 3);
        
        if (willRetain) {
            flags |= 0x20;
        }
        
        if (passwordFlag) {
            flags |= 0x40;
        }
        
        if (usernameFlag) {
            flags |= 0x80;
        }
        
        return flags;
    }
    
    /**
     * 获取Will QoS枚举值
     *
     * @return Will QoS等级
     */
    public MqttQos getWillQos() {
        return MqttQos.fromValue(willQos);
    }
    
    /**
     * 创建默认的连接标志（Clean Session = true，其他均为false/0）
     *
     * @return 默认连接标志
     */
    public static MqttConnectFlags defaultFlags() {
        return new MqttConnectFlags(true, false, 0, false, false, false);
    }
    
    /**
     * 创建带认证的连接标志
     *
     * @param cleanSession 清除会话标志
     * @return 带认证的连接标志
     */
    public static MqttConnectFlags withCredentials(boolean cleanSession) {
        return new MqttConnectFlags(cleanSession, false, 0, false, true, true);
    }
    
    /**
     * 创建带遗嘱的连接标志
     *
     * @param cleanSession 清除会话标志
     * @param willQos 遗嘱QoS
     * @param willRetain 遗嘱保留标志
     * @return 带遗嘱的连接标志
     */
    public static MqttConnectFlags withWill(boolean cleanSession, int willQos, boolean willRetain) {
        return new MqttConnectFlags(cleanSession, true, willQos, willRetain, false, false);
    }
    
    /**
     * 创建完整的连接标志
     *
     * @param cleanSession 清除会话标志
     * @param willQos 遗嘱QoS
     * @param willRetain 遗嘱保留标志
     * @param hasCredentials 是否有认证信息
     * @return 完整的连接标志
     */
    public static MqttConnectFlags create(boolean cleanSession, int willQos, 
                                        boolean willRetain, boolean hasCredentials) {
        boolean willFlag = willQos > 0 || willRetain;
        return new MqttConnectFlags(cleanSession, willFlag, willQos, willRetain, 
                                   hasCredentials, hasCredentials);
    }
    
    @Override
    public String toString() {
        return String.format("MqttConnectFlags{cleanSession=%s, will=%s, willQoS=%d, willRetain=%s, username=%s, password=%s}",
                           cleanSession, willFlag, willQos, willRetain, usernameFlag, passwordFlag);
    }
}