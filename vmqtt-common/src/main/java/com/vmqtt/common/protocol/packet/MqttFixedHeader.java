/**
 * MQTT固定头部
 *
 * @author zhenglin
 * @date 2025/08/06
 */
package com.vmqtt.common.protocol.packet;

import com.vmqtt.common.protocol.MqttQos;

/**
 * MQTT固定头部定义
 * 
 * 固定头部存在于每个MQTT控制包中，包含：
 * - 控制包类型和标志位（1字节）
 * - 剩余长度（1-4字节）
 */
public record MqttFixedHeader(
        MqttPacketType packetType,
        boolean dupFlag,
        MqttQos qos,
        boolean retainFlag,
        int remainingLength) {
    
    /**
     * 构造函数 - 创建不带QoS和标志的固定头部
     *
     * @param packetType 包类型
     * @param remainingLength 剩余长度
     */
    public MqttFixedHeader(MqttPacketType packetType, int remainingLength) {
        this(packetType, false, MqttQos.AT_MOST_ONCE, false, remainingLength);
    }
    
    /**
     * 构造函数 - 创建带QoS但不带其他标志的固定头部
     *
     * @param packetType 包类型
     * @param qos QoS等级
     * @param remainingLength 剩余长度
     */
    public MqttFixedHeader(MqttPacketType packetType, MqttQos qos, int remainingLength) {
        this(packetType, false, qos, false, remainingLength);
    }
    
    /**
     * 构造函数验证
     */
    public MqttFixedHeader {
        validateFixedHeader(packetType, dupFlag, qos, retainFlag, remainingLength);
    }
    
    /**
     * 获取第一个字节（包含包类型和标志位）
     *
     * @return 固定头部第一个字节
     */
    public byte getFirstByte() {
        int firstByte = (packetType.getValue() << 4);
        
        if (dupFlag) {
            firstByte |= 0x08;
        }
        
        firstByte |= (qos.getValue() << 1);
        
        if (retainFlag) {
            firstByte |= 0x01;
        }
        
        return (byte) firstByte;
    }
    
    /**
     * 创建PUBLISH包的固定头部
     *
     * @param dupFlag 重复标志
     * @param qos QoS等级
     * @param retainFlag 保留标志
     * @param remainingLength 剩余长度
     * @return PUBLISH包固定头部
     */
    public static MqttFixedHeader createPublishHeader(boolean dupFlag, MqttQos qos, 
                                                      boolean retainFlag, int remainingLength) {
        return new MqttFixedHeader(MqttPacketType.PUBLISH, dupFlag, qos, retainFlag, remainingLength);
    }
    
    /**
     * 创建PUBREL包的固定头部（固定QoS=1）
     *
     * @param remainingLength 剩余长度
     * @return PUBREL包固定头部
     */
    public static MqttFixedHeader createPubrelHeader(int remainingLength) {
        return new MqttFixedHeader(MqttPacketType.PUBREL, false, MqttQos.AT_LEAST_ONCE, false, remainingLength);
    }
    
    /**
     * 创建SUBSCRIBE包的固定头部（固定QoS=1）
     *
     * @param remainingLength 剩余长度
     * @return SUBSCRIBE包固定头部
     */
    public static MqttFixedHeader createSubscribeHeader(int remainingLength) {
        return new MqttFixedHeader(MqttPacketType.SUBSCRIBE, false, MqttQos.AT_LEAST_ONCE, false, remainingLength);
    }
    
    /**
     * 创建UNSUBSCRIBE包的固定头部（固定QoS=1）
     *
     * @param remainingLength 剩余长度
     * @return UNSUBSCRIBE包固定头部
     */
    public static MqttFixedHeader createUnsubscribeHeader(int remainingLength) {
        return new MqttFixedHeader(MqttPacketType.UNSUBSCRIBE, false, MqttQos.AT_LEAST_ONCE, false, remainingLength);
    }
    
    /**
     * 验证固定头部参数
     *
     * @param packetType 包类型
     * @param dupFlag DUP标志
     * @param qos QoS等级
     * @param retainFlag RETAIN标志
     * @param remainingLength 剩余长度
     * @throws IllegalArgumentException 如果参数无效
     */
    private static void validateFixedHeader(MqttPacketType packetType, boolean dupFlag, 
                                          MqttQos qos, boolean retainFlag, int remainingLength) {
        // 检查包类型
        if (packetType == null) {
            throw new IllegalArgumentException("Packet type cannot be null");
        }
        
        if (packetType == MqttPacketType.RESERVED_0) {
            throw new IllegalArgumentException("Reserved packet type cannot be used");
        }
        
        // 检查DUP标志
        if (dupFlag && !packetType.canHaveDupFlag()) {
            throw new IllegalArgumentException(
                String.format("Packet type %s cannot have DUP flag set", packetType.getName()));
        }
        
        // 检查QoS等级
        if (qos == null) {
            throw new IllegalArgumentException("QoS level cannot be null");
        }
        
        // 检查特定包类型的QoS限制
        validateQosForPacketType(packetType, qos);
        
        // 检查RETAIN标志
        validateRetainForPacketType(packetType, retainFlag);
        
        // 检查剩余长度
        if (remainingLength < 0) {
            throw new IllegalArgumentException("Remaining length cannot be negative");
        }
        
        if (remainingLength > 268435455) { // 2^28 - 1
            throw new IllegalArgumentException("Remaining length cannot exceed 268435455 bytes");
        }
    }
    
    /**
     * 验证特定包类型的QoS等级
     *
     * @param packetType 包类型
     * @param qos QoS等级
     */
    private static void validateQosForPacketType(MqttPacketType packetType, MqttQos qos) {
        switch (packetType) {
            case PUBREL, SUBSCRIBE, UNSUBSCRIBE -> {
                if (qos != MqttQos.AT_LEAST_ONCE) {
                    throw new IllegalArgumentException(
                        String.format("Packet type %s must have QoS=1", packetType.getName()));
                }
            }
            case CONNECT, CONNACK, PUBACK, PUBREC, PUBCOMP, SUBACK, UNSUBACK, 
                 PINGREQ, PINGRESP, DISCONNECT, AUTH -> {
                if (qos != MqttQos.AT_MOST_ONCE) {
                    throw new IllegalArgumentException(
                        String.format("Packet type %s must have QoS=0", packetType.getName()));
                }
            }
            // PUBLISH包可以有任意QoS等级
            case PUBLISH -> { /* 允许任意QoS */ }
            default -> throw new IllegalArgumentException("Unknown packet type: " + packetType);
        }
    }
    
    /**
     * 验证特定包类型的RETAIN标志
     *
     * @param packetType 包类型
     * @param retainFlag RETAIN标志
     */
    private static void validateRetainForPacketType(MqttPacketType packetType, boolean retainFlag) {
        if (retainFlag && packetType != MqttPacketType.PUBLISH) {
            throw new IllegalArgumentException(
                String.format("Packet type %s cannot have RETAIN flag set", packetType.getName()));
        }
    }
    
    @Override
    public String toString() {
        return String.format("MqttFixedHeader{type=%s, dup=%s, qos=%s, retain=%s, remainingLength=%d}",
                           packetType.getName(), dupFlag, qos, retainFlag, remainingLength);
    }
}