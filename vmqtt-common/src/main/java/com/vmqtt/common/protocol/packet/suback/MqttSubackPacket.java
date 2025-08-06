/**
 * MQTT订阅确认包
 *
 * @author zhenglin
 * @date 2025/08/06
 */
package com.vmqtt.common.protocol.packet.suback;

import com.vmqtt.common.protocol.packet.MqttFixedHeader;
import com.vmqtt.common.protocol.packet.MqttPacket;
import com.vmqtt.common.protocol.packet.MqttPacketType;
import com.vmqtt.common.protocol.packet.MqttPacketWithId;

import java.util.List;

/**
 * MQTT SUBACK包定义
 * 
 * 服务器响应客户端SUBSCRIBE请求的确认包
 */
public record MqttSubackPacket(
        MqttFixedHeader fixedHeader,
        MqttSubackVariableHeader variableHeader,
        MqttSubackPayload payload) implements MqttPacket, MqttPacketWithId {
    
    @Override
    public MqttFixedHeader getFixedHeader() {
        return fixedHeader;
    }
    
    /**
     * 构造函数验证
     */
    public MqttSubackPacket {
        if (fixedHeader.packetType() != MqttPacketType.SUBACK) {
            throw new IllegalArgumentException("Invalid packet type for SUBACK packet");
        }
        
        if (variableHeader == null) {
            throw new IllegalArgumentException("Variable header cannot be null for SUBACK packet");
        }
        
        if (payload == null) {
            throw new IllegalArgumentException("Payload cannot be null for SUBACK packet");
        }
        
        // SUBACK包必须有包标识符
        MqttPacketWithId.validatePacketId(variableHeader.packetId());
        
        // SUBACK包负载不能为空
        if (payload.returnCodes().isEmpty()) {
            throw new IllegalArgumentException("SUBACK packet must contain at least one return code");
        }
    }
    
    @Override
    public int getPacketId() {
        return variableHeader.packetId();
    }
    
    /**
     * 获取返回码列表
     *
     * @return 返回码列表
     */
    public List<MqttSubackReturnCode> getReturnCodes() {
        return payload.returnCodes();
    }
    
    /**
     * 检查是否有属性（MQTT 5.0）
     *
     * @return 如果有属性返回true
     */
    public boolean hasProperties() {
        return variableHeader.hasProperties();
    }
    
    /**
     * 获取属性（MQTT 5.0）
     *
     * @return 属性对象
     */
    public Object getProperties() {
        return variableHeader.properties();
    }
    
    /**
     * 检查所有订阅是否都成功
     *
     * @return 如果所有订阅都成功返回true
     */
    public boolean allSubscriptionsSucceeded() {
        return payload.returnCodes().stream().allMatch(MqttSubackReturnCode::isSuccess);
    }
    
    /**
     * 检查是否有订阅失败
     *
     * @return 如果有订阅失败返回true
     */
    public boolean hasFailedSubscriptions() {
        return payload.returnCodes().stream().anyMatch(MqttSubackReturnCode::isFailure);
    }
    
    /**
     * 获取成功订阅的数量
     *
     * @return 成功订阅数量
     */
    public int getSuccessfulSubscriptionsCount() {
        return (int) payload.returnCodes().stream().filter(MqttSubackReturnCode::isSuccess).count();
    }
    
    /**
     * 获取失败订阅的数量
     *
     * @return 失败订阅数量
     */
    public int getFailedSubscriptionsCount() {
        return (int) payload.returnCodes().stream().filter(MqttSubackReturnCode::isFailure).count();
    }
    
    /**
     * 创建SUBACK包
     *
     * @param packetId 包标识符
     * @param returnCodes 返回码列表
     * @return SUBACK包
     */
    public static MqttSubackPacket create(int packetId, List<MqttSubackReturnCode> returnCodes) {
        return create(packetId, returnCodes, null);
    }
    
    /**
     * 创建MQTT 5.0的SUBACK包
     *
     * @param packetId 包标识符
     * @param returnCodes 返回码列表
     * @param properties 属性
     * @return SUBACK包
     */
    public static MqttSubackPacket create(int packetId, List<MqttSubackReturnCode> returnCodes, Object properties) {
        // 计算剩余长度
        int remainingLength = 2; // 包标识符
        
        if (properties != null) {
            // MQTT 5.0属性长度计算
            remainingLength += 1; // 暂时占位
        }
        
        remainingLength += returnCodes.size(); // 返回码
        
        // 创建固定头部
        MqttFixedHeader fixedHeader = new MqttFixedHeader(MqttPacketType.SUBACK, remainingLength);
        
        // 创建可变头部
        MqttSubackVariableHeader variableHeader = new MqttSubackVariableHeader(packetId, properties);
        
        // 创建负载
        MqttSubackPayload payload = new MqttSubackPayload(returnCodes);
        
        return new MqttSubackPacket(fixedHeader, variableHeader, payload);
    }
    
    /**
     * 创建成功的SUBACK包
     *
     * @param packetId 包标识符
     * @param qosLevels QoS等级列表
     * @return 成功的SUBACK包
     */
    public static MqttSubackPacket createSuccess(int packetId, List<Integer> qosLevels) {
        List<MqttSubackReturnCode> returnCodes = qosLevels.stream()
                .map(qos -> switch (qos) {
                    case 0 -> MqttSubackReturnCode.MAXIMUM_QOS_0;
                    case 1 -> MqttSubackReturnCode.MAXIMUM_QOS_1;
                    case 2 -> MqttSubackReturnCode.MAXIMUM_QOS_2;
                    default -> throw new IllegalArgumentException("Invalid QoS level: " + qos);
                })
                .toList();
        
        return create(packetId, returnCodes);
    }
    
    /**
     * 创建失败的SUBACK包
     *
     * @param packetId 包标识符
     * @param subscriptionCount 订阅数量
     * @return 失败的SUBACK包
     */
    public static MqttSubackPacket createFailure(int packetId, int subscriptionCount) {
        List<MqttSubackReturnCode> returnCodes = java.util.Collections.nCopies(
            subscriptionCount, MqttSubackReturnCode.FAILURE
        );
        
        return create(packetId, returnCodes);
    }
    
    @Override
    public String toString() {
        return String.format("MqttSubackPacket{packetId=%d, returnCodesCount=%d, successful=%d, failed=%d}",
                           getPacketId(), getReturnCodes().size(), 
                           getSuccessfulSubscriptionsCount(), getFailedSubscriptionsCount());
    }
}