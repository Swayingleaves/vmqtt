/**
 * MQTT订阅确认包负载
 *
 * @author zhenglin
 * @date 2025/08/06
 */
package com.vmqtt.common.protocol.packet.suback;

import java.util.List;
import java.util.ArrayList;
import java.util.Collections;

/**
 * MQTT SUBACK包负载定义
 * 
 * 包含订阅返回码列表
 */
public record MqttSubackPayload(List<MqttSubackReturnCode> returnCodes) {
    
    /**
     * 构造函数验证
     */
    public MqttSubackPayload {
        if (returnCodes == null) {
            throw new IllegalArgumentException("Return codes cannot be null");
        }
        
        if (returnCodes.isEmpty()) {
            throw new IllegalArgumentException("Return codes cannot be empty");
        }
        
        // 创建不可变副本
        returnCodes = Collections.unmodifiableList(new ArrayList<>(returnCodes));
        
        // 验证所有返回码
        for (MqttSubackReturnCode returnCode : returnCodes) {
            if (returnCode == null) {
                throw new IllegalArgumentException("Return code cannot be null");
            }
        }
    }
    
    /**
     * 获取返回码数量
     *
     * @return 返回码数量
     */
    public int getReturnCodeCount() {
        return returnCodes.size();
    }
    
    /**
     * 获取指定索引的返回码
     *
     * @param index 索引
     * @return 返回码
     */
    public MqttSubackReturnCode getReturnCode(int index) {
        return returnCodes.get(index);
    }
    
    /**
     * 获取返回码字节数组
     *
     * @return 返回码字节数组
     */
    public byte[] getReturnCodeBytes() {
        byte[] bytes = new byte[returnCodes.size()];
        for (int i = 0; i < returnCodes.size(); i++) {
            bytes[i] = returnCodes.get(i).getValue();
        }
        return bytes;
    }
    
    /**
     * 检查所有返回码是否都成功
     *
     * @return 如果所有返回码都成功返回true
     */
    public boolean allSuccess() {
        return returnCodes.stream().allMatch(MqttSubackReturnCode::isSuccess);
    }
    
    /**
     * 检查是否有失败的返回码
     *
     * @return 如果有失败的返回码返回true
     */
    public boolean hasFailure() {
        return returnCodes.stream().anyMatch(MqttSubackReturnCode::isFailure);
    }
    
    /**
     * 获取成功返回码的数量
     *
     * @return 成功数量
     */
    public long getSuccessCount() {
        return returnCodes.stream().filter(MqttSubackReturnCode::isSuccess).count();
    }
    
    /**
     * 获取失败返回码的数量
     *
     * @return 失败数量
     */
    public long getFailureCount() {
        return returnCodes.stream().filter(MqttSubackReturnCode::isFailure).count();
    }
    
    /**
     * 从字节数组创建负载
     *
     * @param bytes 返回码字节数组
     * @return SUBACK负载
     */
    public static MqttSubackPayload fromBytes(byte[] bytes) {
        if (bytes == null || bytes.length == 0) {
            throw new IllegalArgumentException("Return code bytes cannot be null or empty");
        }
        
        List<MqttSubackReturnCode> returnCodes = new ArrayList<>();
        for (byte b : bytes) {
            returnCodes.add(MqttSubackReturnCode.fromValue(b));
        }
        
        return new MqttSubackPayload(returnCodes);
    }
    
    /**
     * 创建成功的负载
     *
     * @param qosLevels QoS等级数组
     * @return 成功负载
     */
    public static MqttSubackPayload success(int... qosLevels) {
        List<MqttSubackReturnCode> returnCodes = new ArrayList<>();
        for (int qos : qosLevels) {
            switch (qos) {
                case 0 -> returnCodes.add(MqttSubackReturnCode.MAXIMUM_QOS_0);
                case 1 -> returnCodes.add(MqttSubackReturnCode.MAXIMUM_QOS_1);
                case 2 -> returnCodes.add(MqttSubackReturnCode.MAXIMUM_QOS_2);
                default -> throw new IllegalArgumentException("Invalid QoS level: " + qos);
            }
        }
        return new MqttSubackPayload(returnCodes);
    }
    
    /**
     * 创建失败的负载
     *
     * @param count 返回码数量
     * @return 失败负载
     */
    public static MqttSubackPayload failure(int count) {
        if (count <= 0) {
            throw new IllegalArgumentException("Count must be positive");
        }
        
        List<MqttSubackReturnCode> returnCodes = Collections.nCopies(count, MqttSubackReturnCode.FAILURE);
        return new MqttSubackPayload(returnCodes);
    }
    
    @Override
    public String toString() {
        return String.format("MqttSubackPayload{returnCodesCount=%d, success=%d, failure=%d}",
                           getReturnCodeCount(), getSuccessCount(), getFailureCount());
    }
}