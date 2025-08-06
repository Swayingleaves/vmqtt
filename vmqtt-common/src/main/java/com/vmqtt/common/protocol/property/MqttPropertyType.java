/**
 * MQTT 5.0属性类型
 *
 * @author zhenglin
 * @date 2025/08/06
 */
package com.vmqtt.common.protocol.property;

/**
 * MQTT 5.0属性数据类型枚举
 * 
 * 定义MQTT 5.0规范中的所有属性数据类型
 */
public enum MqttPropertyType {
    /**
     * 字节类型（1字节）
     */
    BYTE(1, "Byte", "Single byte value"),
    
    /**
     * 双字节整数类型（2字节）
     */
    TWO_BYTE_INTEGER(2, "Two Byte Integer", "16-bit unsigned integer in big-endian order"),
    
    /**
     * 四字节整数类型（4字节）
     */
    FOUR_BYTE_INTEGER(4, "Four Byte Integer", "32-bit unsigned integer in big-endian order"),
    
    /**
     * 可变长度字节整数类型（1-4字节）
     */
    VARIABLE_BYTE_INTEGER(-1, "Variable Byte Integer", "Variable length integer (1 to 4 bytes)"),
    
    /**
     * 二进制数据类型（2字节长度 + 数据）
     */
    BINARY_DATA(-2, "Binary Data", "Two byte length followed by binary data"),
    
    /**
     * UTF-8字符串类型（2字节长度 + UTF-8字符串）
     */
    UTF8_STRING(-3, "UTF-8 String", "Two byte length followed by UTF-8 encoded string"),
    
    /**
     * UTF-8字符串对类型（两个UTF-8字符串）
     */
    UTF8_STRING_PAIR(-4, "UTF-8 String Pair", "Two UTF-8 strings (name and value)");
    
    /**
     * 固定大小（-1表示可变长度）
     */
    private final int fixedSize;
    
    /**
     * 类型名称
     */
    private final String typeName;
    
    /**
     * 类型描述
     */
    private final String description;
    
    /**
     * 构造函数
     *
     * @param fixedSize 固定大小（-1表示可变长度）
     * @param typeName 类型名称
     * @param description 类型描述
     */
    MqttPropertyType(int fixedSize, String typeName, String description) {
        this.fixedSize = fixedSize;
        this.typeName = typeName;
        this.description = description;
    }
    
    /**
     * 获取固定大小
     *
     * @return 固定大小，-1表示可变长度
     */
    public int getFixedSize() {
        return fixedSize;
    }
    
    /**
     * 获取类型名称
     *
     * @return 类型名称
     */
    public String getTypeName() {
        return typeName;
    }
    
    /**
     * 获取类型描述
     *
     * @return 类型描述
     */
    public String getDescription() {
        return description;
    }
    
    /**
     * 检查是否为固定长度类型
     *
     * @return 如果是固定长度返回true
     */
    public boolean isFixedLength() {
        return fixedSize > 0;
    }
    
    /**
     * 检查是否为可变长度类型
     *
     * @return 如果是可变长度返回true
     */
    public boolean isVariableLength() {
        return fixedSize < 0;
    }
    
    /**
     * 检查是否为整数类型
     *
     * @return 如果是整数类型返回true
     */
    public boolean isIntegerType() {
        return switch (this) {
            case BYTE, TWO_BYTE_INTEGER, FOUR_BYTE_INTEGER, VARIABLE_BYTE_INTEGER -> true;
            default -> false;
        };
    }
    
    /**
     * 检查是否为字符串类型
     *
     * @return 如果是字符串类型返回true
     */
    public boolean isStringType() {
        return switch (this) {
            case UTF8_STRING, UTF8_STRING_PAIR -> true;
            default -> false;
        };
    }
    
    /**
     * 检查是否为二进制类型
     *
     * @return 如果是二进制类型返回true
     */
    public boolean isBinaryType() {
        return this == BINARY_DATA;
    }
    
    @Override
    public String toString() {
        return String.format("%s: %s", typeName, description);
    }
}