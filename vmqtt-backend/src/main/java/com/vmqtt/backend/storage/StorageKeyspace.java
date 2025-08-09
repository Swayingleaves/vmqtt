/**
 * RocksDB存储键值空间定义
 *
 * @author zhenglin
 * @date 2025/08/09
 */
package com.vmqtt.backend.storage;

import lombok.experimental.UtilityClass;

/**
 * 定义RocksDB中不同数据类型的键值空间规划
 * 采用前缀分离的方式，支持高效的范围查询和数据管理
 */
@UtilityClass
public class StorageKeyspace {
    
    /**
     * 键分隔符
     */
    public static final String KEY_SEPARATOR = ":";
    
    // ================== 会话相关键值空间 ==================
    
    /**
     * 会话状态前缀: session:{clientId}
     */
    public static final String SESSION_PREFIX = "session";
    
    /**
     * 会话索引前缀: session_idx:{state}:{clientId}  
     * 用于按状态索引会话
     */
    public static final String SESSION_INDEX_PREFIX = "session_idx";
    
    /**
     * 会话到期索引前缀: session_exp:{timestamp}:{clientId}
     * 用于清理过期会话
     */
    public static final String SESSION_EXPIRY_PREFIX = "session_exp";
    
    // ================== 消息相关键值空间 ==================
    
    /**
     * 消息存储前缀: msg:{messageId}
     */
    public static final String MESSAGE_PREFIX = "msg";
    
    /**
     * 客户端消息队列前缀: msg_queue:{clientId}:{priority}:{timestamp}:{messageId}
     * 用于客户端消息队列管理
     */
    public static final String MESSAGE_QUEUE_PREFIX = "msg_queue";
    
    /**
     * 传输中消息前缀: msg_inflight:{clientId}:{packetId}
     * 用于QoS 1/2消息状态跟踪
     */
    public static final String MESSAGE_INFLIGHT_PREFIX = "msg_inflight";
    
    /**
     * 消息到期索引前缀: msg_exp:{timestamp}:{messageId}
     * 用于清理过期消息
     */
    public static final String MESSAGE_EXPIRY_PREFIX = "msg_exp";
    
    // ================== 订阅相关键值空间 ==================
    
    /**
     * 订阅信息前缀: sub:{clientId}:{topicFilter}
     */
    public static final String SUBSCRIPTION_PREFIX = "sub";
    
    /**
     * 主题订阅者索引前缀: sub_topic:{topicFilter}:{clientId}
     * 用于主题发布时快速找到订阅者
     */
    public static final String TOPIC_SUBSCRIBERS_PREFIX = "sub_topic";
    
    // ================== 保留消息键值空间 ==================
    
    /**
     * 保留消息前缀: retain:{topic}
     */
    public static final String RETAINED_MESSAGE_PREFIX = "retain";
    
    /**
     * 保留消息索引前缀: retain_idx:{timestamp}:{topic}
     * 用于保留消息清理和管理
     */
    public static final String RETAINED_MESSAGE_INDEX_PREFIX = "retain_idx";
    
    // ================== 遗嘱消息键值空间 ==================
    
    /**
     * 遗嘱消息前缀: will:{clientId}
     */
    public static final String WILL_MESSAGE_PREFIX = "will";
    
    // ================== QoS消息状态键值空间 ==================
    
    /**
     * QoS消息状态前缀: qos:{clientId}:{packetId}
     */
    public static final String QOS_STATE_PREFIX = "qos";
    
    /**
     * QoS消息超时索引前缀: qos_timeout:{timestamp}:{clientId}:{packetId}
     * 用于QoS消息超时处理
     */
    public static final String QOS_TIMEOUT_PREFIX = "qos_timeout";
    
    // ================== 系统元数据键值空间 ==================
    
    /**
     * 系统配置前缀: sys_config:{configKey}
     */
    public static final String SYSTEM_CONFIG_PREFIX = "sys_config";
    
    /**
     * 统计信息前缀: stats:{statsKey}
     */
    public static final String STATISTICS_PREFIX = "stats";
    
    /**
     * 快照元数据前缀: snapshot:{snapshotId}
     */
    public static final String SNAPSHOT_PREFIX = "snapshot";
    
    // ================== 键构造方法 ==================
    
    /**
     * 构造会话键
     *
     * @param clientId 客户端ID
     * @return 会话键
     */
    public static String sessionKey(String clientId) {
        return SESSION_PREFIX + KEY_SEPARATOR + clientId;
    }
    
    /**
     * 构造会话状态索引键
     *
     * @param state 会话状态
     * @param clientId 客户端ID
     * @return 会话状态索引键
     */
    public static String sessionIndexKey(String state, String clientId) {
        return SESSION_INDEX_PREFIX + KEY_SEPARATOR + state + KEY_SEPARATOR + clientId;
    }
    
    /**
     * 构造会话过期索引键
     *
     * @param timestamp 过期时间戳
     * @param clientId 客户端ID
     * @return 会话过期索引键
     */
    public static String sessionExpiryKey(long timestamp, String clientId) {
        return SESSION_EXPIRY_PREFIX + KEY_SEPARATOR + 
               String.format("%019d", timestamp) + KEY_SEPARATOR + clientId;
    }
    
    /**
     * 构造消息键
     *
     * @param messageId 消息ID
     * @return 消息键
     */
    public static String messageKey(String messageId) {
        return MESSAGE_PREFIX + KEY_SEPARATOR + messageId;
    }
    
    /**
     * 构造消息队列键
     *
     * @param clientId 客户端ID
     * @param priority 消息优先级
     * @param timestamp 时间戳
     * @param messageId 消息ID
     * @return 消息队列键
     */
    public static String messageQueueKey(String clientId, int priority, long timestamp, String messageId) {
        return MESSAGE_QUEUE_PREFIX + KEY_SEPARATOR + clientId + KEY_SEPARATOR + 
               String.format("%010d", priority) + KEY_SEPARATOR + 
               String.format("%019d", timestamp) + KEY_SEPARATOR + messageId;
    }
    
    /**
     * 构造传输中消息键
     *
     * @param clientId 客户端ID
     * @param packetId 包ID
     * @return 传输中消息键
     */
    public static String messageInflightKey(String clientId, int packetId) {
        return MESSAGE_INFLIGHT_PREFIX + KEY_SEPARATOR + clientId + KEY_SEPARATOR + packetId;
    }
    
    /**
     * 构造消息过期索引键
     *
     * @param timestamp 过期时间戳
     * @param messageId 消息ID
     * @return 消息过期索引键
     */
    public static String messageExpiryKey(long timestamp, String messageId) {
        return MESSAGE_EXPIRY_PREFIX + KEY_SEPARATOR + 
               String.format("%019d", timestamp) + KEY_SEPARATOR + messageId;
    }
    
    /**
     * 构造订阅键
     *
     * @param clientId 客户端ID
     * @param topicFilter 主题过滤器
     * @return 订阅键
     */
    public static String subscriptionKey(String clientId, String topicFilter) {
        return SUBSCRIPTION_PREFIX + KEY_SEPARATOR + clientId + KEY_SEPARATOR + topicFilter;
    }
    
    /**
     * 构造主题订阅者索引键
     *
     * @param topicFilter 主题过滤器
     * @param clientId 客户端ID
     * @return 主题订阅者索引键
     */
    public static String topicSubscribersKey(String topicFilter, String clientId) {
        return TOPIC_SUBSCRIBERS_PREFIX + KEY_SEPARATOR + topicFilter + KEY_SEPARATOR + clientId;
    }
    
    /**
     * 构造保留消息键
     *
     * @param topic 主题
     * @return 保留消息键
     */
    public static String retainedMessageKey(String topic) {
        return RETAINED_MESSAGE_PREFIX + KEY_SEPARATOR + topic;
    }
    
    /**
     * 构造保留消息索引键
     *
     * @param timestamp 时间戳
     * @param topic 主题
     * @return 保留消息索引键
     */
    public static String retainedMessageIndexKey(long timestamp, String topic) {
        return RETAINED_MESSAGE_INDEX_PREFIX + KEY_SEPARATOR + 
               String.format("%019d", timestamp) + KEY_SEPARATOR + topic;
    }
    
    /**
     * 构造遗嘱消息键
     *
     * @param clientId 客户端ID
     * @return 遗嘱消息键
     */
    public static String willMessageKey(String clientId) {
        return WILL_MESSAGE_PREFIX + KEY_SEPARATOR + clientId;
    }
    
    /**
     * 构造QoS状态键
     *
     * @param clientId 客户端ID
     * @param packetId 包ID
     * @return QoS状态键
     */
    public static String qosStateKey(String clientId, int packetId) {
        return QOS_STATE_PREFIX + KEY_SEPARATOR + clientId + KEY_SEPARATOR + packetId;
    }
    
    /**
     * 构造QoS超时索引键
     *
     * @param timestamp 超时时间戳
     * @param clientId 客户端ID
     * @param packetId 包ID
     * @return QoS超时索引键
     */
    public static String qosTimeoutKey(long timestamp, String clientId, int packetId) {
        return QOS_TIMEOUT_PREFIX + KEY_SEPARATOR + 
               String.format("%019d", timestamp) + KEY_SEPARATOR + clientId + KEY_SEPARATOR + packetId;
    }
    
    /**
     * 构造系统配置键
     *
     * @param configKey 配置键
     * @return 系统配置键
     */
    public static String systemConfigKey(String configKey) {
        return SYSTEM_CONFIG_PREFIX + KEY_SEPARATOR + configKey;
    }
    
    /**
     * 构造统计信息键
     *
     * @param statsKey 统计键
     * @return 统计信息键
     */
    public static String statisticsKey(String statsKey) {
        return STATISTICS_PREFIX + KEY_SEPARATOR + statsKey;
    }
    
    /**
     * 构造快照键
     *
     * @param snapshotId 快照ID
     * @return 快照键
     */
    public static String snapshotKey(String snapshotId) {
        return SNAPSHOT_PREFIX + KEY_SEPARATOR + snapshotId;
    }
    
    /**
     * 获取键的前缀范围
     *
     * @param prefix 前缀
     * @return 前缀范围
     */
    public static String[] getPrefixRange(String prefix) {
        String start = prefix + KEY_SEPARATOR;
        String end = prefix + (char)(KEY_SEPARATOR.charAt(0) + 1);
        return new String[]{start, end};
    }
    
    /**
     * 检查键是否属于指定前缀
     *
     * @param key 键
     * @param prefix 前缀
     * @return 是否属于指定前缀
     */
    public static boolean hasPrefix(String key, String prefix) {
        return key.startsWith(prefix + KEY_SEPARATOR);
    }
    
    /**
     * 从键中提取客户端ID
     *
     * @param key 键
     * @param expectedPrefix 期望的前缀
     * @return 客户端ID，如果格式不匹配返回null
     */
    public static String extractClientId(String key, String expectedPrefix) {
        if (!hasPrefix(key, expectedPrefix)) {
            return null;
        }
        
        String[] parts = key.split(KEY_SEPARATOR);
        if (parts.length >= 2) {
            return parts[1];
        }
        
        return null;
    }
    
    /**
     * 验证键格式是否正确
     *
     * @param key 键
     * @param expectedPrefix 期望的前缀
     * @param expectedParts 期望的部分数量
     * @return 是否格式正确
     */
    public static boolean isValidKey(String key, String expectedPrefix, int expectedParts) {
        if (!hasPrefix(key, expectedPrefix)) {
            return false;
        }
        
        String[] parts = key.split(KEY_SEPARATOR);
        return parts.length == expectedParts;
    }
}