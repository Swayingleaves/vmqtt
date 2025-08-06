/**
 * 监控指标工具类
 *
 * @author zhenglin
 * @date 2025/08/06
 */
package com.vmqtt.common.util;

import io.micrometer.core.instrument.*;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;

/**
 * 监控指标管理工具类
 */
public class MetricsUtils {
    
    /**
     * 默认度量注册表
     */
    private static volatile MeterRegistry meterRegistry = new SimpleMeterRegistry();
    
    // 连接相关指标
    private static final AtomicLong ACTIVE_CONNECTIONS = new AtomicLong(0);
    private static final Counter TOTAL_CONNECTIONS = Counter.builder("mqtt.connections.total")
            .description("Total number of MQTT connections")
            .register(meterRegistry);
    private static final Counter FAILED_CONNECTIONS = Counter.builder("mqtt.connections.failed")
            .description("Number of failed MQTT connections")
            .register(meterRegistry);
    
    // 消息相关指标
    private static final Counter MESSAGES_PUBLISHED = Counter.builder("mqtt.messages.published")
            .description("Number of published messages")
            .register(meterRegistry);
    private static final Counter MESSAGES_DELIVERED = Counter.builder("mqtt.messages.delivered")
            .description("Number of delivered messages")
            .register(meterRegistry);
    private static final Counter MESSAGES_DROPPED = Counter.builder("mqtt.messages.dropped")
            .description("Number of dropped messages")
            .register(meterRegistry);
    
    // 延迟相关指标
    private static final Timer MESSAGE_PROCESSING_TIME = Timer.builder("mqtt.message.processing.time")
            .description("Message processing time")
            .register(meterRegistry);
    private static final Timer CONNECTION_DURATION = Timer.builder("mqtt.connection.duration")
            .description("Connection duration")
            .register(meterRegistry);
    
    /**
     * 设置度量注册表
     *
     * @param registry 度量注册表
     */
    public static void setMeterRegistry(MeterRegistry registry) {
        if (registry != null) {
            meterRegistry = registry;
        }
    }
    
    /**
     * 获取度量注册表
     *
     * @return 度量注册表
     */
    public static MeterRegistry getMeterRegistry() {
        return meterRegistry;
    }
    
    // ==================== 连接相关指标 ====================
    
    /**
     * 增加活跃连接数
     *
     * @return 当前活跃连接数
     */
    public static long incrementActiveConnections() {
        long current = ACTIVE_CONNECTIONS.incrementAndGet();
        Gauge.builder("mqtt.connections.active", ACTIVE_CONNECTIONS, AtomicLong::get)
                .description("Number of active MQTT connections")
                .register(meterRegistry);
        return current;
    }
    
    /**
     * 减少活跃连接数
     *
     * @return 当前活跃连接数
     */
    public static long decrementActiveConnections() {
        return ACTIVE_CONNECTIONS.decrementAndGet();
    }
    
    /**
     * 获取活跃连接数
     *
     * @return 活跃连接数
     */
    public static long getActiveConnections() {
        return ACTIVE_CONNECTIONS.get();
    }
    
    /**
     * 记录总连接数
     */
    public static void recordTotalConnection() {
        TOTAL_CONNECTIONS.increment();
    }
    
    /**
     * 记录失败连接数
     */
    public static void recordFailedConnection() {
        FAILED_CONNECTIONS.increment();
    }
    
    /**
     * 记录连接持续时间
     *
     * @param duration 持续时间
     * @param timeUnit 时间单位
     */
    public static void recordConnectionDuration(long duration, TimeUnit timeUnit) {
        CONNECTION_DURATION.record(duration, timeUnit);
    }
    
    // ==================== 消息相关指标 ====================
    
    /**
     * 记录发布消息数
     *
     * @param count 消息数量
     */
    public static void recordPublishedMessages(long count) {
        MESSAGES_PUBLISHED.increment(count);
    }
    
    /**
     * 记录发布消息数
     */
    public static void recordPublishedMessage() {
        MESSAGES_PUBLISHED.increment();
    }
    
    /**
     * 记录传递消息数
     *
     * @param count 消息数量
     */
    public static void recordDeliveredMessages(long count) {
        MESSAGES_DELIVERED.increment(count);
    }
    
    /**
     * 记录传递消息数
     */
    public static void recordDeliveredMessage() {
        MESSAGES_DELIVERED.increment();
    }
    
    /**
     * 记录丢弃消息数
     *
     * @param count 消息数量
     */
    public static void recordDroppedMessages(long count) {
        MESSAGES_DROPPED.increment(count);
    }
    
    /**
     * 记录丢弃消息数
     */
    public static void recordDroppedMessage() {
        MESSAGES_DROPPED.increment();
    }
    
    // ==================== 性能相关指标 ====================
    
    /**
     * 记录消息处理时间
     *
     * @param duration 处理时间
     * @param timeUnit 时间单位
     */
    public static void recordMessageProcessingTime(long duration, TimeUnit timeUnit) {
        MESSAGE_PROCESSING_TIME.record(duration, timeUnit);
    }
    
    /**
     * 创建定时器采样
     *
     * @return 定时器采样
     */
    public static Timer.Sample startMessageProcessingTimer() {
        return Timer.start(meterRegistry);
    }
    
    /**
     * 停止定时器采样
     *
     * @param sample 定时器采样
     */
    public static void stopMessageProcessingTimer(Timer.Sample sample) {
        sample.stop(MESSAGE_PROCESSING_TIME);
    }
    
    // ==================== 自定义指标 ====================
    
    /**
     * 创建计数器
     *
     * @param name 指标名称
     * @param description 指标描述
     * @param tags 标签
     * @return 计数器
     */
    public static Counter createCounter(String name, String description, String... tags) {
        return Counter.builder(name)
                .description(description)
                .tags(tags)
                .register(meterRegistry);
    }
    
    /**
     * 创建定时器
     *
     * @param name 指标名称
     * @param description 指标描述
     * @param tags 标签
     * @return 定时器
     */
    public static Timer createTimer(String name, String description, String... tags) {
        return Timer.builder(name)
                .description(description)
                .tags(tags)
                .register(meterRegistry);
    }
    
    /**
     * 创建仪表盘
     *
     * @param name 指标名称
     * @param description 指标描述
     * @param supplier 数值提供者
     * @param tags 标签
     * @return 仪表盘
     */
    public static <T> Gauge createGauge(String name, String description, 
                                       Supplier<Number> supplier, String... tags) {
        return Gauge.builder(name, supplier, s -> s.get().doubleValue())
                .description(description)
                .tags(tags)
                .register(meterRegistry);
    }
    
    /**
     * 创建分布摘要
     *
     * @param name 指标名称
     * @param description 指标描述
     * @param tags 标签
     * @return 分布摘要
     */
    public static DistributionSummary createDistributionSummary(String name, String description, 
                                                                String... tags) {
        return DistributionSummary.builder(name)
                .description(description)
                .tags(tags)
                .register(meterRegistry);
    }
    
    // ==================== QoS相关指标 ====================
    
    /**
     * 记录QoS消息数
     *
     * @param qos QoS级别
     */
    public static void recordQosMessage(int qos) {
        Counter.builder("mqtt.messages.qos")
                .description("Messages by QoS level")
                .tag("qos", String.valueOf(qos))
                .register(meterRegistry)
                .increment();
    }
    
    // ==================== Topic相关指标 ====================
    
    /**
     * 记录主题订阅数
     *
     * @param topic 主题
     */
    public static void recordTopicSubscription(String topic) {
        Counter.builder("mqtt.subscriptions.topic")
                .description("Subscriptions by topic")
                .tag("topic", sanitizeTopicForMetrics(topic))
                .register(meterRegistry)
                .increment();
    }
    
    /**
     * 记录主题发布数
     *
     * @param topic 主题
     */
    public static void recordTopicPublication(String topic) {
        Counter.builder("mqtt.publications.topic")
                .description("Publications by topic")
                .tag("topic", sanitizeTopicForMetrics(topic))
                .register(meterRegistry)
                .increment();
    }
    
    /**
     * 清理主题名称用于指标
     *
     * @param topic 原始主题
     * @return 清理后的主题
     */
    private static String sanitizeTopicForMetrics(String topic) {
        if (topic == null || topic.isEmpty()) {
            return "unknown";
        }
        // 替换特殊字符，避免指标名称问题
        return topic.replaceAll("[^a-zA-Z0-9/_-]", "_");
    }
    
    // ==================== 系统指标 ====================
    
    /**
     * 注册JVM指标
     */
    public static void registerJvmMetrics() {
        // 内存指标
        Gauge.builder("jvm.memory.used", Runtime.getRuntime(), 
                     r -> r.totalMemory() - r.freeMemory())
                .description("Used JVM memory")
                .register(meterRegistry);
        
        Gauge.builder("jvm.memory.free", Runtime.getRuntime(), Runtime::freeMemory)
                .description("Free JVM memory")
                .register(meterRegistry);
        
        Gauge.builder("jvm.memory.total", Runtime.getRuntime(), Runtime::totalMemory)
                .description("Total JVM memory")
                .register(meterRegistry);
        
        Gauge.builder("jvm.memory.max", Runtime.getRuntime(), Runtime::maxMemory)
                .description("Max JVM memory")
                .register(meterRegistry);
        
        // 线程指标
        Gauge.builder("jvm.threads.active", Thread.class, t -> Thread.activeCount())
                .description("Active threads")
                .register(meterRegistry);
    }
    
    /**
     * 清理所有指标
     */
    public static void clear() {
        meterRegistry.clear();
        ACTIVE_CONNECTIONS.set(0);
    }
}