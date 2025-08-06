/**
 * MQTT服务器配置类
 *
 * @author zhenglin
 * @date 2025/08/06
 */
package com.vmqtt.common.config;

import lombok.Data;
import lombok.Builder;
import lombok.AllArgsConstructor;
import lombok.NoArgsConstructor;

/**
 * MQTT服务器全局配置
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class ServerConfig {
    
    /**
     * 服务器ID
     */
    private String serverId;
    
    /**
     * 服务器角色
     */
    private ServerRole role;
    
    /**
     * 网络配置
     */
    private NetworkConfig network;
    
    /**
     * 性能配置
     */
    private PerformanceConfig performance;
    
    /**
     * 存储配置
     */
    private StorageConfig storage;
    
    /**
     * 集群配置
     */
    private ClusterConfig cluster;
    
    /**
     * 安全配置
     */
    private SecurityConfig security;
    
    /**
     * 监控配置
     */
    private MonitoringConfig monitoring;
    
    /**
     * 服务器角色枚举
     */
    public enum ServerRole {
        /**
         * 前端服务器（处理连接和协议）
         */
        FRONTEND,
        
        /**
         * 后端服务器（处理存储和业务逻辑）
         */
        BACKEND,
        
        /**
         * 集群协调器
         */
        COORDINATOR,
        
        /**
         * 一体化服务器（包含所有功能）
         */
        ALL_IN_ONE
    }
    
    /**
     * 网络配置
     */
    @Data
    @Builder
    @NoArgsConstructor
    @AllArgsConstructor
    public static class NetworkConfig {
        /**
         * MQTT端口
         */
        @Builder.Default
        private int mqttPort = 1883;
        
        /**
         * MQTT over SSL端口
         */
        @Builder.Default
        private int mqttsPort = 8883;
        
        /**
         * WebSocket端口
         */
        @Builder.Default
        private int websocketPort = 8080;
        
        /**
         * WebSocket over SSL端口
         */
        @Builder.Default
        private int websocketSslPort = 8443;
        
        /**
         * QUIC端口
         */
        @Builder.Default
        private int quicPort = 14567;
        
        /**
         * 绑定地址
         */
        @Builder.Default
        private String bindAddress = "0.0.0.0";
        
        /**
         * SO_BACKLOG设置
         */
        @Builder.Default
        private int soBacklog = 1024;
        
        /**
         * SO_RCVBUF设置
         */
        @Builder.Default
        private int soRcvbuf = 65536;
        
        /**
         * SO_SNDBUF设置
         */
        @Builder.Default
        private int soSndbuf = 65536;
        
        /**
         * TCP_NODELAY设置
         */
        @Builder.Default
        private boolean tcpNodelay = true;
        
        /**
         * SO_KEEPALIVE设置
         */
        @Builder.Default
        private boolean soKeepalive = true;
        
        /**
         * SO_REUSEADDR设置
         */
        @Builder.Default
        private boolean soReuseaddr = true;
    }
    
    /**
     * 性能配置
     */
    @Data
    @Builder
    @NoArgsConstructor
    @AllArgsConstructor
    public static class PerformanceConfig {
        /**
         * Boss线程数
         */
        @Builder.Default
        private int bossThreads = 1;
        
        /**
         * Worker线程数
         */
        @Builder.Default
        private int workerThreads = Runtime.getRuntime().availableProcessors() * 2;
        
        /**
         * 业务线程池大小
         */
        @Builder.Default
        private int businessThreads = Runtime.getRuntime().availableProcessors() * 4;
        
        /**
         * 最大连接数
         */
        private int maxConnections = 1_000_000;
        
        /**
         * 连接空闲超时（秒）
         */
        private int idleTimeoutSeconds = 300;
        
        /**
         * 连接超时（秒）
         */
        private int connectTimeoutSeconds = 30;
        
        /**
         * 缓冲区大小
         */
        private int bufferSize = 8192;
        
        /**
         * 批处理大小
         */
        private int batchSize = 100;
        
        /**
         * 启用零拷贝
         */
        private boolean enableZeroCopy = true;
        
        /**
         * 启用直接内存
         */
        private boolean enableDirectMemory = true;
    }
    
    /**
     * 存储配置
     */
    @Data
    @Builder
    @NoArgsConstructor
    @AllArgsConstructor
    public static class StorageConfig {
        /**
         * 存储类型
         */
        private StorageType type = StorageType.ROCKSDB;
        
        /**
         * 数据目录
         */
        private String dataDir = "/data/vmqtt";
        
        /**
         * RocksDB配置
         */
        private RocksDBConfig rocksdb;
        
        /**
         * 消息保留时间（小时）
         */
        private int messageRetentionHours = 24;
        
        /**
         * 最大消息大小（字节）
         */
        private int maxMessageSize = 256 * 1024; // 256KB
        
        /**
         * 启用压缩
         */
        private boolean enableCompression = true;
        
        /**
         * 压缩算法
         */
        private CompressionType compressionType = CompressionType.SNAPPY;
        
        /**
         * 存储类型枚举
         */
        public enum StorageType {
            MEMORY, ROCKSDB, DISTRIBUTED
        }
        
        /**
         * 压缩类型枚举
         */
        public enum CompressionType {
            NONE, GZIP, SNAPPY, LZ4, ZSTD
        }
    }
    
    /**
     * RocksDB配置
     */
    @Data
    @Builder
    @NoArgsConstructor
    @AllArgsConstructor
    public static class RocksDBConfig {
        /**
         * 写缓冲区大小
         */
        private long writeBufferSize = 128 * 1024 * 1024L; // 128MB
        
        /**
         * 最大写缓冲区数量
         */
        private int maxWriteBufferNumber = 6;
        
        /**
         * 数据库写缓冲区大小
         */
        private long dbWriteBufferSize = 512 * 1024 * 1024L; // 512MB
        
        /**
         * 最大后台任务数
         */
        private int maxBackgroundJobs = Runtime.getRuntime().availableProcessors();
        
        /**
         * 允许并发内存表写入
         */
        private boolean allowConcurrentMemtableWrites = true;
        
        /**
         * 压缩类型
         */
        private String compressionType = "SNAPPY_COMPRESSION";
        
        /**
         * 最底层压缩类型
         */
        private String bottommostCompressionType = "ZSTD_COMPRESSION";
        
        /**
         * 块缓存大小
         */
        private long blockCacheSize = 256 * 1024 * 1024L; // 256MB
        
        /**
         * 启用统计信息
         */
        private boolean enableStatistics = true;
    }
    
    /**
     * 集群配置
     */
    @Data
    @Builder
    @NoArgsConstructor
    @AllArgsConstructor
    public static class ClusterConfig {
        /**
         * 启用集群模式
         */
        private boolean enabled = false;
        
        /**
         * 节点ID
         */
        private String nodeId;
        
        /**
         * 节点地址
         */
        private String nodeAddress;
        
        /**
         * 节点端口
         */
        private int nodePort = 9090;
        
        /**
         * 种子节点列表
         */
        private String[] seedNodes;
        
        /**
         * Raft选举超时（毫秒）
         */
        private int electionTimeoutMs = 5000;
        
        /**
         * Raft心跳间隔（毫秒）
         */
        private int heartbeatIntervalMs = 1000;
        
        /**
         * 快照间隔（毫秒）
         */
        private long snapshotIntervalMs = 3600000L; // 1小时
        
        /**
         * 日志最大大小（字节）
         */
        private long logMaxSize = 10 * 1024 * 1024L; // 10MB
        
        /**
         * 健康检查间隔（毫秒）
         */
        private int healthCheckIntervalMs = 5000;
    }
    
    /**
     * 安全配置
     */
    @Data
    @Builder
    @NoArgsConstructor
    @AllArgsConstructor
    public static class SecurityConfig {
        /**
         * 启用SSL/TLS
         */
        private boolean enableSsl = false;
        
        /**
         * SSL证书路径
         */
        private String sslCertPath;
        
        /**
         * SSL私钥路径
         */
        private String sslKeyPath;
        
        /**
         * SSL密码
         */
        private String sslPassword;
        
        /**
         * 启用客户端认证
         */
        private boolean enableClientAuth = true;
        
        /**
         * 认证类型
         */
        private AuthType authType = AuthType.USERNAME_PASSWORD;
        
        /**
         * 启用ACL
         */
        private boolean enableAcl = true;
        
        /**
         * ACL配置文件路径
         */
        private String aclConfigPath;
        
        /**
         * 认证类型枚举
         */
        public enum AuthType {
            NONE, USERNAME_PASSWORD, TOKEN, CERTIFICATE, OAUTH2
        }
    }
    
    /**
     * 监控配置
     */
    @Data
    @Builder
    @NoArgsConstructor
    @AllArgsConstructor
    public static class MonitoringConfig {
        /**
         * 启用监控
         */
        private boolean enabled = true;
        
        /**
         * Prometheus端口
         */
        private int prometheusPort = 9090;
        
        /**
         * 指标收集间隔（秒）
         */
        private int metricsIntervalSeconds = 10;
        
        /**
         * 启用JVM指标
         */
        private boolean enableJvmMetrics = true;
        
        /**
         * 启用系统指标
         */
        private boolean enableSystemMetrics = true;
        
        /**
         * 启用应用指标
         */
        private boolean enableApplicationMetrics = true;
        
        /**
         * 日志级别
         */
        private LogLevel logLevel = LogLevel.INFO;
        
        /**
         * 日志级别枚举
         */
        public enum LogLevel {
            TRACE, DEBUG, INFO, WARN, ERROR
        }
    }
    
    /**
     * 创建默认配置
     *
     * @return 默认服务器配置
     */
    public static ServerConfig createDefault() {
        return ServerConfig.builder()
                .serverId("vmqtt-server-001")
                .role(ServerRole.ALL_IN_ONE)
                .network(NetworkConfig.builder().build())
                .performance(PerformanceConfig.builder().build())
                .storage(StorageConfig.builder()
                        .rocksdb(RocksDBConfig.builder().build())
                        .build())
                .cluster(ClusterConfig.builder().build())
                .security(SecurityConfig.builder().build())
                .monitoring(MonitoringConfig.builder().build())
                .build();
    }
}