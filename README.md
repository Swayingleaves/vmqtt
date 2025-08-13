# V-MQTT 高性能MQTT服务器

基于Java 21、Spring Boot和Netty构建的高性能MQTT服务器，支持单机百万连接和集群亿级连接的消息吞吐能力。

## 🎯 核心目标

- **高性能**: 单机百万连接，集群亿级连接
- **多协议**: 支持MQTT 3.x/5.x协议
- **高可靠**: RocksDB持久化，数据零丢失
- **高安全**: TLS/SSL + ACL授权机制
- **可扩展**: 集群自动发现和负载均衡

## 🏗️ 系统架构

### 核心架构模式
采用**前后端分离**的分层架构设计：

```
┌─────────────────────────────────────────────────────┐
│                   客户端连接层                        │
├─────────────────────────────────────────────────────┤
│  vmqtt-frontend (连接处理 + 协议解析)                  │
│  - Netty服务器 + 虚拟线程                             │
│  - MQTT协议处理                                      │
│  - 连接管理和认证                                     │
├─────────────────────────────────────────────────────┤
│  vmqtt-core (核心协调服务)                            │
│  - 消息路由和分发                                     │
│  - 集群协调                                          │
│  - 负载均衡                                          │
├─────────────────────────────────────────────────────┤
│  vmqtt-backend (存储和业务服务)                        │
│  - RocksDB持久化                                     │
│  - 会话管理                                          │
│  - QoS消息处理                                       │
└─────────────────────────────────────────────────────┘
```

### Maven模块结构

```
vmqtt-parent/                    # 父POM，依赖版本管理
├── vmqtt-common/                # 通用组件和gRPC接口定义
│   ├── src/main/proto/          # Protocol Buffers定义
│   ├── protocol/                # MQTT协议实现
│   └── model/                   # 核心数据模型
├── vmqtt-frontend/              # 连接处理层(Netty服务器)
│   ├── server/                  # Netty服务器配置
│   └── handler/                 # MQTT协议处理器
├── vmqtt-backend/               # 数据存储层(RocksDB)
│   ├── storage/                 # 存储引擎
│   └── service/                 # 持久化服务
└── vmqtt-core/                  # 核心协调层(Spring Boot应用)
    ├── service/                 # 消息路由服务
    └── cluster/                 # 集群管理
```

## 🔧 技术栈

### 核心技术
- **Java 21**: 虚拟线程支持
- **Spring Boot 3.2.0**: 应用框架
- **Netty 4.1.104.Final**: 网络通信
- **gRPC 1.58.0**: 内部服务通信
- **RocksDB 8.8.1**: 数据持久化
- **Protocol Buffers 3.24.4**: 数据序列化

### 性能优化
- **ZGC**: 低延迟垃圾回收器
- **虚拟线程**: 高并发处理
- **零拷贝**: Netty网络优化
- **内存池**: 对象复用机制

## 🚀 快速开始

### 环境要求
- **Java**: JDK 21+
- **Maven**: 3.8.0+
- **操作系统**: Linux/macOS/Windows

### 1. 构建项目

```bash
# 克隆项目
git clone https://github.com/your-repo/v-mqtt.git
cd v-mqtt

# 编译整个项目
mvn clean compile

# 生成protobuf代码
mvn clean generate-sources

# 打包项目(跳过测试)
mvn clean package -DskipTests
```

### 2. 运行单体模式

```bash
# 运行vmqtt-core(核心服务)
cd vmqtt-core
mvn spring-boot:run

# 或使用jar包运行
java -jar target/vmqtt-core-*.jar
```

### 3. 运行分布式模式

**启动Backend服务(数据存储)**
```bash
cd vmqtt-backend
mvn spring-boot:run
# 或 java -jar target/vmqtt-backend-*.jar
```

**启动Frontend服务(连接处理)**
```bash
cd vmqtt-frontend  
mvn spring-boot:run
# 或 java -jar target/vmqtt-frontend-*.jar
```


### 4. 性能优化启动

**使用ZGC和虚拟线程启动:**
```bash
# 使用优化脚本启动
chmod +x scripts/start-with-zgc.sh
./scripts/start-with-zgc.sh

# 或手动指定JVM参数
java -XX:+UseZGC \
     -XX:+UnlockExperimentalVMOptions \
     --enable-preview \
     -Xmx8g \
     -XX:MaxDirectMemorySize=4g \
     -jar vmqtt-core/target/vmqtt-core-*.jar
```

## 📋 常用命令

### Maven操作
```bash
# 编译所有模块
mvn clean compile

# 运行测试
mvn test

# 只编译protobuf文件
mvn -pl vmqtt-common protobuf:compile protobuf:compile-custom

# 运行单个模块测试
mvn -pl vmqtt-core test

# 切换Java版本(如果需要)
jenv local 21
```

### 应用操作
```bash
# 查看应用状态
curl http://localhost:8080/actuator/health

# 查看性能指标  
curl http://localhost:8080/actuator/metrics

# 停止应用
pkill -f vmqtt-core
```

## 🌐 服务端口

| 服务 | 端口 | 协议 | 说明 |
|-----|------|------|------|
| vmqtt-frontend | 1883 | MQTT | MQTT标准端口 |
| vmqtt-frontend | 8883 | MQTTS | MQTT over TLS |
| vmqtt-core | 8080 | HTTP | 管理和监控 |
| vmqtt-backend | 9090 | gRPC | 内部存储服务 |

## 🔧 配置文件

### 核心配置 (vmqtt-core)
```yaml
# application.yml
server:
  port: 8080

vmqtt:
  mqtt:
    port: 1883
    max-connections: 1000000
  cluster:
    enabled: true
    nodes: ["node1:8080", "node2:8080"]
```

### Frontend配置 (vmqtt-frontend)  
```yaml
# application.yml
netty:
  server:
    port: 1883
    worker-threads: -1  # 使用虚拟线程
    max-connections: 1000000
```

### Backend配置 (vmqtt-backend)
```yaml
# application.yml
rocksdb:
  data-path: ./data/rocksdb
  enable-statistics: true
  max-background-jobs: 4
```

## 📊 性能指标

### 单机性能目标
- **并发连接数**: 100万
- **消息吞吐量**: 100万条/秒  
- **消息延迟**: P99 < 10ms
- **CPU使用率**: < 70%
- **内存使用**: < 8GB

### 集群性能目标
- **总连接数**: 1亿
- **总吞吐量**: 1000万条/秒
- **水平扩展**: 支持100+节点
- **故障恢复**: < 30秒

## 🧪 测试

### 单元测试
```bash
# 运行所有测试
mvn test

# 运行单个模块测试
mvn -pl vmqtt-core test

# 运行单个测试类
mvn -Dtest=MessageRoutingEngineTest test
```

### MQTT客户端测试
```bash
# 使用mosquitto客户端测试
mosquitto_pub -h localhost -p 1883 -t test/topic -m "Hello V-MQTT"
mosquitto_sub -h localhost -p 1883 -t test/topic
```

## 📚 相关文档

- [TASK.md](TASK.md) - 详细的开发计划和进度
- [CLAUDE.md](CLAUDE.md) - 开发指导文档  
- [PERFORMANCE_OPTIMIZATION_SUMMARY.md](PERFORMANCE_OPTIMIZATION_SUMMARY.md) - 性能优化总结

## 🤝 贡献指南

1. Fork 本仓库
2. 创建特性分支 (`git checkout -b feature/AmazingFeature`)
3. 提交更改 (`git commit -m 'feat: add some AmazingFeature'`)
4. 推送到分支 (`git push origin feature/AmazingFeature`)
5. 开启 Pull Request

## 📄 许可证

本项目基于 MIT 许可证开源 - 查看 [LICENSE](LICENSE) 文件了解详情。
