# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## 项目架构

V-MQTT 是一个基于 Java 21 和 Spring Boot 3.2.0 构建的高性能 MQTT 服务器，采用多模块 Maven 项目结构：

- **vmqtt-parent**: 父 POM，管理依赖版本和通用构建配置
- **vmqtt-common**: 通用模块，包含 gRPC 接口定义、协议定义和工具类
- **vmqtt-core**: 核心服务模块，Spring Boot 应用程序入口

### 关键技术栈
- Java 21
- Spring Boot 3.2.0
- Netty 4.1.104.Final (网络通信)
- gRPC 1.58.0 (内部服务通信)
- Protocol Buffers 3.24.4 (数据序列化)
- JUnit 5.10.1 + Mockito 5.7.0 (测试)

## 常用命令

### 构建和编译
```bash
# 编译整个项目
mvn clean compile

# 编译并生成 protobuf 代码
mvn clean generate-sources

# 打包项目
mvn clean package

# 跳过测试打包
mvn clean package -DskipTests
```

注意：如果mvn打包失败，可以使用`jenv local 21` 切换到java21环境，再执行mvn命令。

### 运行应用
```bash
# 运行 vmqtt-core 应用
cd vmqtt-core
mvn spring-boot:run

# 或使用打包后的 jar
java -jar vmqtt-core/target/vmqtt-core-*.jar
```

### 测试
```bash
# 运行所有测试
mvn test

# 运行单个模块测试
mvn -pl vmqtt-core test

# 运行单个测试类
mvn -Dtest=VmqttCoreApplicationTests test
```

### Protocol Buffers
```bash
# 仅编译 protobuf 文件
mvn -pl vmqtt-common protobuf:compile protobuf:compile-custom
```

## 模块说明

### vmqtt-common
- 包含 gRPC 服务定义和生成的代码
- Protocol Buffers 文件应放在 `src/main/proto/` 目录
- 生成的 Java 代码位于 `target/generated-sources/protobuf/`
- 包含通用工具类和协议定义

### vmqtt-core
- Spring Boot 主应用程序
- 主类：`com.vmqtt.vmqttcore.VmqttCoreApplication`
- 配置文件：`src/main/resources/application.yml`

## 开发指南

### 添加新的 gRPC 服务
1. 在 `vmqtt-common/src/main/proto/` 中定义 .proto 文件
2. 运行 `mvn generate-sources` 生成 Java 代码
3. 在 vmqtt-core 中实现服务接口

### 依赖管理
- 所有版本在父 POM 的 `<properties>` 中统一管理
- 使用 `<dependencyManagement>` 确保版本一致性
- 新依赖应先在父 POM 中声明版本

### Maven 配置特点
- 使用 protobuf-maven-plugin 自动编译 .proto 文件
- 配置了 os-maven-plugin 支持跨平台编译
- Spring Boot Maven Plugin 用于打包可执行 jar

## 项目状态
这是一个正在开发中的项目，基础架构已搭建完成，包含：
- Maven 多模块结构
- gRPC 和 Protocol Buffers 支持 
- Spring Boot 基础配置
- 基本的测试框架

# TASK
请基于CLAUDE.md和TASK.md，完成预定的任务，如果完成了任务请更新TASK里的状态