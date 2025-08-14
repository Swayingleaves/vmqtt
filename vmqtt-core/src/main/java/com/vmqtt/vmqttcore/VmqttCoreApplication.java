/**
 * V-MQTT核心应用启动类
 *
 * @author zhenglin
 * @date 2025/08/06
 */
package com.vmqtt.vmqttcore;

import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.ComponentScan;

/**
 * V-MQTT服务器核心应用
 * 基于Spring Boot的高性能MQTT服务器核心协调层
 */
@Slf4j
@SpringBootApplication
@ComponentScan(basePackages = {
    "com.vmqtt.vmqttcore.config",
    "com.vmqtt.vmqttcore.controller", 
    "com.vmqtt.vmqttcore.service",
    "com.vmqtt.common.service.impl"
})
public class VmqttCoreApplication {

    public static void main(String[] args) {
        try {
            log.info("启动V-MQTT核心服务器...");
            log.info("Java版本: {}", System.getProperty("java.version"));
            log.info("Spring Boot版本: {}", SpringBootVersion.getVersion());
            
            SpringApplication app = new SpringApplication(VmqttCoreApplication.class);
            
            // 设置应用程序属性
            app.setLogStartupInfo(true);
            
            // 启动应用
            var context = app.run(args);
            
            log.info("V-MQTT核心服务器启动成功!");
            log.info("可通过以下端点访问服务状态:");
            log.info("  - 健康检查: http://localhost:8080/api/v1/service/health");
            log.info("  - 就绪检查: http://localhost:8080/api/v1/service/ready");
            log.info("  - 服务状态: http://localhost:8080/api/v1/service/status");
            log.info("  - 服务信息: http://localhost:8080/api/v1/service/info");
            log.info("  - Actuator健康检查: http://localhost:8080/actuator/health");
            
        } catch (Exception e) {
            log.error("V-MQTT核心服务器启动失败", e);
            System.exit(1);
        }
    }

    /**
     * Spring Boot版本工具类
     */
    private static class SpringBootVersion {
        public static String getVersion() {
            Package pkg = SpringBootVersion.class.getPackage();
            return pkg != null ? pkg.getImplementationVersion() : "unknown";
        }
    }
}
