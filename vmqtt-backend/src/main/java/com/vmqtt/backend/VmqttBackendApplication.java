/**
 * V-MQTT后端服务启动类
 *
 * @author zhenglin
 * @date 2025/08/09
 */
package com.vmqtt.backend;

import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableAsync;
import org.springframework.scheduling.annotation.EnableScheduling;

/**
 * V-MQTT后端服务应用程序
 * 提供gRPC存储服务接口和持久化功能
 */
@Slf4j
@EnableAsync
@EnableScheduling
@SpringBootApplication
public class VmqttBackendApplication {
    
    public static void main(String[] args) {
        try {
            log.info("Starting V-MQTT Backend Application...");
            
            SpringApplication app = new SpringApplication(VmqttBackendApplication.class);
            
            // 设置默认配置文件
            app.setAdditionalProfiles("backend");
            
            app.run(args);
            
            log.info("V-MQTT Backend Application started successfully!");
            
        } catch (Exception e) {
            log.error("Failed to start V-MQTT Backend Application", e);
            System.exit(1);
        }
    }
}