/**
 * MQTT前端服务器应用程序
 *
 * @author zhenglin
 * @date 2025/08/08
 */
package com.vmqtt.frontend;

import com.vmqtt.frontend.server.NettyMqttServer;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.scheduling.annotation.EnableAsync;
import org.springframework.scheduling.annotation.EnableScheduling;

/**
 * V-MQTT前端服务器主应用程序
 * 基于Spring Boot和Netty的高性能MQTT服务器
 */
@Slf4j
@SpringBootApplication(scanBasePackages = {"com.vmqtt.frontend", "com.vmqtt.common"})
@EnableConfigurationProperties
@EnableAsync
@EnableScheduling
@RequiredArgsConstructor
public class VmqttFrontendApplication implements CommandLineRunner {
    
    private final NettyMqttServer mqttServer;

    public static void main(String[] args) {
        // 设置系统属性
        setupSystemProperties();
        
        // 启动Spring应用
        SpringApplication app = new SpringApplication(VmqttFrontendApplication.class);
        
        // 配置应用属性
        configureApplication(app);
        
        app.run(args);
    }
    
    @Override
    public void run(String... args) throws Exception {
        log.info("=================================================");
        log.info("           启动 V-MQTT 前端服务器");
        log.info("=================================================");
        
        try {
            // 启动Netty MQTT服务器
            mqttServer.start().get();
            
            log.info("=================================================");
            log.info("         V-MQTT 前端服务器启动成功");
            log.info("=================================================");
            
            // 添加关闭钩子
            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                log.info("正在关闭V-MQTT前端服务器...");
                try {
                    mqttServer.stop().get();
                    log.info("V-MQTT前端服务器已关闭");
                } catch (Exception e) {
                    log.error("关闭V-MQTT前端服务器时发生错误", e);
                }
            }));
            
        } catch (Exception e) {
            log.error("启动V-MQTT前端服务器失败", e);
            System.exit(1);
        }
    }
    
    /**
     * 设置系统属性
     */
    private static void setupSystemProperties() {
        // 启用虚拟线程（Java 21+）
        System.setProperty("java.util.concurrent.ForkJoinPool.common.parallelism", 
            String.valueOf(Runtime.getRuntime().availableProcessors() * 2));
        
        // Netty优化
        System.setProperty("io.netty.leakDetection.level", "SIMPLE");
        System.setProperty("io.netty.allocator.type", "pooled");
        System.setProperty("io.netty.allocator.numDirectArenas", 
            String.valueOf(Runtime.getRuntime().availableProcessors() * 2));
        
        // 垃圾回收优化
        if (isZGCAvailable()) {
            System.setProperty("java.util.concurrent.ForkJoinPool.common.maximumSpares", "256");
        }
        
        log.info("系统属性配置完成");
    }
    
    /**
     * 配置Spring应用
     */
    private static void configureApplication(SpringApplication app) {
        // 设置默认配置文件
        app.setAdditionalProfiles("frontend");
        
        // 禁用Web服务器（使用Netty作为网络层）
        // app.setWebApplicationType(WebApplicationType.NONE);
        
        log.info("Spring应用配置完成");
    }
    
    /**
     * 检查ZGC是否可用
     */
    private static boolean isZGCAvailable() {
        try {
            String javaVersion = System.getProperty("java.version");
            if (javaVersion != null) {
                String[] parts = javaVersion.split("\\.");
                int majorVersion = Integer.parseInt(parts[0]);
                return majorVersion >= 17; // ZGC在Java 17+中稳定可用
            }
        } catch (Exception e) {
            log.debug("检查ZGC可用性时发生错误", e);
        }
        return false;
    }
}