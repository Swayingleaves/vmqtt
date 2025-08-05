/**
 * MQTT后端服务器应用程序
 *
 * @author zhenglin
 * @date 2025/08/05
 */
package com.vmqtt.backend;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class VmqttBackendApplication {

    public static void main(String[] args) {
        SpringApplication.run(VmqttBackendApplication.class, args);
    }

}