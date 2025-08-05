/**
 * MQTT集群管理服务器应用程序
 *
 * @author zhenglin
 * @date 2025/08/05
 */
package com.vmqtt.cluster;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class VmqttClusterApplication {

    public static void main(String[] args) {
        SpringApplication.run(VmqttClusterApplication.class, args);
    }

}