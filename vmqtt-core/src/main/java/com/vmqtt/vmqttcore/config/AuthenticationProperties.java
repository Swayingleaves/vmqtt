/**
 * 认证配置属性
 *
 * @author zhenglin
 * @date 2025/08/14
 */
package com.vmqtt.vmqttcore.config;

import com.vmqtt.common.config.AuthenticationConfig;
import lombok.Data;
import lombok.EqualsAndHashCode;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

/**
 * 认证配置属性 - Spring Boot配置绑定
 * 继承AuthenticationConfig以复用基础配置结构
 */
@Data
@EqualsAndHashCode(callSuper = true)
@Component
@ConfigurationProperties(prefix = "vmqtt.auth")
public class AuthenticationProperties extends AuthenticationConfig {
    // 这个类主要用于Spring Boot的配置属性绑定
    // 实际的配置逻辑在父类AuthenticationConfig中
}