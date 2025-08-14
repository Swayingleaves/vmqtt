/**
 * 前端认证配置
 *
 * @author zhenglin
 * @date 2025/08/14
 */
package com.vmqtt.frontend.config;

import com.vmqtt.common.config.AuthenticationConfig;
import com.vmqtt.common.service.AuthenticationConfigManager;
import com.vmqtt.common.service.AuthenticationService;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * 前端认证配置
 * 为前端模块提供默认的认证配置Bean
 */
@Configuration
public class FrontendAuthConfig {
    
    /**
     * 默认认证配置Bean
     * 只有在没有其他认证配置Bean时才会创建
     *
     * @return 默认认证配置
     */
    @Bean
    @ConditionalOnMissingBean(AuthenticationConfig.class)
    public AuthenticationConfig authenticationConfig() {
        AuthenticationConfig config = new AuthenticationConfig();
        config.setEnabled(false); // 前端模块默认不启用认证
        config.setAllowAnonymous(true); // 允许匿名连接
        config.init(); // 初始化默认值
        return config;
    }
    
    /**
     * 认证配置管理器Bean
     * 只有在没有其他认证配置管理器Bean时才会创建
     *
     * @param authConfig 认证配置
     * @param authService 认证服务
     * @return 认证配置管理器
     */
    @Bean
    @ConditionalOnMissingBean(AuthenticationConfigManager.class)
    public AuthenticationConfigManager authenticationConfigManager(
            AuthenticationConfig authConfig,
            AuthenticationService authService) {
        return new AuthenticationConfigManager(authConfig, authService);
    }
}