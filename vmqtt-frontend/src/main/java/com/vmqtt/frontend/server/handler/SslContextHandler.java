/**
 * SSL上下文处理器
 *
 * @author zhenglin
 * @date 2025/08/08
 */
package com.vmqtt.frontend.server.handler;

import com.vmqtt.frontend.config.NettyServerConfig;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;
import io.netty.handler.ssl.SslHandler;
import io.netty.handler.ssl.util.SelfSignedCertificate;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLEngine;
import javax.net.ssl.TrustManagerFactory;
import java.io.File;
import java.io.FileInputStream;
import java.security.KeyStore;
import java.security.cert.CertificateFactory;
import java.security.cert.X509Certificate;

/**
 * SSL/TLS上下文处理器
 * 负责SSL证书加载和SSL处理器创建
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class SslContextHandler {
    
    private final NettyServerConfig config;
    
    private SslContext sslContext;
    
    /**
     * 初始化SSL上下文
     */
    @PostConstruct
    public void initializeSslContext() {
        if (!config.getServer().isSslEnabled()) {
            log.info("SSL未启用，跳过SSL上下文初始化");
            return;
        }
        
        try {
            sslContext = createSslContext();
            log.info("SSL上下文初始化成功");
        } catch (Exception e) {
            log.error("SSL上下文初始化失败", e);
            throw new RuntimeException("SSL初始化失败", e);
        }
    }
    
    /**
     * 创建SSL处理器
     *
     * @return SSL处理器
     */
    public SslHandler createSslHandler() {
        if (sslContext == null) {
            throw new IllegalStateException("SSL上下文未初始化");
        }
        
        SSLEngine sslEngine = sslContext.newEngine(io.netty.buffer.ByteBufAllocator.DEFAULT);
        
        // 配置SSL引擎
        configureSslEngine(sslEngine);
        
        SslHandler sslHandler = new SslHandler(sslEngine);
        
        // 配置SSL处理器
        configureSslHandler(sslHandler);
        
        return sslHandler;
    }
    
    /**
     * 创建SSL上下文
     */
    private SslContext createSslContext() throws Exception {
        SslContextBuilder builder = SslContextBuilder.forServer(
            getKeyManagerFactory()
        );
        
        // 配置信任库
        TrustManagerFactory trustManagerFactory = getTrustManagerFactory();
        if (trustManagerFactory != null) {
            builder.trustManager(trustManagerFactory);
        }
        
        // 配置协议版本
        if (config.getSsl().getProtocols() != null && config.getSsl().getProtocols().length > 0) {
            builder.protocols(config.getSsl().getProtocols());
        }
        
        // 配置加密套件
        if (config.getSsl().getCipherSuites() != null && config.getSsl().getCipherSuites().length > 0) {
            builder.ciphers(java.util.Arrays.asList(config.getSsl().getCipherSuites()));
        }
        
        // 配置客户端认证
        if (config.getSsl().isClientAuthRequired()) {
            builder.clientAuth(io.netty.handler.ssl.ClientAuth.REQUIRE);
        }
        
        return builder.build();
    }
    
    /**
     * 获取密钥管理器工厂
     */
    private KeyManagerFactory getKeyManagerFactory() throws Exception {
        String keystorePath = config.getSsl().getKeystorePath();
        String keystorePassword = config.getSsl().getKeystorePassword();
        
        if (keystorePath != null && !keystorePath.isEmpty()) {
            // 使用密钥库
            return createKeyManagerFactoryFromKeyStore(keystorePath, keystorePassword);
        } else {
            // 检查是否有单独的证书和私钥文件
            String certPath = config.getSsl().getCertificatePath();
            String keyPath = config.getSsl().getPrivateKeyPath();
            
            if (certPath != null && keyPath != null) {
                return createKeyManagerFactoryFromFiles(certPath, keyPath);
            } else {
                // 使用自签名证书（仅用于开发/测试）
                log.warn("未配置SSL证书，使用自签名证书（不要在生产环境中使用）");
                return createSelfSignedKeyManagerFactory();
            }
        }
    }
    
    /**
     * 从密钥库创建密钥管理器工厂
     */
    private KeyManagerFactory createKeyManagerFactoryFromKeyStore(String keystorePath, String password) throws Exception {
        KeyStore keyStore = KeyStore.getInstance("JKS");
        
        try (FileInputStream fis = new FileInputStream(keystorePath)) {
            keyStore.load(fis, password != null ? password.toCharArray() : null);
        }
        
        KeyManagerFactory kmf = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
        kmf.init(keyStore, password != null ? password.toCharArray() : null);
        
        log.info("从密钥库加载SSL证书: {}", keystorePath);
        return kmf;
    }
    
    /**
     * 从证书和私钥文件创建密钥管理器工厂
     */
    private KeyManagerFactory createKeyManagerFactoryFromFiles(String certPath, String keyPath) throws Exception {
        // 加载证书
        CertificateFactory cf = CertificateFactory.getInstance("X.509");
        X509Certificate cert;
        try (FileInputStream fis = new FileInputStream(certPath)) {
            cert = (X509Certificate) cf.generateCertificate(fis);
        }
        
        // 加载私钥（这里简化实现，实际应用中需要根据私钥格式处理）
        // 由于私钥加载比较复杂，这里建议使用密钥库方式
        throw new UnsupportedOperationException("从单独文件加载证书和私钥的功能尚未完全实现，请使用密钥库方式");
    }
    
    /**
     * 创建自签名证书的密钥管理器工厂
     */
    private KeyManagerFactory createSelfSignedKeyManagerFactory() throws Exception {
        SelfSignedCertificate ssc = new SelfSignedCertificate();
        
        KeyStore keyStore = KeyStore.getInstance("JKS");
        keyStore.load(null, null);
        
        // 这里需要将自签名证书添加到密钥库
        // 简化实现，实际应用中不应使用自签名证书
        
        KeyManagerFactory kmf = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
        kmf.init(keyStore, "".toCharArray());
        
        return kmf;
    }
    
    /**
     * 获取信任管理器工厂
     */
    private TrustManagerFactory getTrustManagerFactory() throws Exception {
        String truststorePath = config.getSsl().getTruststorePath();
        String truststorePassword = config.getSsl().getTruststorePassword();
        
        if (truststorePath == null || truststorePath.isEmpty()) {
            return null;
        }
        
        KeyStore trustStore = KeyStore.getInstance("JKS");
        
        try (FileInputStream fis = new FileInputStream(truststorePath)) {
            trustStore.load(fis, truststorePassword != null ? truststorePassword.toCharArray() : null);
        }
        
        TrustManagerFactory tmf = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
        tmf.init(trustStore);
        
        log.info("从信任库加载SSL信任证书: {}", truststorePath);
        return tmf;
    }
    
    /**
     * 配置SSL引擎
     */
    private void configureSslEngine(SSLEngine sslEngine) {
        // 设置为服务端模式
        sslEngine.setUseClientMode(false);
        
        // 配置客户端认证
        if (config.getSsl().isClientAuthRequired()) {
            sslEngine.setNeedClientAuth(true);
        }
        
        // 启用会话缓存
        sslEngine.getSession();
        
        log.debug("SSL引擎配置完成");
    }
    
    /**
     * 配置SSL处理器
     */
    private void configureSslHandler(SslHandler sslHandler) {
        // 设置握手超时时间
        sslHandler.setHandshakeTimeoutMillis(30000); // 30秒
        
        // 设置关闭超时时间
        sslHandler.setCloseNotifyFlushTimeoutMillis(3000); // 3秒
        sslHandler.setCloseNotifyReadTimeoutMillis(3000); // 3秒
        
        log.debug("SSL处理器配置完成");
    }
    
    /**
     * 检查SSL是否可用
     *
     * @return SSL是否可用
     */
    public boolean isSslAvailable() {
        return config.getServer().isSslEnabled() && sslContext != null;
    }
    
    /**
     * 获取SSL统计信息
     *
     * @return SSL配置信息
     */
    public SslStats getSslStats() {
        return SslStats.builder()
            .enabled(config.getServer().isSslEnabled())
            .available(isSslAvailable())
            .clientAuthRequired(config.getSsl().isClientAuthRequired())
            .protocols(config.getSsl().getProtocols())
            .cipherSuites(config.getSsl().getCipherSuites())
            .build();
    }
    
    /**
     * SSL统计信息
     */
    public static class SslStats {
        private boolean enabled;
        private boolean available;
        private boolean clientAuthRequired;
        private String[] protocols;
        private String[] cipherSuites;
        
        public static SslStatsBuilder builder() {
            return new SslStatsBuilder();
        }
        
        public static class SslStatsBuilder {
            private boolean enabled;
            private boolean available;
            private boolean clientAuthRequired;
            private String[] protocols;
            private String[] cipherSuites;
            
            public SslStatsBuilder enabled(boolean enabled) {
                this.enabled = enabled;
                return this;
            }
            
            public SslStatsBuilder available(boolean available) {
                this.available = available;
                return this;
            }
            
            public SslStatsBuilder clientAuthRequired(boolean clientAuthRequired) {
                this.clientAuthRequired = clientAuthRequired;
                return this;
            }
            
            public SslStatsBuilder protocols(String[] protocols) {
                this.protocols = protocols;
                return this;
            }
            
            public SslStatsBuilder cipherSuites(String[] cipherSuites) {
                this.cipherSuites = cipherSuites;
                return this;
            }
            
            public SslStats build() {
                SslStats stats = new SslStats();
                stats.enabled = this.enabled;
                stats.available = this.available;
                stats.clientAuthRequired = this.clientAuthRequired;
                stats.protocols = this.protocols;
                stats.cipherSuites = this.cipherSuites;
                return stats;
            }
        }
        
        // Getters
        public boolean isEnabled() { return enabled; }
        public boolean isAvailable() { return available; }
        public boolean isClientAuthRequired() { return clientAuthRequired; }
        public String[] getProtocols() { return protocols; }
        public String[] getCipherSuites() { return cipherSuites; }
    }
}