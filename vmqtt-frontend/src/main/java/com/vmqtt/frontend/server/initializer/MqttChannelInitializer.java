/**
 * MQTT通道初始化器
 *
 * @author zhenglin
 * @date 2025/08/06
 */
package com.vmqtt.frontend.server.initializer;

import com.vmqtt.frontend.config.NettyServerConfig;
import com.vmqtt.frontend.server.handler.MqttConnectionHandler;
import com.vmqtt.frontend.server.handler.MqttProtocolHandler;
import com.vmqtt.frontend.server.handler.SslContextHandler;
import com.vmqtt.common.protocol.codec.MqttCodecFactory;
import com.vmqtt.common.protocol.codec.MqttChannelHandlerAdapter;
import com.vmqtt.common.protocol.codec.MqttProtocolVersionManager;
import com.vmqtt.common.protocol.MqttVersion;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.ssl.SslHandler;
import io.netty.handler.timeout.IdleStateHandler;
import io.netty.handler.traffic.GlobalTrafficShapingHandler;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.stereotype.Component;

import java.util.concurrent.TimeUnit;

/**
 * MQTT Channel Pipeline初始化器
 * 配置完整的网络处理链路
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class MqttChannelInitializer extends ChannelInitializer<SocketChannel> implements InitializingBean {
    
    private final NettyServerConfig config;
    private final SslContextHandler sslHandler;
    private final MqttConnectionHandler connectionHandler;
    private final MqttProtocolHandler protocolHandler;
    private final GlobalTrafficShapingHandler trafficHandler;
    
    @Override
    protected void initChannel(SocketChannel ch) throws Exception {
        ChannelPipeline pipeline = ch.pipeline();
        
        // 1. 流量整形处理器（可选）
        if (config.getConnection().isRateLimitEnabled()) {
            pipeline.addLast("traffic", trafficHandler);
        }
        
        // 2. SSL/TLS处理器（如果启用）
        if (config.getServer().isSslEnabled()) {
            pipeline.addLast("ssl", sslHandler.createSslHandler());
        }
        
        // 3. 空闲状态检测处理器
        pipeline.addLast("idle", new IdleStateHandler(
            config.getConnection().getIdleTimeout(), // 读空闲时间
            config.getConnection().getIdleTimeout(), // 写空闲时间  
            config.getConnection().getIdleTimeout() * 2, // 读写空闲时间
            TimeUnit.SECONDS
        ));
        
        // 4. MQTT协议有自己的帧格式，不需要额外的帧长度检测
        
        // 5. MQTT协议编解码器适配器 - 支持动态版本协商
        pipeline.addLast("mqtt-codec", new MqttChannelHandlerAdapter());
        
        // 6. 连接管理处理器
        pipeline.addLast("connection", connectionHandler);
        
        // 7. MQTT协议处理器
        pipeline.addLast("mqtt", protocolHandler);
        
        log.debug("初始化MQTT通道Pipeline: {}", ch.remoteAddress());
    }
    
    /**
     * Spring Bean初始化后回调
     */
    @Override
    public void afterPropertiesSet() throws Exception {
        log.info("开始初始化MQTT通道初始化器");
        
        // 预热MQTT编解码器缓存
        MqttProtocolVersionManager.warmupCodecCache();
        
        log.info("MQTT通道初始化器初始化完成");
    }
    
    /**
     * 处理初始化异常
     */
    @Override
    public void exceptionCaught(io.netty.channel.ChannelHandlerContext ctx, Throwable cause) {
        log.error("MQTT通道初始化失败: {}", ctx.channel().remoteAddress(), cause);
        ctx.close();
    }
    
    /**
     * 获取编解码器统计信息
     *
     * @return 统计信息
     */
    public String getCodecStats() {
        return MqttProtocolVersionManager.getCodecStats();
    }
}