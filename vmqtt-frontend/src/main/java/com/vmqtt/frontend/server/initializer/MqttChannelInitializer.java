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
import com.vmqtt.common.protocol.codec.MqttDecoder;
import com.vmqtt.common.protocol.codec.MqttEncoder;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.timeout.IdleStateHandler;
import io.netty.handler.traffic.GlobalTrafficShapingHandler;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.util.concurrent.TimeUnit;

/**
 * MQTT Channel Pipeline初始化器
 * 配置完整的网络处理链路
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class MqttChannelInitializer extends ChannelInitializer<SocketChannel> {
    
    private final NettyServerConfig config;
    private final SslContextHandler sslHandler;
    private final MqttConnectionHandler connectionHandler;
    private final MqttProtocolHandler protocolHandler;
    private final GlobalTrafficShapingHandler trafficHandler;
    
    @Override
    protected void initChannel(SocketChannel ch) throws Exception {
        ChannelPipeline pipeline = ch.pipeline();
        
        // 1. 流量整形处理器（可选）
        if (config.isRateLimitEnabled()) {
            pipeline.addLast("traffic", trafficHandler);
        }
        
        // 2. SSL/TLS处理器（如果启用）
        if (config.isSslEnabled()) {
            pipeline.addLast("ssl", sslHandler);
        }
        
        // 3. 空闲状态检测处理器
        pipeline.addLast("idle", new IdleStateHandler(
            config.getIdleTimeout(), // 读空闲时间
            config.getIdleTimeout(), // 写空闲时间  
            config.getKeepAliveTimeout(), // 读写空闲时间
            TimeUnit.SECONDS
        ));
        
        // 4. MQTT协议编解码器
        pipeline.addLast("decoder", new MqttDecoder(config.getMaxFrameLength()));
        pipeline.addLast("encoder", new MqttEncoder());
        
        // 5. 连接管理处理器
        pipeline.addLast("connection", connectionHandler);
        
        // 6. MQTT协议处理器
        pipeline.addLast("mqtt", protocolHandler);
        
        log.debug("初始化MQTT通道Pipeline: {}", ch.remoteAddress());
    }
    
    /**
     * 处理初始化异常
     */
    @Override
    public void exceptionCaught(io.netty.channel.ChannelHandlerContext ctx, Throwable cause) {
        log.error("MQTT通道初始化失败: {}", ctx.channel().remoteAddress(), cause);
        ctx.close();
    }
}