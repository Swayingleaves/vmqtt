/**
 * MQTT编解码器适配器
 *
 * @author zhenglin
 * @date 2025/08/13
 */
package com.vmqtt.common.protocol.codec;

import com.vmqtt.common.protocol.MqttVersion;
import com.vmqtt.common.protocol.error.MqttProtocolErrorHandler;
import com.vmqtt.common.protocol.packet.MqttPacket;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ByteToMessageCodec;
import io.netty.handler.codec.DecoderException;
import io.netty.handler.codec.EncoderException;
import lombok.extern.slf4j.Slf4j;

import java.util.List;

/**
 * MQTT通道处理器适配器
 * 
 * 将MQTT编解码器包装为Netty的ChannelHandler，支持动态协议版本切换
 */
@Slf4j
public class MqttChannelHandlerAdapter extends ByteToMessageCodec<MqttPacket> {
    
    // 最大消息长度 (256MB - 1字节)
    private static final int MAX_MESSAGE_SIZE = 268435455;
    
    // 初始读缓冲区大小
    private static final int INITIAL_READ_BUFFER_SIZE = 1024;
    
    /**
     * 解码入站字节流为MQTT数据包
     */
    @Override
    protected void decode(ChannelHandlerContext ctx, ByteBuf in, List<Object> out) throws Exception {
        if (in == null || !in.isReadable()) {
            return;
        }
        
        // 检查并协商协议版本
        if (!MqttProtocolVersionManager.isVersionNegotiated(ctx.channel())) {
            if (!negotiateProtocolVersion(ctx, in)) {
                // 版本协商失败或数据不足，等待更多数据
                return;
            }
        }
        
        // 获取编解码器对
        MqttCodecFactory.MqttCodecPair codecPair = MqttProtocolVersionManager.getCodecPair(ctx.channel());
        if (codecPair == null) {
            log.error("编解码器未初始化: {}", ctx.channel().remoteAddress());
            throw new DecoderException("MQTT编解码器未初始化");
        }
        
        try {
            // 标记读取位置，以便在解码失败时回滚
            int readerIndexBefore = in.readerIndex();
            
            // 尝试解码MQTT数据包
            MqttDecoder decoder = codecPair.decoder();
            MqttPacket packet = decoder.decode(in);
            
            if (packet != null) {
                // 解码成功，添加到输出列表
                out.add(packet);
                
                log.debug("解码MQTT数据包成功: {} from {}", 
                    packet.getFixedHeader().packetType(), ctx.channel().remoteAddress());
                    
                // 更新统计信息
                updateDecodeStats(ctx, packet);
                
            } else {
                // 数据不完整，回滚读取位置，等待更多数据
                in.readerIndex(readerIndexBefore);
                log.debug("MQTT数据包不完整，等待更多数据: {}", ctx.channel().remoteAddress());
            }
            
        } catch (MqttCodecException e) {
            log.error("MQTT解码失败: {}", ctx.channel().remoteAddress(), e);
            throw new DecoderException("MQTT解码失败: " + e.getMessage(), e);
            
        } catch (Exception e) {
            log.error("解码过程中发生未预期异常: {}", ctx.channel().remoteAddress(), e);
            throw new DecoderException("解码过程中发生异常", e);
        }
    }
    
    /**
     * 编码出站MQTT数据包为字节流
     */
    @Override
    protected void encode(ChannelHandlerContext ctx, MqttPacket packet, ByteBuf out) throws Exception {
        if (packet == null) {
            return;
        }
        
        // 获取编解码器对
        MqttCodecFactory.MqttCodecPair codecPair = MqttProtocolVersionManager.getCodecPair(ctx.channel());
        if (codecPair == null) {
            log.error("编解码器未初始化: {}", ctx.channel().remoteAddress());
            throw new EncoderException("MQTT编解码器未初始化");
        }
        
        try {
            // 记录编码前的写入位置
            int writerIndexBefore = out.writerIndex();
            
            // 编码MQTT数据包
            MqttEncoder encoder = codecPair.encoder();
            encoder.encode(packet, out);
            
            // 检查编码后的数据长度
            int encodedLength = out.writerIndex() - writerIndexBefore;
            if (encodedLength > MAX_MESSAGE_SIZE) {
                log.error("编码后的MQTT消息过大: {} 字节, 最大允许: {} 字节", 
                    encodedLength, MAX_MESSAGE_SIZE);
                throw new EncoderException("MQTT消息过大: " + encodedLength + " 字节");
            }
            
            log.debug("编码MQTT数据包成功: {} ({} 字节) to {}", 
                packet.getFixedHeader().packetType(), encodedLength, ctx.channel().remoteAddress());
                
            // 更新统计信息
            updateEncodeStats(ctx, packet, encodedLength);
            
        } catch (MqttCodecException e) {
            log.error("MQTT编码失败: {}", ctx.channel().remoteAddress(), e);
            throw new EncoderException("MQTT编码失败: " + e.getMessage(), e);
            
        } catch (Exception e) {
            log.error("编码过程中发生未预期异常: {}", ctx.channel().remoteAddress(), e);
            throw new EncoderException("编码过程中发生异常", e);
        }
    }
    
    /**
     * 协商MQTT协议版本
     */
    private boolean negotiateProtocolVersion(ChannelHandlerContext ctx, ByteBuf buffer) {
        try {
            // 检测协议版本
            MqttVersion detectedVersion = MqttProtocolVersionDetector.detectVersion(buffer);
            
            if (detectedVersion == null) {
                // 无法检测版本，可能数据不完整
                log.debug("无法检测MQTT协议版本，等待更多数据: {}", ctx.channel().remoteAddress());
                return false;
            }
            
            // 协商协议版本
            MqttVersion negotiatedVersion = MqttProtocolVersionManager.negotiateVersion(ctx.channel(), detectedVersion);
            
            if (negotiatedVersion == null) {
                log.error("MQTT协议版本协商失败: detected={}, channel={}", 
                    detectedVersion, ctx.channel().remoteAddress());
                ctx.close();
                return false;
            }
            
            log.info("MQTT协议版本协商成功: {} for channel: {}", 
                negotiatedVersion, ctx.channel().remoteAddress());
            return true;
            
        } catch (Exception e) {
            log.error("协商MQTT协议版本时发生异常: {}", ctx.channel().remoteAddress(), e);
            ctx.close();
            return false;
        }
    }
    
    /**
     * 更新解码统计信息
     */
    private void updateDecodeStats(ChannelHandlerContext ctx, MqttPacket packet) {
        // 这里可以添加性能统计逻辑
        // 例如：消息计数、QPS统计、错误率等
    }
    
    /**
     * 更新编码统计信息
     */
    private void updateEncodeStats(ChannelHandlerContext ctx, MqttPacket packet, int encodedLength) {
        // 这里可以添加性能统计逻辑
        // 例如：消息计数、流量统计、压缩比等
    }
    
    /**
     * 处理异常
     */
    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
        log.error("MQTT编解码器异常: {}", ctx.channel().remoteAddress(), cause);
        
        // 使用错误处理器处理异常
        boolean shouldCloseConnection = MqttProtocolErrorHandler.handleException(ctx.channel(), cause);
        
        if (shouldCloseConnection) {
            // 重置协议版本
            MqttProtocolVersionManager.resetProtocolVersion(ctx.channel());
            // 关闭连接
            ctx.close();
        } else {
            // 传播给下游处理器
            super.exceptionCaught(ctx, cause);
        }
    }
    
    /**
     * 通道激活时初始化
     */
    @Override
    public void channelActive(ChannelHandlerContext ctx) throws Exception {
        log.debug("MQTT编解码器通道激活: {}", ctx.channel().remoteAddress());
        super.channelActive(ctx);
    }
    
    /**
     * 通道关闭时清理资源
     */
    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
        log.debug("MQTT编解码器通道关闭: {}", ctx.channel().remoteAddress());
        
        // 清理协议版本信息
        MqttProtocolVersionManager.resetProtocolVersion(ctx.channel());
        
        super.channelInactive(ctx);
    }
    
    /**
     * 获取通道的协议版本信息
     *
     * @param ctx 通道上下文
     * @return 协议版本信息字符串
     */
    public String getProtocolInfo(ChannelHandlerContext ctx) {
        MqttVersion version = MqttProtocolVersionManager.getProtocolVersion(ctx.channel());
        boolean negotiated = MqttProtocolVersionManager.isVersionNegotiated(ctx.channel());
        
        return String.format("协议版本: %s, 已协商: %s", 
            version != null ? version : "未知", negotiated);
    }
}