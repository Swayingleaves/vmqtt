/**
 * 零拷贝MQTT消息处理器
 *
 * @author zhenglin
 * @date 2025/08/10
 */
package com.vmqtt.frontend.handler;

import com.vmqtt.frontend.config.NettyOptimizationConfig;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.CompositeByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.util.ReferenceCountUtil;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 零拷贝MQTT消息处理器
 * 使用Netty的零拷贝技术优化消息处理性能
 */
@Slf4j
@Component
public class ZeroCopyMqttMessageHandler extends SimpleChannelInboundHandler<ByteBuf> {
    
    private final NettyOptimizationConfig.ZeroCopyMessageEncoderConfig encoderConfig;
    private final AtomicLong processedMessages = new AtomicLong(0);
    private final AtomicLong totalBytesProcessed = new AtomicLong(0);
    
    // 批处理相关
    private final List<ByteBuf> batchBuffer = new ArrayList<>();
    private volatile long lastBatchTime = System.nanoTime();
    
    public ZeroCopyMqttMessageHandler(NettyOptimizationConfig.ZeroCopyMessageEncoderConfig encoderConfig) {
        this.encoderConfig = encoderConfig;
    }
    
    @Override
    protected void channelRead0(ChannelHandlerContext ctx, ByteBuf msg) throws Exception {
        try {
            if (!msg.isReadable()) {
                return;
            }
            
            // 增加引用计数以防止被释放
            msg.retain();
            
            // 更新统计信息
            processedMessages.incrementAndGet();
            totalBytesProcessed.addAndGet(msg.readableBytes());
            
            if (encoderConfig.isEnableWriteAndFlushBatching()) {
                processBatchedMessage(ctx, msg);
            } else {
                processImmediateMessage(ctx, msg);
            }
            
        } catch (Exception e) {
            log.error("处理MQTT消息时发生错误", e);
            ReferenceCountUtil.safeRelease(msg);
            throw e;
        }
    }
    
    /**
     * 批处理消息（减少系统调用，提高吞吐量）
     */
    private void processBatchedMessage(ChannelHandlerContext ctx, ByteBuf msg) {
        synchronized (batchBuffer) {
            batchBuffer.add(msg);
            
            long currentTime = System.nanoTime();
            long timeSinceLastBatch = currentTime - lastBatchTime;
            
            // 如果缓冲区满了或者超时了，就批量处理
            if (batchBuffer.size() >= 32 || 
                timeSinceLastBatch > encoderConfig.getBatchingDelayMicros() * 1000) {
                
                processBatch(ctx);
                lastBatchTime = currentTime;
            }
        }
    }
    
    /**
     * 处理批次消息
     */
    private void processBatch(ChannelHandlerContext ctx) {
        if (batchBuffer.isEmpty()) {
            return;
        }
        
        try {
            if (encoderConfig.isEnableCompositeBuffer() && batchBuffer.size() > 1) {
                // 使用CompositeByteBuf实现零拷贝合并
                CompositeByteBuf composite = createOptimizedCompositeBuf(ctx.alloc(), batchBuffer);
                
                // 处理合并后的消息
                processCompositeMessage(ctx, composite);
                
            } else {
                // 单独处理每个消息
                for (ByteBuf buf : batchBuffer) {
                    processImmediateMessage(ctx, buf);
                }
            }
        } finally {
            // 清理缓冲区
            batchBuffer.clear();
        }
    }
    
    /**
     * 创建优化的CompositeByteBuf
     */
    private CompositeByteBuf createOptimizedCompositeBuf(ByteBufAllocator alloc, List<ByteBuf> buffers) {
        CompositeByteBuf composite;
        
        if (encoderConfig.isEnableDirectBuffer()) {
            composite = alloc.compositeDirectBuffer(
                Math.min(buffers.size(), encoderConfig.getMaxComponentsPerComposite())
            );
        } else {
            composite = alloc.compositeHeapBuffer(
                Math.min(buffers.size(), encoderConfig.getMaxComponentsPerComposite())
            );
        }
        
        // 添加所有组件，实现零拷贝
        for (ByteBuf buf : buffers) {
            if (buf.isReadable()) {
                composite.addComponent(true, buf);
            } else {
                ReferenceCountUtil.safeRelease(buf);
            }
        }
        
        return composite;
    }
    
    /**
     * 处理合并后的消息
     */
    private void processCompositeMessage(ChannelHandlerContext ctx, CompositeByteBuf composite) {
        try {
            if (!composite.isReadable()) {
                return;
            }
            
            // 这里实现具体的MQTT协议处理逻辑
            // 由于使用了CompositeByteBuf，避免了内存拷贝
            
            // 零拷贝读取消息头
            ByteBuf header = readMessageHeaderZeroCopy(composite);
            if (header != null) {
                try {
                    processMessageHeader(ctx, header);
                } finally {
                    ReferenceCountUtil.safeRelease(header);
                }
            }
            
            // 零拷贝读取消息体
            ByteBuf payload = readMessagePayloadZeroCopy(composite);
            if (payload != null) {
                try {
                    processMessagePayload(ctx, payload);
                } finally {
                    ReferenceCountUtil.safeRelease(payload);
                }
            }
            
        } finally {
            ReferenceCountUtil.safeRelease(composite);
        }
    }
    
    /**
     * 立即处理消息（低延迟模式）
     */
    private void processImmediateMessage(ChannelHandlerContext ctx, ByteBuf msg) {
        try {
            // 零拷贝slice操作
            ByteBuf header = readMessageHeaderZeroCopy(msg);
            ByteBuf payload = readMessagePayloadZeroCopy(msg);
            
            if (header != null) {
                try {
                    processMessageHeader(ctx, header);
                } finally {
                    // header是slice，不需要释放，原msg会被释放
                }
            }
            
            if (payload != null) {
                try {
                    processMessagePayload(ctx, payload);
                } finally {
                    // payload是slice，不需要释放，原msg会被释放
                }
            }
            
        } finally {
            ReferenceCountUtil.safeRelease(msg);
        }
    }
    
    /**
     * 零拷贝读取消息头
     */
    private ByteBuf readMessageHeaderZeroCopy(ByteBuf source) {
        if (!source.isReadable() || source.readableBytes() < 2) {
            return null;
        }
        
        // 使用slice实现零拷贝
        int readerIndex = source.readerIndex();
        
        // MQTT固定头部至少2字节
        byte firstByte = source.getByte(readerIndex);
        byte secondByte = source.getByte(readerIndex + 1);
        
        // 计算剩余长度
        int remainingLength = calculateRemainingLength(source, readerIndex + 1);
        if (remainingLength < 0) {
            return null; // 消息不完整
        }
        
        int headerLength = 1 + getRemainingLengthByteCount(remainingLength);
        
        if (source.readableBytes() < headerLength) {
            return null; // 消息不完整
        }
        
        // 零拷贝slice
        return source.slice(readerIndex, headerLength);
    }
    
    /**
     * 零拷贝读取消息载荷
     */
    private ByteBuf readMessagePayloadZeroCopy(ByteBuf source) {
        if (!source.isReadable()) {
            return null;
        }
        
        int readerIndex = source.readerIndex();
        
        // 计算头部长度
        int remainingLength = calculateRemainingLength(source, readerIndex + 1);
        if (remainingLength < 0) {
            return null;
        }
        
        int headerLength = 1 + getRemainingLengthByteCount(remainingLength);
        int variableHeaderLength = calculateVariableHeaderLength(source, readerIndex, headerLength);
        
        int payloadStart = readerIndex + headerLength + variableHeaderLength;
        int payloadLength = remainingLength - variableHeaderLength;
        
        if (payloadLength <= 0 || source.readableBytes() < payloadStart + payloadLength) {
            return null;
        }
        
        // 零拷贝slice
        return source.slice(payloadStart, payloadLength);
    }
    
    /**
     * 处理消息头
     */
    private void processMessageHeader(ChannelHandlerContext ctx, ByteBuf header) {
        // 这里实现MQTT消息头的处理逻辑
        // 由于使用零拷贝，避免了内存分配和拷贝
        
        byte messageType = (byte) ((header.getByte(0) & 0xF0) >> 4);
        boolean dup = (header.getByte(0) & 0x08) != 0;
        int qos = (header.getByte(0) & 0x06) >> 1;
        boolean retain = (header.getByte(0) & 0x01) != 0;
        
        log.debug("处理MQTT消息头: type={}, dup={}, qos={}, retain={}", 
            messageType, dup, qos, retain);
    }
    
    /**
     * 处理消息载荷
     */
    private void processMessagePayload(ChannelHandlerContext ctx, ByteBuf payload) {
        // 这里实现MQTT消息载荷的处理逻辑
        // 由于使用零拷贝，避免了内存分配和拷贝
        
        log.debug("处理MQTT消息载荷: {} 字节", payload.readableBytes());
    }
    
    /**
     * 计算剩余长度
     */
    private int calculateRemainingLength(ByteBuf buffer, int startIndex) {
        int remainingLength = 0;
        int multiplier = 1;
        int byteIndex = startIndex;
        
        do {
            if (byteIndex >= buffer.writerIndex()) {
                return -1; // 不完整的消息
            }
            
            byte b = buffer.getByte(byteIndex++);
            remainingLength += (b & 0x7F) * multiplier;
            multiplier *= 128;
            
            if ((b & 0x80) == 0) {
                break;
            }
            
            if (multiplier > 128 * 128 * 128) {
                return -1; // 无效的剩余长度
            }
        } while (true);
        
        return remainingLength;
    }
    
    /**
     * 获取剩余长度字段的字节数
     */
    private int getRemainingLengthByteCount(int remainingLength) {
        if (remainingLength < 128) return 1;
        if (remainingLength < 16384) return 2;
        if (remainingLength < 2097152) return 3;
        return 4;
    }
    
    /**
     * 计算可变头部长度
     */
    private int calculateVariableHeaderLength(ByteBuf source, int readerIndex, int headerLength) {
        // 这里需要根据消息类型计算可变头部长度
        // 简化实现，返回默认值
        return 2; // 大多数MQTT消息的可变头部至少有2字节的包标识符
    }
    
    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
        // 确保批处理缓冲区被清理
        synchronized (batchBuffer) {
            if (!batchBuffer.isEmpty()) {
                processBatch(ctx);
            }
        }
        
        log.info("通道关闭，处理消息总数: {}, 处理字节总数: {}", 
            processedMessages.get(), totalBytesProcessed.get());
        
        super.channelInactive(ctx);
    }
    
    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
        log.error("零拷贝消息处理器发生异常", cause);
        
        // 清理批处理缓冲区
        synchronized (batchBuffer) {
            for (ByteBuf buf : batchBuffer) {
                ReferenceCountUtil.safeRelease(buf);
            }
            batchBuffer.clear();
        }
        
        ctx.close();
    }
    
    /**
     * 获取处理统计信息
     */
    public ProcessingStats getProcessingStats() {
        return ProcessingStats.builder()
            .processedMessages(processedMessages.get())
            .totalBytesProcessed(totalBytesProcessed.get())
            .averageBytesPerMessage(processedMessages.get() > 0 ? 
                (double) totalBytesProcessed.get() / processedMessages.get() : 0.0)
            .build();
    }
    
    /**
     * 处理统计信息
     */
    public static class ProcessingStats {
        private final long processedMessages;
        private final long totalBytesProcessed;
        private final double averageBytesPerMessage;
        
        private ProcessingStats(long processedMessages, long totalBytesProcessed, 
                              double averageBytesPerMessage) {
            this.processedMessages = processedMessages;
            this.totalBytesProcessed = totalBytesProcessed;
            this.averageBytesPerMessage = averageBytesPerMessage;
        }
        
        public static ProcessingStatsBuilder builder() {
            return new ProcessingStatsBuilder();
        }
        
        public static class ProcessingStatsBuilder {
            private long processedMessages;
            private long totalBytesProcessed;
            private double averageBytesPerMessage;
            
            public ProcessingStatsBuilder processedMessages(long processedMessages) {
                this.processedMessages = processedMessages;
                return this;
            }
            
            public ProcessingStatsBuilder totalBytesProcessed(long totalBytesProcessed) {
                this.totalBytesProcessed = totalBytesProcessed;
                return this;
            }
            
            public ProcessingStatsBuilder averageBytesPerMessage(double averageBytesPerMessage) {
                this.averageBytesPerMessage = averageBytesPerMessage;
                return this;
            }
            
            public ProcessingStats build() {
                return new ProcessingStats(processedMessages, totalBytesProcessed, averageBytesPerMessage);
            }
        }
        
        // Getters
        public long getProcessedMessages() { return processedMessages; }
        public long getTotalBytesProcessed() { return totalBytesProcessed; }
        public double getAverageBytesPerMessage() { return averageBytesPerMessage; }
    }
}