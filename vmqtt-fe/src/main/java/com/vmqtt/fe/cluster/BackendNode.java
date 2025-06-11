package com.vmqtt.fe.cluster;

import java.time.Instant;
import java.util.Objects;
import java.util.Map;
import java.util.HashMap;

/**
 * Backend节点信息
 * 
 * @author zhenglin
 * @mail zhenglin.cn.cq@gmail.com
 */
public class BackendNode {
    
    private final String nodeId;
    private final String host;
    private final int grpcPort;
    private int port; // HTTP端口
    private NodeStatus status;
    private long lastHeartbeat;
    private long messagesProcessed;
    private long activeConnections;
    private int failureCount;
    private Map<String, String> metadata;
    
    public BackendNode(String nodeId, String host, int grpcPort) {
        this.nodeId = nodeId;
        this.host = host;
        this.grpcPort = grpcPort;
        this.port = 8080; // 默认HTTP端口
        this.status = NodeStatus.ACTIVE;
        this.lastHeartbeat = Instant.now().toEpochMilli();
        this.messagesProcessed = 0;
        this.activeConnections = 0;
        this.failureCount = 0;
        this.metadata = new HashMap<>();
    }
    
    public BackendNode(String nodeId, String host, int grpcPort, int port, Map<String, String> metadata) {
        this.nodeId = nodeId;
        this.host = host;
        this.grpcPort = grpcPort;
        this.port = port;
        this.status = NodeStatus.ACTIVE;
        this.lastHeartbeat = Instant.now().toEpochMilli();
        this.messagesProcessed = 0;
        this.activeConnections = 0;
        this.failureCount = 0;
        this.metadata = metadata != null ? new HashMap<>(metadata) : new HashMap<>();
    }
    
    /**
     * 节点状态枚举
     */
    public enum NodeStatus {
        ACTIVE,     // 活跃状态
        INACTIVE,   // 非活跃状态
        FAILED      // 故障状态
    }
    
    // Getters
    public String getNodeId() {
        return nodeId;
    }
    
    public String getHost() {
        return host;
    }
    
    public int getGrpcPort() {
        return grpcPort;
    }
    
    public int getPort() {
        return port;
    }
    
    public void setPort(int port) {
        this.port = port;
    }
    
    public NodeStatus getStatus() {
        return status;
    }
    
    public NodeStatus getState() {
        return status;
    }
    
    public void setStatus(NodeStatus status) {
        this.status = status;
    }
    
    public long getLastHeartbeat() {
        return lastHeartbeat;
    }
    
    public void updateHeartbeat() {
        this.lastHeartbeat = Instant.now().toEpochMilli();
        if (this.status == NodeStatus.FAILED) {
            this.status = NodeStatus.ACTIVE;
            this.failureCount = 0;
        }
    }
    
    public long getMessagesProcessed() {
        return messagesProcessed;
    }
    
    public void setMessagesProcessed(long messagesProcessed) {
        this.messagesProcessed = messagesProcessed;
    }
    
    public long getActiveConnections() {
        return activeConnections;
    }
    
    public void setActiveConnections(long activeConnections) {
        this.activeConnections = activeConnections;
    }
    
    public int getFailureCount() {
        return failureCount;
    }
    
    public void incrementFailureCount() {
        this.failureCount++;
    }
    
    public void resetFailureCount() {
        this.failureCount = 0;
    }
    
    public Map<String, String> getMetadata() {
        return metadata;
    }
    
    public void setMetadata(Map<String, String> metadata) {
        this.metadata = metadata != null ? new HashMap<>(metadata) : new HashMap<>();
    }
    
    public boolean isAvailable() {
        return status == NodeStatus.ACTIVE;
    }
    
    public void markAlive() {
        this.status = NodeStatus.ACTIVE;
        this.failureCount = 0;
    }
    
    public void markSuspect() {
        this.status = NodeStatus.INACTIVE;
    }
    
    public void markFailed() {
        this.status = NodeStatus.FAILED;
    }
    
    public void incrementMessagesProcessed() {
        this.messagesProcessed++;
    }
    
    /**
     * 检查节点是否超时
     */
    public boolean isTimeout(long timeoutMs) {
        return (Instant.now().toEpochMilli() - lastHeartbeat) > timeoutMs;
    }
    
    /**
     * 获取节点地址
     */
    public String getAddress() {
        return host + ":" + grpcPort;
    }
    
    /**
     * 获取gRPC地址
     */
    public String getGrpcAddress() {
        return host + ":" + grpcPort;
    }
    
    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        BackendNode that = (BackendNode) o;
        return Objects.equals(nodeId, that.nodeId);
    }
    
    @Override
    public int hashCode() {
        return Objects.hash(nodeId);
    }
    
    @Override
    public String toString() {
        return "BackendNode{" +
                "nodeId='" + nodeId + '\'' +
                ", host='" + host + '\'' +
                ", grpcPort=" + grpcPort +
                ", status=" + status +
                ", lastHeartbeat=" + lastHeartbeat +
                ", messagesProcessed=" + messagesProcessed +
                ", activeConnections=" + activeConnections +
                ", failureCount=" + failureCount +
                '}';
    }
} 