package com.vmqtt.fe.cluster;

import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

public class BeNode {
    
    public enum NodeState {
        ALIVE, SUSPECT, FAILED
    }
    
    private final String nodeId;
    private final String host;
    private final int port;
    private final int grpcPort;
    private final Map<String, String> metadata;

    private final AtomicLong lastHeartbeat = new AtomicLong();
    private final AtomicInteger failureCount = new AtomicInteger(0);
    private volatile NodeState state = NodeState.ALIVE;
    
    private final AtomicInteger activeConnections = new AtomicInteger(0);
    private final AtomicLong messagesProcessed = new AtomicLong(0);

    public BeNode(String nodeId, String host, int port, int grpcPort, Map<String, String> metadata) {
        this.nodeId = nodeId;
        this.host = host;
        this.port = port;
        this.grpcPort = grpcPort;
        this.metadata = metadata;
        this.lastHeartbeat.set(System.currentTimeMillis());
    }

    public String getNodeId() { return nodeId; }
    public String getHost() { return host; }
    public int getPort() { return port; }
    public int getGrpcPort() { return grpcPort; }
    public long getLastHeartbeat() { return lastHeartbeat.get(); }
    public NodeState getState() { return state; }
    public int getActiveConnections() { return activeConnections.get(); }
    public long getMessagesProcessed() { return messagesProcessed.get(); }
    
    public String getGrpcAddress() {
        return host + ":" + grpcPort;
    }

    public void updateHeartbeat() {
        this.lastHeartbeat.set(System.currentTimeMillis());
        this.failureCount.set(0);
        this.state = NodeState.ALIVE;
    }
    
    public void updateHeartbeat(long timestamp) {
        this.lastHeartbeat.set(timestamp);
        this.failureCount.set(0);
        this.state = NodeState.ALIVE;
    }
    
    public void setMetrics(int connections, long msgProcessed) {
        this.activeConnections.set(connections);
        this.messagesProcessed.set(msgProcessed);
    }
    
    public void incrementMessagesProcessed() {
        messagesProcessed.incrementAndGet();
    }
    
    public void resetFailureCount() {
        failureCount.set(0);
    }

    public int incrementFailureCount() {
        return failureCount.incrementAndGet();
    }

    public boolean isAlive() {
        return state == NodeState.ALIVE;
    }
    
    public boolean isAvailable() {
        return state != NodeState.FAILED;
    }

    public void markSuspect() {
        this.state = NodeState.SUSPECT;
    }
    
    public void markFailed() {
        this.state = NodeState.FAILED;
    }

    @Override
    public String toString() {
        return "BeNode{" +
                "nodeId='" + nodeId + '\'' +
                ", host='" + host + '\'' +
                ", port=" + port +
                ", state=" + state +
                '}';
    }
} 