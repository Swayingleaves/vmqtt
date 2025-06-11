package com.vmqtt.fe.cluster.gossip;

import java.io.Serializable;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Gossip节点信息
 *
 * @author zhenglin
 * @mail zhenglin.cn.cq@gmail.com
 */
public class GossipNode implements Serializable {
    private static final long serialVersionUID = 1L;

    private final String nodeId;
    private final String host;
    private final int port;
    private final int eventBusPort;
    private long heartbeat;
    private final AtomicInteger failureCount = new AtomicInteger(0);
    private volatile boolean alive = true;

    public GossipNode(String nodeId, String host, int port, int eventBusPort) {
        this.nodeId = nodeId;
        this.host = host;
        this.port = port;
        this.eventBusPort = eventBusPort;
        this.heartbeat = System.currentTimeMillis();
    }

    public String getNodeId() {
        return nodeId;
    }

    public String getHost() {
        return host;
    }

    public int getPort() {
        return port;
    }

    public long getHeartbeat() {
        return heartbeat;
    }

    public void updateHeartbeat() {
        this.heartbeat = System.currentTimeMillis();
        this.failureCount.set(0);
        this.alive = true;
    }

    public int getEventBusPort() {
        return eventBusPort;
    }

    public int incrementFailureCount() {
        return failureCount.incrementAndGet();
    }

    public boolean isAlive() {
        return alive;
    }

    public void markAsDead() {
        this.alive = false;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        GossipNode that = (GossipNode) o;
        return Objects.equals(nodeId, that.nodeId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(nodeId);
    }

    @Override
    public String toString() {
        return "GossipNode{" +
                "nodeId='" + nodeId + '\'' +
                ", host='" + host + '\'' +
                ", port=" + port +
                ", eventBusPort=" + eventBusPort +
                ", alive=" + alive +
                '}';
    }
} 