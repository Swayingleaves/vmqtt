package com.vmqtt.fe.cluster.gossip;

import com.vmqtt.fe.cluster.BackendManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import java.io.*;
import java.net.*;
import java.util.*;
import java.util.concurrent.*;

/**
 * Gossip协议实现
 * @author zhenglin
 * @mail zhenglin.cn.cq@gmail.com
 */
@Component
public class GossipProtocol {

    private static final Logger logger = LoggerFactory.getLogger(GossipProtocol.class);

    @Value("${vmqtt.fe.node-id:fe-node-1}")
    private String nodeId;
    
    @Value("${vmqtt.fe.gossip.enabled:true}")
    private boolean gossipEnabled;

    @Value("${vmqtt.fe.gossip.port:7946}")
    private int gossipPort;
    
    @Value("${vmqtt.fe.eventbus.port:7949}")
    private int eventBusPort;

    @Value("${vmqtt.fe.gossip.interval:5000}")
    private int gossipInterval;

    @Value("${vmqtt.fe.gossip.timeout:10000}")
    private int gossipTimeout;

    @Value("${vmqtt.fe.gossip.failure-threshold:3}")
    private int failureThreshold;

    @Value("${vmqtt.fe.gossip.seed-nodes:}")
    private String seedNodes;
    
    @Autowired
    private BackendManager backendManager;

    private final Map<String, GossipNode> nodes = new ConcurrentHashMap<>();
    private GossipNode self;
    private DatagramSocket socket;
    private volatile boolean running = false;
    private ScheduledExecutorService scheduler;
    private ExecutorService messageProcessor;

    @PostConstruct
    public void start() {
        if (!gossipEnabled) {
            logger.info("Gossip协议已禁用");
            return;
        }

        running = true;
        try {
            socket = new DatagramSocket(gossipPort);
            logger.info("Gossip UDP服务器正在监听端口: {}", gossipPort);

            // 初始化自身节点
            String localHost = getLocalHostAddress();
            self = new GossipNode(nodeId, localHost, gossipPort, eventBusPort);
            nodes.put(nodeId, self);
            
            messageProcessor = Executors.newVirtualThreadPerTaskExecutor();
            messageProcessor.submit(this::listen);
            
            scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
                Thread t = new Thread(r, "gossip-scheduler");
                t.setDaemon(true);
                return t;
            });
            
            scheduler.scheduleWithFixedDelay(this::performGossip, 0, gossipInterval, TimeUnit.MILLISECONDS);
            scheduler.scheduleWithFixedDelay(this::checkFailures, gossipInterval, gossipInterval, TimeUnit.MILLISECONDS);
            
            connectToSeedNodes();
            
            logger.info("Gossip协议启动成功: nodeId={}, gossipPort={}, localHost={}", nodeId, gossipPort, localHost);
            
        } catch (SocketException e) {
            logger.error("启动Gossip服务器失败", e);
            running = false;
        }
    }

    @PreDestroy
    public void stop() {
        if (!gossipEnabled) {
            return;
        }
        
        running = false;
        if (scheduler != null) {
            scheduler.shutdown();
        }
        if (messageProcessor != null) {
            messageProcessor.shutdown();
        }
        if (socket != null && !socket.isClosed()) {
            socket.close();
        }
        logger.info("Gossip协议已停止");
    }

    private void listen() {
        while (running) {
            try {
                byte[] buffer = new byte[65535];
                DatagramPacket packet = new DatagramPacket(buffer, buffer.length);
                socket.receive(packet);
                
                // 异步处理消息
                messageProcessor.submit(() -> processMessage(packet));
                
            } catch (IOException e) {
                if (running) {
                    logger.error("接收Gossip消息失败", e);
                }
            }
        }
    }

    private void processMessage(DatagramPacket packet) {
        try (ByteArrayInputStream bais = new ByteArrayInputStream(packet.getData(), 0, packet.getLength());
             ObjectInputStream ois = new ObjectInputStream(bais)) {
             
            GossipMessage message = (GossipMessage) ois.readObject();
            logger.debug("收到Gossip消息: {} from {}", message.getType(), message.getNodeId());

            switch (message.getType()) {
                case JOIN:
                    handleJoin(message, packet.getSocketAddress());
                    break;
                case ACCEPT:
                    handleAccept(message);
                    break;
                case HEARTBEAT:
                    handleHeartbeat(message);
                    break;
                case BACKEND_UPDATE:
                    handleBackendUpdate(message);
                    break;
            }
        } catch (Exception e) {
            logger.error("处理Gossip消息失败", e);
        }
    }

    private void performGossip() {
        try {
            List<GossipNode> gossipTargets = new ArrayList<>(nodes.values());
            gossipTargets.remove(self);
            if (gossipTargets.isEmpty()) {
                return;
            }
            
            // 随机选择一个节点进行Gossip
            GossipNode target = gossipTargets.get(ThreadLocalRandom.current().nextInt(gossipTargets.size()));
            
            Map<String, GossipMessage.BackendInfo> backendInfo = getBackendInfo();
            GossipMessage message = new GossipMessage(
                GossipMessage.MessageType.HEARTBEAT,
                self.getNodeId(),
                self.getHost(),
                self.getPort(),
                self.getEventBusPort(),
                new ArrayList<>(nodes.values()),
                backendInfo
            );
            
            sendMessage(message, new InetSocketAddress(target.getHost(), target.getPort()));
            
        } catch (Exception e) {
            logger.error("Gossip失败", e);
        }
    }

    private void handleHeartbeat(GossipMessage message) {
        GossipNode node = nodes.computeIfAbsent(message.getNodeId(), k ->
            new GossipNode(message.getNodeId(), message.getHost(), message.getPort(), message.getEventBusPort())
        );
        node.updateHeartbeat();
        
        // 合并节点列表
        mergeNodeLists(message.getAliveNodes());
        syncBackendNodes(message.getBackendNodes());
    }

    private void handleJoin(GossipMessage message, SocketAddress sender) {
        logger.info("节点请求加入集群: {}", message.getNodeId());
        
        GossipNode newNode = new GossipNode(
            message.getNodeId(),
            message.getHost(),
            message.getPort(),
            message.getEventBusPort()
        );
        nodes.put(newNode.getNodeId(), newNode);
        logger.info("发现新节点: {} ({}:{})", newNode.getNodeId(), newNode.getHost(), newNode.getPort());

        // 回复ACCEPT消息
        GossipMessage response = new GossipMessage(
            GossipMessage.MessageType.ACCEPT,
            self.getNodeId(),
            self.getHost(),
            self.getPort(),
            self.getEventBusPort(),
            new ArrayList<>(nodes.values()),
            getBackendInfo()
        );
        sendMessage(response, sender);
    }
    
    private void handleAccept(GossipMessage message) {
        logger.info("成功加入集群，收到来自 {} 的ACCEPT", message.getNodeId());
        mergeNodeLists(message.getAliveNodes());
        syncBackendNodes(message.getBackendNodes());
    }

    private void handleBackendUpdate(GossipMessage message) {
        logger.debug("收到BE节点更新消息 from {}", message.getNodeId());
        syncBackendNodes(message.getBackendNodes());
    }

    private void mergeNodeLists(List<GossipNode> remoteNodes) {
        for (GossipNode remoteNode : remoteNodes) {
            if (!remoteNode.getNodeId().equals(self.getNodeId())) {
                nodes.compute(remoteNode.getNodeId(), (k, existingNode) -> {
                    if (existingNode == null || remoteNode.getHeartbeat() > existingNode.getHeartbeat()) {
                        return remoteNode;
                    }
                    return existingNode;
                });
            }
        }
    }

    private void syncBackendNodes(Map<String, GossipMessage.BackendInfo> remoteBackendNodes) {
        if (remoteBackendNodes == null || remoteBackendNodes.isEmpty()) {
            return;
        }
        
        for (Map.Entry<String, GossipMessage.BackendInfo> entry : remoteBackendNodes.entrySet()) {
            GossipMessage.BackendInfo beInfo = entry.getValue();
            boolean updated = backendManager.addOrUpdateBeNode(
                beInfo.getNodeId(), beInfo.getHost(), beInfo.getPort(), beInfo.getTimestamp());
            if (updated) {
                logger.info("通过Gossip同步了BE节点信息: {}", beInfo.getNodeId());
            }
        }
    }

    private void checkFailures() {
        long now = System.currentTimeMillis();
        for (GossipNode node : nodes.values()) {
            if (!node.equals(self) && node.isAlive()) {
                if (now - node.getHeartbeat() > gossipTimeout) {
                    if (node.incrementFailureCount() >= failureThreshold) {
                        node.markAsDead();
                        logger.warn("节点被标记为死亡: {}", node.getNodeId());
                    }
                }
            }
        }
    }

    private void sendMessage(GossipMessage message, SocketAddress address) {
        try (ByteArrayOutputStream bais = new ByteArrayOutputStream();
             ObjectOutputStream oos = new ObjectOutputStream(bais)) {
             
            oos.writeObject(message);
            byte[] data = bais.toByteArray();
            DatagramPacket packet = new DatagramPacket(data, data.length, address);
            socket.send(packet);
            
        } catch (IOException e) {
            logger.error("发送Gossip消息失败到 {}", address, e);
        }
    }

    private void connectToSeedNodes() {
        if (seedNodes == null || seedNodes.trim().isEmpty()) {
            logger.info("没有配置种子节点");
            return;
        }
        
        String[] seeds = seedNodes.split(",");
        for (String seed : seeds) {
            try {
                String[] parts = seed.trim().split(":");
                String host = parts[0];
                int port = Integer.parseInt(parts[1]);
                
                logger.info("开始连接种子节点: {}", seed);
                GossipMessage joinMessage = new GossipMessage(
                    GossipMessage.MessageType.JOIN,
                    self.getNodeId(),
                    self.getHost(),
                    self.getPort(),
                    self.getEventBusPort(),
                    null,
                    null
                );
                
                sendMessage(joinMessage, new InetSocketAddress(host, port));
                
            } catch (Exception e) {
                logger.error("连接种子节点失败: {}", seed, e);
            }
        }
    }

    private String getLocalHostAddress() {
        try {
            return InetAddress.getLocalHost().getHostAddress();
        } catch (UnknownHostException e) {
            logger.error("获取本地主机地址失败", e);
            return "127.0.0.1";
        }
    }

    public List<GossipNode> getAliveNodes() {
        List<GossipNode> aliveNodes = new ArrayList<>();
        for (GossipNode node : nodes.values()) {
            if (node.isAlive()) {
                aliveNodes.add(node);
            }
        }
        return aliveNodes;
    }

    public void broadcastBackendUpdate() {
        if (!gossipEnabled) return;

        Map<String, GossipMessage.BackendInfo> backendInfo = getBackendInfo();
        if (backendInfo.isEmpty()) {
            return;
        }

        GossipMessage message = new GossipMessage(
            GossipMessage.MessageType.BACKEND_UPDATE,
            self.getNodeId(),
            self.getHost(),
            self.getPort(),
            self.getEventBusPort(),
            null,
            backendInfo
        );

        for (GossipNode node : getAliveNodes()) {
            if (!node.equals(self)) {
                sendMessage(message, new InetSocketAddress(node.getHost(), node.getPort()));
            }
        }
        logger.info("广播了BE节点更新到集群");
    }
    
    private Map<String, GossipMessage.BackendInfo> getBackendInfo() {
        Map<String, GossipMessage.BackendInfo> backendInfo = new HashMap<>();
        backendManager.getBeNodes().forEach(beNode -> {
            backendInfo.put(beNode.getNodeId(), new GossipMessage.BackendInfo(
                beNode.getNodeId(), beNode.getHost(), beNode.getPort(), beNode.getLastHeartbeat()
            ));
        });
        return backendInfo;
    }

    public static class GossipMessage implements Serializable {
        private static final long serialVersionUID = 2L;

        private final MessageType type;
        private final String nodeId;
        private final String host;
        private final int port;
        private final int eventBusPort;
        private final List<GossipNode> aliveNodes;
        private final Map<String, BackendInfo> backendNodes;

        public GossipMessage(MessageType type, String nodeId, String host, int port, int eventBusPort,
                             List<GossipNode> aliveNodes, Map<String, BackendInfo> backendNodes) {
            this.type = type;
            this.nodeId = nodeId;
            this.host = host;
            this.port = port;
            this.eventBusPort = eventBusPort;
            this.aliveNodes = (aliveNodes != null) ? new ArrayList<>(aliveNodes) : null;
            this.backendNodes = (backendNodes != null) ? new HashMap<>(backendNodes) : null;
        }
        
        public MessageType getType() { return type; }
        public String getNodeId() { return nodeId; }
        public String getHost() { return host; }
        public int getPort() { return port; }
        public int getEventBusPort() { return eventBusPort; }
        public List<GossipNode> getAliveNodes() { return aliveNodes; }
        public Map<String, BackendInfo> getBackendNodes() { return backendNodes; }

        public enum MessageType {
            JOIN, ACCEPT, HEARTBEAT, BACKEND_UPDATE
        }

        public static class BackendInfo implements Serializable {
            private static final long serialVersionUID = 1L;
            private final String nodeId;
            private final String host;
            private final int port;
            private final long timestamp;

            public BackendInfo(String nodeId, String host, int port, long timestamp) {
                this.nodeId = nodeId;
                this.host = host;
                this.port = port;
                this.timestamp = timestamp;
            }
            
            public String getNodeId() { return nodeId; }
            public String getHost() { return host; }
            public int getPort() { return port; }
            public long getTimestamp() { return timestamp; }
        }
    }
} 