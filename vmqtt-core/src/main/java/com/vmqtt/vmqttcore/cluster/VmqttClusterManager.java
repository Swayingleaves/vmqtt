package com.vmqtt.vmqttcore.cluster;

import com.vmqtt.common.grpc.cluster.ServiceInfo;
import com.vmqtt.vmqttcore.cluster.grpc.ClusterGrpcClient;
import lombok.Data;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Component;

import javax.annotation.PreDestroy;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

/**
 * V-MQTT集群管理器
 * 协调和管理所有集群组件，提供统一的集群管理接口
 *
 * @author zhenglin
 * @date 2025/08/10
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class VmqttClusterManager {

    // 核心组件依赖
    private final ClusterNodeManager nodeManager;
    private final ServiceRegistry serviceRegistry;
    private final ClusterSubscriptionManager subscriptionManager;
    private final ClusterMessageRoutingEngine routingEngine;
    private final ClusterMessageDistributor messageDistributor;
    private final GossipSubscriptionProtocol gossipProtocol;
    private final SubscriptionConflictResolver conflictResolver;
    private final ClusterRoutingOptimizer routingOptimizer;
    private final ClusterAwareLoadBalancer loadBalancer;
    private final ClusterAwareSessionManager sessionManager;
    private final ClusterGrpcClient grpcClient;
    
    private volatile boolean clusterEnabled = true;
    private volatile ClusterState currentState = ClusterState.INITIALIZING;
    
    /**
     * 应用启动后初始化集群
     */
    @EventListener(ApplicationReadyEvent.class)
    public void initializeCluster() {
        if (!clusterEnabled) {
            log.info("Cluster functionality is disabled");
            currentState = ClusterState.DISABLED;
            return;
        }
        
        try {
            log.info("Initializing V-MQTT cluster manager");
            currentState = ClusterState.STARTING;
            
            // 1. 启动基础组件
            startBasicComponents();
            
            // 2. 启动网络组件
            startNetworkComponents();
            
            // 3. 启动业务组件
            startBusinessComponents();
            
            // 4. 等待集群稳定
            waitForClusterStability();
            
            currentState = ClusterState.RUNNING;
            log.info("V-MQTT cluster manager initialized successfully");
            
        } catch (Exception e) {
            log.error("Failed to initialize cluster manager", e);
            currentState = ClusterState.FAILED;
            
            // 尝试清理已启动的组件
            shutdownCluster();
            throw new RuntimeException("Cluster initialization failed", e);
        }
    }
    
    /**
     * 关闭集群管理器
     */
    @PreDestroy
    public void shutdownCluster() {
        if (currentState == ClusterState.DISABLED || currentState == ClusterState.STOPPED) {
            return;
        }
        
        try {
            log.info("Shutting down V-MQTT cluster manager");
            currentState = ClusterState.STOPPING;
            
            // 优雅关闭各个组件（与启动顺序相反）
            shutdownBusinessComponents();
            shutdownNetworkComponents();
            shutdownBasicComponents();
            
            currentState = ClusterState.STOPPED;
            log.info("V-MQTT cluster manager shut down successfully");
            
        } catch (Exception e) {
            log.error("Error during cluster shutdown", e);
            currentState = ClusterState.FAILED;
        }
    }
    
    /**
     * 启动基础组件
     */
    private void startBasicComponents() {
        log.debug("Starting basic components");
        
        // 启动路由优化器
        routingOptimizer.start();
        
        // 初始化集群感知会话管理器
        sessionManager.init();
        
        log.debug("Basic components started");
    }
    
    /**
     * 启动网络组件
     */
    private void startNetworkComponents() {
        log.debug("Starting network components");
        
        // 启动Gossip协议
        gossipProtocol.start();
        
        // 启动订阅管理器
        subscriptionManager.start();
        
        log.debug("Network components started");
    }
    
    /**
     * 启动业务组件
     */
    private void startBusinessComponents() {
        log.debug("Starting business components");
        
        // 业务组件通常不需要显式启动，因为它们是按需工作的
        // 这里可以进行一些初始化验证
        
        log.debug("Business components started");
    }
    
    /**
     * 等待集群稳定
     */
    private void waitForClusterStability() {
        log.debug("Waiting for cluster stability");
        
        try {
            // 等待一段时间让集群发现和订阅同步完成
            Thread.sleep(5000); // 5秒
            
            // 检查集群状态
            checkClusterHealth();
            
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("Interrupted while waiting for cluster stability", e);
        }
    }
    
    /**
     * 检查集群健康状态
     */
    private void checkClusterHealth() {
        try {
            // 检查服务注册
            List<ServiceInfo> nodes = serviceRegistry.discover("vmqtt-core");
            log.info("Discovered {} cluster nodes", nodes.size());
            
            // 检查订阅管理器状态
            var subscriptionStats = subscriptionManager.getSubscriptionsForNode(nodeManager.getCurrentNodeId());
            log.info("Local subscription count: {}", subscriptionStats.size());
            
        } catch (Exception e) {
            log.warn("Cluster health check encountered issues", e);
        }
    }
    
    /**
     * 关闭业务组件
     */
    private void shutdownBusinessComponents() {
        log.debug("Shutting down business components");
        // 业务组件关闭
        log.debug("Business components shut down");
    }
    
    /**
     * 关闭网络组件
     */
    private void shutdownNetworkComponents() {
        log.debug("Shutting down network components");
        
        try {
            // 关闭订阅管理器
            subscriptionManager.stop();
        } catch (Exception e) {
            log.warn("Error shutting down subscription manager", e);
        }
        
        try {
            // 关闭Gossip协议
            gossipProtocol.stop();
        } catch (Exception e) {
            log.warn("Error shutting down gossip protocol", e);
        }
        
        try {
            // 关闭gRPC客户端连接
            grpcClient.closeAllConnections();
        } catch (Exception e) {
            log.warn("Error shutting down gRPC client", e);
        }
        
        log.debug("Network components shut down");
    }
    
    /**
     * 关闭基础组件
     */
    private void shutdownBasicComponents() {
        log.debug("Shutting down basic components");
        
        try {
            // 关闭集群感知会话管理器
            sessionManager.shutdown();
        } catch (Exception e) {
            log.warn("Error shutting down session manager", e);
        }
        
        try {
            // 关闭路由优化器
            routingOptimizer.stop();
        } catch (Exception e) {
            log.warn("Error shutting down routing optimizer", e);
        }
        
        try {
            // 关闭消息分发器
            messageDistributor.shutdown();
        } catch (Exception e) {
            log.warn("Error shutting down message distributor", e);
        }
        
        log.debug("Basic components shut down");
    }
    
    /**
     * 获取集群状态信息
     */
    public ClusterStatusInfo getClusterStatus() {
        try {
            // 获取各组件状态
            var routingStats = routingEngine.getStats();
            var distributionStats = messageDistributor.getDistributionStats();
            var optimizerStats = routingOptimizer.getStats();
            var loadBalanceStats = loadBalancer.getStats();
            var conflictStats = conflictResolver.getConflictStatistics();
            
            // 获取节点信息
            List<ServiceInfo> clusterNodes = serviceRegistry.discover("vmqtt-core");
            String currentNodeId = nodeManager.getCurrentNodeId();
            
            return new ClusterStatusInfo(
                currentState,
                currentNodeId,
                clusterNodes.size(),
                routingStats,
                distributionStats,
                optimizerStats,
                loadBalanceStats,
                conflictStats,
                System.currentTimeMillis()
            );
            
        } catch (Exception e) {
            log.error("Failed to get cluster status", e);
            return new ClusterStatusInfo(
                ClusterState.FAILED,
                "unknown",
                0,
                null, null, null, null, null,
                System.currentTimeMillis()
            );
        }
    }
    
    /**
     * 手动触发集群同步
     */
    public CompletableFuture<Boolean> triggerClusterSync() {
        if (currentState != ClusterState.RUNNING) {
            log.warn("Cannot trigger sync, cluster is not running. Current state: {}", currentState);
            return CompletableFuture.completedFuture(false);
        }
        
        return CompletableFuture.supplyAsync(() -> {
            try {
                log.info("Triggering manual cluster sync");
                
                // 触发订阅信息同步
                boolean syncResult = sessionManager.syncClusterSubscriptions().get();
                
                log.info("Manual cluster sync completed: {}", syncResult ? "SUCCESS" : "FAILED");
                return syncResult;
                
            } catch (Exception e) {
                log.error("Failed to trigger cluster sync", e);
                return false;
            }
        });
    }
    
    /**
     * 获取集群节点信息
     */
    public List<ServiceInfo> getClusterNodes() {
        try {
            return serviceRegistry.discover("vmqtt-core");
        } catch (Exception e) {
            log.error("Failed to get cluster nodes", e);
            return List.of();
        }
    }
    
    /**
     * 获取节点负载均衡状态
     */
    public Map<String, ClusterAwareLoadBalancer.NodeStatus> getNodeLoadBalanceStatus() {
        return loadBalancer.getNodeStatus();
    }
    
    /**
     * 设置负载均衡策略
     */
    public void setLoadBalanceStrategy(ClusterAwareLoadBalancer.LoadBalanceStrategy strategy) {
        loadBalancer.setStrategy(strategy);
        log.info("Load balance strategy changed to: {}", strategy);
    }
    
    /**
     * 启用/禁用集群功能
     */
    public void setClusterEnabled(boolean enabled) {
        if (this.clusterEnabled != enabled) {
            log.info("Cluster enabled status changed: {} -> {}", this.clusterEnabled, enabled);
            this.clusterEnabled = enabled;
            
            if (!enabled && currentState == ClusterState.RUNNING) {
                shutdownCluster();
            } else if (enabled && currentState == ClusterState.DISABLED) {
                initializeCluster();
            }
        }
    }
    
    /**
     * 是否启用集群功能
     */
    public boolean isClusterEnabled() {
        return clusterEnabled;
    }
    
    /**
     * 获取当前集群状态
     */
    public ClusterState getCurrentState() {
        return currentState;
    }
    
    // ========== 数据类定义 ==========
    
    /**
     * 集群状态枚举
     */
    public enum ClusterState {
        DISABLED,      // 集群功能禁用
        INITIALIZING,  // 初始化中
        STARTING,      // 启动中
        RUNNING,       // 运行中
        STOPPING,      // 停止中
        STOPPED,       // 已停止
        FAILED         // 失败状态
    }
    
    /**
     * 集群状态信息
     */
    @Data
    public static class ClusterStatusInfo {
        private final ClusterState state;
        private final String currentNodeId;
        private final int totalNodes;
        private final ClusterMessageRoutingEngine.ClusterRoutingStats routingStats;
        private final ClusterMessageDistributor.DistributionStats distributionStats;
        private final ClusterRoutingOptimizer.OptimizerStats optimizerStats;
        private final ClusterAwareLoadBalancer.LoadBalanceStats loadBalanceStats;
        private final SubscriptionConflictResolver.ConflictStatistics conflictStats;
        private final long timestamp;
    }
}