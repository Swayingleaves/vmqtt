package com.vmqtt.vmqttcore.cluster;

import com.google.protobuf.Timestamp;
import com.vmqtt.common.grpc.cluster.*;
import lombok.Data;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.time.Instant;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

/**
 * 订阅信息冲突解决器
 * 处理集群中订阅信息的版本冲突、重复订阅等问题
 *
 * @author zhenglin
 * @date 2025/08/10
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class SubscriptionConflictResolver {

    private final ClusterNodeManager clusterNodeManager;
    
    // 冲突解决策略配置
    private static final ConflictResolutionStrategy DEFAULT_STRATEGY = ConflictResolutionStrategy.LAST_WRITER_WINS;
    private static final long CONFLICT_DETECTION_THRESHOLD_MS = 5000; // 5秒内的并发更新视为冲突
    
    // 冲突记录缓存
    private final ConcurrentHashMap<String, ConflictRecord> conflictHistory = new ConcurrentHashMap<>();
    
    /**
     * 解决订阅冲突
     *
     * @param request 冲突解决请求
     * @return 冲突解决响应
     */
    public ResolveSubscriptionConflictResponse resolveConflicts(ResolveSubscriptionConflictRequest request) {
        try {
            List<ClusterSubscriptionInfo> resolvedSubscriptions = new ArrayList<>();
            
            for (ConflictingSubscription conflict : request.getConflictsList()) {
                ClusterSubscriptionInfo resolved = resolveConflict(conflict);
                if (resolved != null) {
                    resolvedSubscriptions.add(resolved);
                    
                    // 记录冲突解决历史
                    recordConflictResolution(conflict, resolved);
                }
            }
            
            return ResolveSubscriptionConflictResponse.newBuilder()
                .setSuccess(true)
                .addAllResolvedSubscriptions(resolvedSubscriptions)
                .build();
                
        } catch (Exception e) {
            log.error("Failed to resolve subscription conflicts from node {}", request.getNodeId(), e);
            return ResolveSubscriptionConflictResponse.newBuilder()
                .setSuccess(false)
                .setErrorMessage("Conflict resolution failed: " + e.getMessage())
                .build();
        }
    }
    
    /**
     * 检测订阅信息冲突
     *
     * @param newSubscription 新的订阅信息
     * @param existingSubscription 现有的订阅信息
     * @return 冲突类型，如果没有冲突返回null
     */
    public ConflictType detectConflict(ClusterSubscriptionInfo newSubscription, 
                                      ClusterSubscriptionInfo existingSubscription) {
        
        if (newSubscription == null || existingSubscription == null) {
            return null;
        }
        
        // 检查订阅ID是否相同
        if (!newSubscription.getSubscriptionId().equals(existingSubscription.getSubscriptionId())) {
            return null;
        }
        
        // 版本冲突检测
        if (isVersionConflict(newSubscription, existingSubscription)) {
            return ConflictType.VERSION_CONFLICT;
        }
        
        // 节点冲突检测（同一订阅出现在不同节点）
        if (isNodeConflict(newSubscription, existingSubscription)) {
            return ConflictType.NODE_CONFLICT;
        }
        
        // 重复订阅检测
        if (isDuplicateSubscription(newSubscription, existingSubscription)) {
            return ConflictType.DUPLICATE_SUBSCRIPTION;
        }
        
        return null;
    }
    
    /**
     * 批量检测冲突
     *
     * @param subscriptions 待检测的订阅信息列表
     * @return 检测到的冲突列表
     */
    public List<ConflictingSubscription> detectConflicts(List<ClusterSubscriptionInfo> subscriptions) {
        Map<String, List<ClusterSubscriptionInfo>> groupedBySubscriptionId = subscriptions.stream()
            .collect(Collectors.groupingBy(ClusterSubscriptionInfo::getSubscriptionId));
            
        List<ConflictingSubscription> conflicts = new ArrayList<>();
        
        for (Map.Entry<String, List<ClusterSubscriptionInfo>> entry : groupedBySubscriptionId.entrySet()) {
            String subscriptionId = entry.getKey();
            List<ClusterSubscriptionInfo> candidates = entry.getValue();
            
            if (candidates.size() > 1) {
                // 检查是否存在实际冲突
                ConflictType conflictType = analyzeConflictType(candidates);
                if (conflictType != null) {
                    ConflictingSubscription conflict = ConflictingSubscription.newBuilder()
                        .setSubscriptionId(subscriptionId)
                        .addAllConflictingVersions(candidates)
                        .setConflictType(conflictType)
                        .build();
                    conflicts.add(conflict);
                }
            }
        }
        
        return conflicts;
    }
    
    /**
     * 解决单个冲突
     */
    private ClusterSubscriptionInfo resolveConflict(ConflictingSubscription conflict) {
        List<ClusterSubscriptionInfo> candidates = conflict.getConflictingVersionsList();
        ConflictType conflictType = conflict.getConflictType();
        
        log.debug("Resolving conflict for subscription {}: type={}, candidates={}",
            conflict.getSubscriptionId(), conflictType, candidates.size());
            
        switch (conflictType) {
            case VERSION_CONFLICT:
                return resolveVersionConflict(candidates);
            case NODE_CONFLICT:
                return resolveNodeConflict(candidates);
            case DUPLICATE_SUBSCRIPTION:
                return resolveDuplicateConflict(candidates);
            default:
                log.warn("Unknown conflict type: {}", conflictType);
                return resolveByDefaultStrategy(candidates);
        }
    }
    
    /**
     * 解决版本冲突
     */
    private ClusterSubscriptionInfo resolveVersionConflict(List<ClusterSubscriptionInfo> candidates) {
        // 使用最高版本号的订阅
        return candidates.stream()
            .max(Comparator.comparingLong(ClusterSubscriptionInfo::getVersion))
            .orElse(null);
    }
    
    /**
     * 解决节点冲突
     */
    private ClusterSubscriptionInfo resolveNodeConflict(List<ClusterSubscriptionInfo> candidates) {
        // 优先选择当前节点的订阅
        String currentNodeId = clusterNodeManager.getCurrentNodeId();
        
        Optional<ClusterSubscriptionInfo> localSub = candidates.stream()
            .filter(sub -> currentNodeId.equals(sub.getNodeId()))
            .findFirst();
            
        if (localSub.isPresent()) {
            log.debug("Resolved node conflict by selecting local subscription");
            return localSub.get();
        }
        
        // 如果没有本地订阅，选择最新的
        return candidates.stream()
            .max(Comparator.comparing(sub -> sub.getUpdatedAt().getSeconds()))
            .orElse(null);
    }
    
    /**
     * 解决重复订阅冲突
     */
    private ClusterSubscriptionInfo resolveDuplicateConflict(List<ClusterSubscriptionInfo> candidates) {
        // 选择状态为ACTIVE的订阅
        Optional<ClusterSubscriptionInfo> activeSub = candidates.stream()
            .filter(sub -> sub.getStatus() == SubscriptionStatus.SUBSCRIPTION_ACTIVE)
            .findFirst();
            
        if (activeSub.isPresent()) {
            return activeSub.get();
        }
        
        // 如果没有ACTIVE状态的，选择最新的
        return candidates.stream()
            .max(Comparator.comparing(sub -> sub.getCreatedAt().getSeconds()))
            .orElse(null);
    }
    
    /**
     * 使用默认策略解决冲突
     */
    private ClusterSubscriptionInfo resolveByDefaultStrategy(List<ClusterSubscriptionInfo> candidates) {
        switch (DEFAULT_STRATEGY) {
            case LAST_WRITER_WINS:
                return candidates.stream()
                    .max(Comparator.comparing(sub -> sub.getUpdatedAt().getSeconds()))
                    .orElse(null);
            case FIRST_WRITER_WINS:
                return candidates.stream()
                    .min(Comparator.comparing(sub -> sub.getCreatedAt().getSeconds()))
                    .orElse(null);
            case HIGHEST_VERSION:
                return candidates.stream()
                    .max(Comparator.comparingLong(ClusterSubscriptionInfo::getVersion))
                    .orElse(null);
            default:
                return candidates.get(0); // 默认选择第一个
        }
    }
    
    /**
     * 检查是否为版本冲突
     */
    private boolean isVersionConflict(ClusterSubscriptionInfo sub1, ClusterSubscriptionInfo sub2) {
        // 如果时间戳接近但版本号不同，可能是版本冲突
        long timeDiff = Math.abs(sub1.getUpdatedAt().getSeconds() - sub2.getUpdatedAt().getSeconds()) * 1000;
        return timeDiff < CONFLICT_DETECTION_THRESHOLD_MS && sub1.getVersion() != sub2.getVersion();
    }
    
    /**
     * 检查是否为节点冲突
     */
    private boolean isNodeConflict(ClusterSubscriptionInfo sub1, ClusterSubscriptionInfo sub2) {
        return !sub1.getNodeId().equals(sub2.getNodeId()) && 
               sub1.getClientId().equals(sub2.getClientId());
    }
    
    /**
     * 检查是否为重复订阅
     */
    private boolean isDuplicateSubscription(ClusterSubscriptionInfo sub1, ClusterSubscriptionInfo sub2) {
        return sub1.getClientId().equals(sub2.getClientId()) &&
               sub1.getTopicFilter().equals(sub2.getTopicFilter()) &&
               sub1.getNodeId().equals(sub2.getNodeId()) &&
               sub1.getVersion() == sub2.getVersion();
    }
    
    /**
     * 分析冲突类型
     */
    private ConflictType analyzeConflictType(List<ClusterSubscriptionInfo> candidates) {
        if (candidates.size() <= 1) {
            return null;
        }
        
        // 检查是否有不同的节点ID
        Set<String> nodeIds = candidates.stream()
            .map(ClusterSubscriptionInfo::getNodeId)
            .collect(Collectors.toSet());
        if (nodeIds.size() > 1) {
            return ConflictType.NODE_CONFLICT;
        }
        
        // 检查版本冲突
        Set<Long> versions = candidates.stream()
            .map(ClusterSubscriptionInfo::getVersion)
            .collect(Collectors.toSet());
        if (versions.size() > 1) {
            // 检查时间戳是否接近
            long minTime = candidates.stream()
                .mapToLong(sub -> sub.getUpdatedAt().getSeconds())
                .min().orElse(0);
            long maxTime = candidates.stream()
                .mapToLong(sub -> sub.getUpdatedAt().getSeconds())
                .max().orElse(0);
                
            if ((maxTime - minTime) * 1000 < CONFLICT_DETECTION_THRESHOLD_MS) {
                return ConflictType.VERSION_CONFLICT;
            }
        }
        
        // 默认为重复订阅
        return ConflictType.DUPLICATE_SUBSCRIPTION;
    }
    
    /**
     * 记录冲突解决历史
     */
    private void recordConflictResolution(ConflictingSubscription conflict, ClusterSubscriptionInfo resolved) {
        String conflictId = conflict.getSubscriptionId() + ":" + System.currentTimeMillis();
        ConflictRecord record = new ConflictRecord(
            conflictId,
            conflict.getSubscriptionId(),
            conflict.getConflictType(),
            conflict.getConflictingVersionsCount(),
            resolved.getNodeId(),
            resolved.getVersion(),
            Instant.now()
        );
        
        conflictHistory.put(conflictId, record);
        
        // 限制历史记录数量
        if (conflictHistory.size() > 1000) {
            cleanupConflictHistory();
        }
        
        log.debug("Recorded conflict resolution: subscriptionId={}, resolvedVersion={}, resolvedNode={}",
            conflict.getSubscriptionId(), resolved.getVersion(), resolved.getNodeId());
    }
    
    /**
     * 清理冲突历史记录
     */
    private void cleanupConflictHistory() {
        long cutoffTime = Instant.now().minusSeconds(3600).getEpochSecond(); // 保留1小时内的记录
        
        List<String> expiredRecords = conflictHistory.entrySet().stream()
            .filter(entry -> entry.getValue().getResolvedAt().getEpochSecond() < cutoffTime)
            .map(Map.Entry::getKey)
            .collect(Collectors.toList());
            
        expiredRecords.forEach(conflictHistory::remove);
        
        log.debug("Cleaned up {} expired conflict records", expiredRecords.size());
    }
    
    /**
     * 获取冲突统计信息
     *
     * @return 冲突统计
     */
    public ConflictStatistics getConflictStatistics() {
        Map<ConflictType, Long> conflictTypeCounts = conflictHistory.values().stream()
            .collect(Collectors.groupingBy(
                ConflictRecord::getConflictType,
                Collectors.counting()
            ));
            
        return new ConflictStatistics(
            conflictHistory.size(),
            conflictTypeCounts,
            Instant.now()
        );
    }
    
    /**
     * 冲突解决策略枚举
     */
    private enum ConflictResolutionStrategy {
        LAST_WRITER_WINS,    // 最后写入者获胜
        FIRST_WRITER_WINS,   // 第一写入者获胜
        HIGHEST_VERSION      // 最高版本号获胜
    }
    
    /**
     * 冲突记录数据结构
     */
    @Data
    private static class ConflictRecord {
        private final String conflictId;
        private final String subscriptionId;
        private final ConflictType conflictType;
        private final int conflictCount;
        private final String resolvedNodeId;
        private final long resolvedVersion;
        private final Instant resolvedAt;
    }
    
    /**
     * 冲突统计信息
     */
    @Data
    public static class ConflictStatistics {
        private final int totalConflicts;
        private final Map<ConflictType, Long> conflictsByType;
        private final Instant statisticsTime;
    }
}