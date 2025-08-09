package com.vmqtt.vmqttcore.service;

import com.vmqtt.common.model.ClientSession;
import com.vmqtt.common.model.QueuedMessage;
import com.vmqtt.common.model.TopicSubscription;
import com.vmqtt.common.protocol.MqttQos;
import com.vmqtt.common.service.MessageRouter;
import com.vmqtt.common.service.SessionManager;
import com.vmqtt.vmqttcore.service.router.LoadBalancer;
import com.vmqtt.vmqttcore.service.router.RoundRobinLoadBalancer;
import com.vmqtt.vmqttcore.service.router.TopicMatcher;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

/**
 * 核心消息路由引擎
 * - 主题匹配
 * - 共享订阅选择
 * - 分发到订阅者
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class MessageRoutingEngine {

    private final SessionManager sessionManager;
    private final MessageRouter messageRouter;

    private final LoadBalancer<ClientSession> sharedSubLoadBalancer = new RoundRobinLoadBalancer<>();

    public CompletableFuture<MessageRouter.RouteResult> route(String publisherClientId, String topic, byte[] payload, MqttQos qos, boolean retain) {
        // 使用已有的 MessageRouter 实现承载分发，核心引擎负责前置的匹配与筛选（共享订阅）
        return messageRouter.routeMessage(publisherClientId, topic, io.netty.buffer.Unpooled.wrappedBuffer(payload), qos, retain);
    }

    public List<ClientSession> matchSubscribers(String topic) {
        List<ClientSession> candidates = sessionManager.getSubscribedSessions(topic);
        if (candidates.isEmpty()) return candidates;

        // 将共享订阅分组，每个组内挑选一个
        Map<String, List<ClientSession>> sharedGroups = new HashMap<>();
        List<ClientSession> normalSubs = new ArrayList<>();

        for (ClientSession session : candidates) {
            // 使用核心匹配器遍历会话内所有订阅，找到任一匹配的订阅
            TopicSubscription matched = session.getSubscriptions().values().stream()
                    .filter(s -> TopicMatcher.matches(topic, s.getActualTopicFilter()))
                    .findFirst()
                    .orElse(null);

            if (matched == null) {
                continue;
            }

            String filter = matched.getTopicFilter();
            if (filter != null && filter.startsWith("$share/")) {
                String[] parts = filter.split("/", 3);
                if (parts.length >= 3) {
                    String groupKey = parts[1] + ":" + parts[2];
                    sharedGroups.computeIfAbsent(groupKey, k -> new ArrayList<>()).add(session);
                } else {
                    normalSubs.add(session);
                }
            } else {
                normalSubs.add(session);
            }
        }

        // 对每个共享组按轮询选择一个会话
        List<ClientSession> selectedShared = sharedGroups.entrySet().stream()
                .map(e -> sharedSubLoadBalancer.select(e.getValue(), e.getKey()))
                .filter(Objects::nonNull)
                .collect(Collectors.toList());

        List<ClientSession> result = new ArrayList<>(normalSubs);
        result.addAll(selectedShared);
        return result;
    }

    public boolean topicMatches(String topic, String filter) {
        return TopicMatcher.matches(topic, filter);
    }
}


