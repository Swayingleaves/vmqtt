package com.vmqtt.vmqttcore.service;

import com.vmqtt.common.model.ClientSession;
import com.vmqtt.common.model.QueuedMessage;
import com.vmqtt.common.model.TopicSubscription;
import com.vmqtt.common.protocol.MqttQos;
import com.vmqtt.common.service.MessageRouter;
import com.vmqtt.common.service.SessionManager;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

/**
 * QoS 交付管理器
 * - 负责消息在不同 QoS 等级下的入队、发送、确认与重传
 * - 与 SessionManager 协作维护 inflight 消息
 */
@Slf4j
@Service
public class QoSDeliveryManager {

    private final SessionManager sessionManager;
    private final MessageRouter messageRouter;

    private final ScheduledExecutorService retransmitScheduler;
    private final ConcurrentMap<String, AtomicInteger> clientIdToPacketId = new ConcurrentHashMap<>();
    private final ConcurrentMap<String, ConcurrentSkipListSet<Integer>> processedQoS1AckIds = new ConcurrentHashMap<>();
    private final ConcurrentMap<String, ConcurrentSkipListSet<Integer>> processedQoS2RecIds = new ConcurrentHashMap<>();
    private final ConcurrentMap<String, ConcurrentSkipListSet<Integer>> processedQoS2CompIds = new ConcurrentHashMap<>();

    // 基础重传参数（后续可做成配置）
    private static final long QOS1_ACK_TIMEOUT_MS = 5_000;
    private static final long QOS2_ACK_TIMEOUT_MS = 5_000;

    public QoSDeliveryManager(SessionManager sessionManager, MessageRouter messageRouter) {
        this.sessionManager = sessionManager;
        this.messageRouter = messageRouter;
        this.retransmitScheduler = Executors.newScheduledThreadPool(2, r -> {
            Thread t = new Thread(r, "qos-retransmit");
            t.setDaemon(true);
            return t;
        });
    }

    /**
     * 将消息按订阅者有效 QoS 投递
     */
    public CompletableFuture<MessageRouter.BatchSendResult> deliverToSubscribers(String topic,
                                                                                byte[] payload,
                                                                                MqttQos publishQos,
                                                                                boolean retain,
                                                                                List<ClientSession> subscribers,
                                                                                String publisherClientId) {
        // 为每个订阅者计算有效 QoS，并构造各自的消息
        List<MessageRouter.ClientMessage> clientMessages = subscribers.stream()
            .map(session -> buildClientMessageForSession(session, topic, payload, publishQos, retain))
            .filter(Objects::nonNull)
            .collect(Collectors.toList());

        // 先批量发送
        CompletableFuture<MessageRouter.BatchSendResult> resultFuture = messageRouter.sendMessagesToClients(clientMessages);

        // 针对需要确认的消息，登记 inflight 并安排重传
        resultFuture.thenAccept(result -> clientMessages.forEach(cm -> {
            QueuedMessage msg = cm.getMessage();
            if (msg.getQos().requiresAcknowledgment()) {
                int packetId = nextPacketId(cm.getClientId());
                msg.setPacketId(packetId);
                sessionManager.addInflightMessage(cm.getClientId(), packetId, msg).join();
                scheduleRetransmission(cm.getClientId(), msg);
            }
        })).exceptionally(ex -> {
            log.warn("批量发送后登记inflight失败: {}", ex.getMessage());
            return null;
        });

        return resultFuture;
    }

    /**
     * 处理来自客户端的 PUBACK（QoS1）
     */
    public void handlePubAck(String clientId, int packetId) {
        // 去重：忽略重复ACK
        if (isDuplicate(processedQoS1AckIds, clientId, packetId)) {
            log.debug("重复PUBACK被忽略: clientId={}, packetId={}", clientId, packetId);
            return;
        }
        Optional<QueuedMessage> removed = sessionManager.removeInflightMessage(clientId, packetId).join();
        if (removed.isPresent()) {
            log.debug("收到PUBACK并移除inflight: clientId={}, packetId={}, msgId={}", clientId, packetId, removed.get().getMessageId());
        } else {
            log.debug("PUBACK对应消息未找到: clientId={}, packetId={}", clientId, packetId);
        }
    }

    /**
     * 处理来自客户端的 PUBREC（QoS2）
     */
    public void handlePubRec(String clientId, int packetId) {
        // 去重：忽略重复PUBREC
        if (isDuplicate(processedQoS2RecIds, clientId, packetId)) {
            log.debug("重复PUBREC被忽略: clientId={}, packetId={}", clientId, packetId);
            return;
        }
        sessionManager.getInflightMessage(clientId, packetId).ifPresent(msg -> {
            msg.handlePubRec();
            // 对端已收到，等待 PUBCOMP；此处可根据需要立即发送 PUBREL（由FE处理网络包）。
        });
    }

    /**
     * 处理来自客户端的 PUBCOMP（QoS2）
     */
    public void handlePubComp(String clientId, int packetId) {
        // 去重：忽略重复PUBCOMP
        if (isDuplicate(processedQoS2CompIds, clientId, packetId)) {
            log.debug("重复PUBCOMP被忽略: clientId={}, packetId={}", clientId, packetId);
            return;
        }
        Optional<QueuedMessage> removed = sessionManager.removeInflightMessage(clientId, packetId).join();
        if (removed.isPresent()) {
            log.debug("收到PUBCOMP并完成QoS2: clientId={}, packetId={}, msgId={}", clientId, packetId, removed.get().getMessageId());
        }
    }

    private MessageRouter.ClientMessage buildClientMessageForSession(ClientSession session,
                                                                    String topic,
                                                                    byte[] payload,
                                                                    MqttQos publishQos,
                                                                    boolean retain) {
        TopicSubscription subscription = session.getMatchingSubscription(topic);
        if (subscription == null) {
            return null;
        }
        MqttQos effectiveQos = subscription.getEffectiveQos(publishQos);
        QueuedMessage message = QueuedMessage.builder()
            .messageId(generateMessageId())
            .clientId(session.getClientId())
            .sessionId(session.getSessionId())
            .topic(topic)
            .payload(payload)
            .qos(effectiveQos)
            .retain(retain)
            .build();

        return new ClientMessageImpl(session.getClientId(), message);
    }

    private void scheduleRetransmission(String clientId, QueuedMessage message) {
        long timeout = message.getQos() == MqttQos.AT_LEAST_ONCE ? QOS1_ACK_TIMEOUT_MS : QOS2_ACK_TIMEOUT_MS;
        retransmitScheduler.schedule(() -> {
            try {
                // 仍在 inflight 才进行重传
                Optional<QueuedMessage> inflight = sessionManager.getInflightMessage(clientId, message.getPacketId());
                if (!inflight.isPresent()) {
                    return;
                }

                QueuedMessage current = inflight.get();
                if (!current.canRetry()) {
                    log.warn("消息达到最大重传次数或过期，将标记失败: clientId={}, packetId={}, msgId={}",
                        clientId, current.getPacketId(), current.getMessageId());
                    current.markAsFailed();
                    return;
                }

                current.incrementRetryCount();
                log.debug("触发重传: clientId={}, packetId={}, retryCount={}", clientId, current.getPacketId(), current.getRetryCount());
                current.setDuplicate(true);
                messageRouter.sendMessageToClient(clientId, current).join();

                // 继续安排下一次重传
                scheduleRetransmission(clientId, current);

            } catch (Exception e) {
                log.warn("重传异常: clientId={}, packetId={}, err={}", clientId, message.getPacketId(), e.getMessage());
            }
        }, timeout, TimeUnit.MILLISECONDS);
    }

    private int nextPacketId(String clientId) {
        return clientIdToPacketId.computeIfAbsent(clientId, k -> new AtomicInteger(1))
            .updateAndGet(prev -> prev >= 65535 ? 1 : prev + 1);
    }

    private String generateMessageId() {
        return "msg_" + java.util.UUID.randomUUID().toString().replace("-", "");
    }

    private boolean isDuplicate(ConcurrentMap<String, ConcurrentSkipListSet<Integer>> store, String clientId, int packetId) {
        ConcurrentSkipListSet<Integer> set = store.computeIfAbsent(clientId, k -> new ConcurrentSkipListSet<>());
        // 如果添加失败，说明已存在，判为重复
        return !set.add(packetId);
    }

    private static class ClientMessageImpl implements MessageRouter.ClientMessage {
        private final String clientId;
        private final QueuedMessage message;

        private ClientMessageImpl(String clientId, QueuedMessage message) {
            this.clientId = clientId;
            this.message = message;
        }

        @Override
        public String getClientId() {
            return clientId;
        }

        @Override
        public QueuedMessage getMessage() {
            return message;
        }
    }
}


