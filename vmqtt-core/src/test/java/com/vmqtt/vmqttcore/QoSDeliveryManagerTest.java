package com.vmqtt.vmqttcore;

import com.vmqtt.common.model.ClientSession;
import com.vmqtt.common.model.QueuedMessage;
import com.vmqtt.common.model.TopicSubscription;
import com.vmqtt.common.protocol.MqttQos;
import com.vmqtt.common.service.MessageRouter;
import com.vmqtt.common.service.SessionManager;
import com.vmqtt.vmqttcore.service.QoSDeliveryManager;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.time.LocalDateTime;
import java.util.Arrays;
import java.util.Collections;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class QoSDeliveryManagerTest {

    @Mock
    private SessionManager sessionManager;
    @Mock
    private MessageRouter messageRouter;

    private QoSDeliveryManager deliveryManager;

    @BeforeEach
    void setUp() {
        deliveryManager = new QoSDeliveryManager(sessionManager, messageRouter);
    }

    @Test
    void testDeliverToSubscribers_QoS0() {
        ClientSession s = baseSession("c1");
        s.addSubscription(TopicSubscription.builder().clientId("c1").topicFilter("t/0")
                .qos(MqttQos.AT_MOST_ONCE).build());

        when(messageRouter.sendMessagesToClients(anyList())).thenReturn(CompletableFuture.completedFuture(new DummyBatchResult()));

        deliveryManager.deliverToSubscribers("t/0", new byte[]{1}, MqttQos.AT_MOST_ONCE, false,
                Collections.singletonList(s), "pub").join();

        // QoS0 无需 inflight
        verify(sessionManager, never()).addInflightMessage(anyString(), anyInt(), any());
    }

    @Test
    void testDeliverToSubscribers_QoS1() {
        ClientSession s = baseSession("c1");
        s.addSubscription(TopicSubscription.builder().clientId("c1").topicFilter("t/1")
                .qos(MqttQos.AT_LEAST_ONCE).build());

        when(messageRouter.sendMessagesToClients(anyList())).thenReturn(CompletableFuture.completedFuture(new DummyBatchResult()));
        when(sessionManager.addInflightMessage(anyString(), anyInt(), any())).thenReturn(CompletableFuture.completedFuture(true));

        deliveryManager.deliverToSubscribers("t/1", new byte[]{1}, MqttQos.AT_LEAST_ONCE, false,
                Collections.singletonList(s), "pub").join();

        verify(sessionManager, atLeastOnce()).addInflightMessage(eq("c1"), anyInt(), any());
    }

    @Test
    void testHandlePubAck() {
        when(sessionManager.removeInflightMessage(eq("c1"), eq(10))).thenReturn(CompletableFuture.completedFuture(Optional.of(QueuedMessage.builder().messageId("m").build())));
        deliveryManager.handlePubAck("c1", 10);
        verify(sessionManager, times(1)).removeInflightMessage("c1", 10);
    }

    private ClientSession baseSession(String clientId) {
        return ClientSession.builder()
                .sessionId("sess_" + clientId)
                .clientId(clientId)
                .cleanSession(false)
                .state(ClientSession.SessionState.ACTIVE)
                .createdAt(LocalDateTime.now())
                .lastActivity(LocalDateTime.now())
                .build();
    }

    private static class DummyBatchResult implements MessageRouter.BatchSendResult {
        @Override
        public int getTotalSent() { return 1; }
        @Override
        public int getSuccessfulSent() { return 1; }
        @Override
        public int getFailedSent() { return 0; }
        @Override
        public long getSendTimeMs() { return 1; }
    }
}


