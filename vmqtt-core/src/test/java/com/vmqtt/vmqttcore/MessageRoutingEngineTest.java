package com.vmqtt.vmqttcore;

import com.vmqtt.common.model.ClientSession;
import com.vmqtt.common.model.TopicSubscription;
import com.vmqtt.common.protocol.MqttQos;
import com.vmqtt.common.service.MessageRouter;
import com.vmqtt.common.service.SessionManager;
import com.vmqtt.vmqttcore.service.MessageRoutingEngine;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.time.LocalDateTime;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class MessageRoutingEngineTest {

    @Mock
    private SessionManager sessionManager;
    @Mock
    private MessageRouter messageRouter;

    private MessageRoutingEngine engine;

    @BeforeEach
    void setUp() {
        engine = new MessageRoutingEngine(sessionManager, messageRouter);
    }

    @Test
    void testTopicMatch() {
        assertTrue(engine.topicMatches("a/b/c", "a/b/c"));
        assertTrue(engine.topicMatches("a/b/c", "a/+/c"));
        assertTrue(engine.topicMatches("a/b/c", "a/#"));
        assertFalse(engine.topicMatches("a/b/c", "a/+"));
    }

    @Test
    void testMatchSubscribersWithSharedGroups() {
        ClientSession s1 = baseSession("c1");
        ClientSession s2 = baseSession("c2");
        ClientSession s3 = baseSession("c3");

        s1.addSubscription(TopicSubscription.builder().clientId("c1").topicFilter("$share/g1/sensors/+/t")
                .qos(MqttQos.AT_LEAST_ONCE).build());
        s2.addSubscription(TopicSubscription.builder().clientId("c2").topicFilter("$share/g1/sensors/+/t")
                .qos(MqttQos.AT_LEAST_ONCE).build());
        s3.addSubscription(TopicSubscription.builder().clientId("c3").topicFilter("sensors/room1/t")
                .qos(MqttQos.AT_LEAST_ONCE).build());

        when(sessionManager.getSubscribedSessions("sensors/room1/t")).thenReturn(Arrays.asList(s1, s2, s3));

        List<ClientSession> selected = engine.matchSubscribers("sensors/room1/t");
        // 共享组g1应该被挑选一个，加上普通订阅s3，总数应为2
        assertEquals(2, selected.size());
    }

    @Test
    void testRouteDelegatesToMessageRouter() {
        when(messageRouter.routeMessage(anyString(), anyString(), any(), any(), anyBoolean()))
                .thenReturn(CompletableFuture.completedFuture(null));
        engine.route("pub", "t", new byte[]{1,2,3}, MqttQos.AT_MOST_ONCE, false).join();
        verify(messageRouter, times(1)).routeMessage(anyString(), anyString(), any(), any(), anyBoolean());
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
}


