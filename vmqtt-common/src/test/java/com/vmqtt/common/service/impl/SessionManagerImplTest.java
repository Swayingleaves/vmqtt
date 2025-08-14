/**
 * 会话管理器实现类测试
 *
 * @author zhenglin
 * @date 2025/08/06
 */
package com.vmqtt.common.service.impl;

import com.vmqtt.common.model.ClientSession;
import com.vmqtt.common.model.QueuedMessage;
import com.vmqtt.common.model.TopicSubscription;
import com.vmqtt.common.protocol.MqttQos;
import com.vmqtt.common.service.SessionManager;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.time.LocalDateTime;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.jupiter.api.Assertions.*;

/**
 * 会话管理器实现类单元测试
 */
class SessionManagerImplTest {

    private SessionManagerImpl sessionManager;

    @BeforeEach
    void setUp() {
        sessionManager = new SessionManagerImpl();
    }

    @Test
    void testCreateNewSession() {
        // 创建新会话
        SessionManager.SessionResult result = sessionManager.createOrGetSession("client_1", true, "conn_1").join();

        assertNotNull(result);
        assertTrue(result.isNewSession());
        assertFalse(result.isSessionPresent());

        ClientSession session = result.getSession();
        assertNotNull(session);
        assertEquals("client_1", session.getClientId());
        assertEquals("conn_1", session.getConnectionId());
        assertTrue(session.isCleanSession());
        assertEquals(ClientSession.SessionState.ACTIVE, session.getState());
    }

    @Test
    void testGetExistingSessionWithCleanSession() {
        // 先创建一个会话
        sessionManager.createOrGetSession("client_1", false, "conn_1").join();

        // 使用cleanSession=true重新连接
        SessionManager.SessionResult result = sessionManager.createOrGetSession("client_1", true, "conn_2").join();

        assertTrue(result.isNewSession());
        assertFalse(result.isSessionPresent());
        assertEquals("conn_2", result.getSession().getConnectionId());
    }

    @Test
    void testGetExistingSessionWithoutCleanSession() {
        // 先创建一个会话
        SessionManager.SessionResult firstResult = sessionManager.createOrGetSession("client_1", false, "conn_1").join();
        String originalSessionId = firstResult.getSession().getSessionId();

        // 使用cleanSession=false重新连接
        SessionManager.SessionResult result = sessionManager.createOrGetSession("client_1", false, "conn_2").join();

        assertFalse(result.isNewSession());
        assertTrue(result.isSessionPresent());
        assertEquals("conn_2", result.getSession().getConnectionId());
        assertEquals(originalSessionId, result.getSession().getSessionId());
    }

    @Test
    void testGetSession() {
        // 创建会话
        sessionManager.createOrGetSession("client_1", false, "conn_1").join();

        // 获取会话
        Optional<ClientSession> session = sessionManager.getSession("client_1");
        assertTrue(session.isPresent());
        assertEquals("client_1", session.get().getClientId());

        // 获取不存在的会话
        Optional<ClientSession> notFound = sessionManager.getSession("not_exist");
        assertFalse(notFound.isPresent());
    }

    @Test
    void testGetSessionById() {
        // 创建会话
        SessionManager.SessionResult result = sessionManager.createOrGetSession("client_1", false, "conn_1").join();
        String sessionId = result.getSession().getSessionId();

        // 根据会话ID获取会话
        Optional<ClientSession> session = sessionManager.getSessionById(sessionId);
        assertTrue(session.isPresent());
        assertEquals("client_1", session.get().getClientId());

        // 获取不存在的会话
        Optional<ClientSession> notFound = sessionManager.getSessionById("not_exist");
        assertFalse(notFound.isPresent());
    }

    @Test
    void testRemoveSession() {
        // 创建会话
        sessionManager.createOrGetSession("client_1", false, "conn_1").join();

        // 移除会话
        Optional<ClientSession> removed = sessionManager.removeSession("client_1").join();
        assertTrue(removed.isPresent());
        assertEquals("client_1", removed.get().getClientId());

        // 验证会话已移除
        Optional<ClientSession> session = sessionManager.getSession("client_1");
        assertFalse(session.isPresent());
    }

    @Test
    void testCleanSession() {
        // 创建会话并添加订阅和消息
        sessionManager.createOrGetSession("client_1", false, "conn_1").join();

        TopicSubscription subscription = createTestSubscription("test/topic", MqttQos.AT_LEAST_ONCE);
        sessionManager.addSubscription("client_1", subscription).join();

        QueuedMessage message = createTestMessage("msg_1", "test/topic", "test message");
        sessionManager.addQueuedMessage("client_1", message).join();

        // 清理会话
        boolean result = sessionManager.cleanSession("client_1").join();
        assertTrue(result);

        // 验证订阅和消息已清理
        List<TopicSubscription> subscriptions = sessionManager.getSubscriptions("client_1");
        assertTrue(subscriptions.isEmpty());

        List<QueuedMessage> messages = sessionManager.getQueuedMessages("client_1", 100);
        assertTrue(messages.isEmpty());
    }

    @Test
    void testAddSubscription() {
        // 创建会话
        sessionManager.createOrGetSession("client_1", false, "conn_1").join();

        // 添加订阅
        TopicSubscription subscription = createTestSubscription("test/topic", MqttQos.AT_LEAST_ONCE);
        boolean result = sessionManager.addSubscription("client_1", subscription).join();
        assertTrue(result);

        // 验证订阅已添加
        List<TopicSubscription> subscriptions = sessionManager.getSubscriptions("client_1");
        assertEquals(1, subscriptions.size());
        assertEquals("test/topic", subscriptions.get(0).getTopicFilter());
    }

    @Test
    void testRemoveSubscription() {
        // 创建会话并添加订阅
        sessionManager.createOrGetSession("client_1", false, "conn_1").join();
        TopicSubscription subscription = createTestSubscription("test/topic", MqttQos.AT_LEAST_ONCE);
        sessionManager.addSubscription("client_1", subscription).join();

        // 移除订阅
        Optional<TopicSubscription> removed = sessionManager.removeSubscription("client_1", "test/topic").join();
        assertTrue(removed.isPresent());
        assertEquals("test/topic", removed.get().getTopicFilter());

        // 验证订阅已移除
        List<TopicSubscription> subscriptions = sessionManager.getSubscriptions("client_1");
        assertTrue(subscriptions.isEmpty());
    }

    @Test
    void testGetSubscribedSessions() {
        // 创建多个会话并添加订阅
        sessionManager.createOrGetSession("client_1", false, "conn_1").join();
        sessionManager.createOrGetSession("client_2", false, "conn_2").join();

        TopicSubscription subscription1 = createTestSubscription("test/topic", MqttQos.AT_LEAST_ONCE);
        TopicSubscription subscription2 = createTestSubscription("test/#", MqttQos.AT_LEAST_ONCE);

        sessionManager.addSubscription("client_1", subscription1).join();
        sessionManager.addSubscription("client_2", subscription2).join();

        // 获取订阅了指定主题的会话
        List<ClientSession> subscribedSessions = sessionManager.getSubscribedSessions("test/topic");
        assertEquals(2, subscribedSessions.size()); // 两个客户端都应该匹配
    }

    @Test
    void testAddQueuedMessage() {
        // 创建会话
        sessionManager.createOrGetSession("client_1", false, "conn_1").join();

        // 添加排队消息
        QueuedMessage message = createTestMessage("msg_1", "test/topic", "test message");
        boolean result = sessionManager.addQueuedMessage("client_1", message).join();
        assertTrue(result);

        // 验证消息已添加
        List<QueuedMessage> messages = sessionManager.getQueuedMessages("client_1", 100);
        assertEquals(1, messages.size());
        assertEquals("msg_1", messages.get(0).getMessageId());
    }

    @Test
    void testRemoveQueuedMessage() {
        // 创建会话并添加消息
        sessionManager.createOrGetSession("client_1", false, "conn_1").join();
        QueuedMessage message = createTestMessage("msg_1", "test/topic", "test message");
        sessionManager.addQueuedMessage("client_1", message).join();

        // 移除排队消息
        Optional<QueuedMessage> removed = sessionManager.removeQueuedMessage("client_1", "msg_1").join();
        assertTrue(removed.isPresent());
        assertEquals("msg_1", removed.get().getMessageId());

        // 验证消息已移除
        List<QueuedMessage> messages = sessionManager.getQueuedMessages("client_1", 100);
        assertTrue(messages.isEmpty());
    }

    @Test
    void testInflightMessages() {
        // 创建会话
        sessionManager.createOrGetSession("client_1", false, "conn_1").join();

        // 添加传输中消息
        QueuedMessage message = createTestMessage("msg_1", "test/topic", "test message");
        boolean added = sessionManager.addInflightMessage("client_1", 1001, message).join();
        assertTrue(added);

        // 获取传输中消息
        Optional<QueuedMessage> retrieved = sessionManager.getInflightMessage("client_1", 1001);
        assertTrue(retrieved.isPresent());
        assertEquals("msg_1", retrieved.get().getMessageId());

        // 移除传输中消息
        Optional<QueuedMessage> removed = sessionManager.removeInflightMessage("client_1", 1001).join();
        assertTrue(removed.isPresent());
        assertEquals("msg_1", removed.get().getMessageId());

        // 验证消息已移除
        Optional<QueuedMessage> notFound = sessionManager.getInflightMessage("client_1", 1001);
        assertFalse(notFound.isPresent());
    }

    @Test
    void testUpdateSessionActivity() {
        // 创建会话
        SessionManager.SessionResult result = sessionManager.createOrGetSession("client_1", false, "conn_1").join();
        LocalDateTime originalActivity = result.getSession().getLastActivity();

        // 等待一小段时间
        try {
            Thread.sleep(10);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }

        // 更新会话活动时间
        sessionManager.updateSessionActivity("client_1");

        // 验证活动时间已更新
        Optional<ClientSession> updated = sessionManager.getSession("client_1");
        assertTrue(updated.isPresent());
        assertTrue(updated.get().getLastActivity().isAfter(originalActivity));
    }

    @Test
    void testHasSession() {
        // 会话不存在时
        assertFalse(sessionManager.hasSession("client_1"));

        // 创建会话
        sessionManager.createOrGetSession("client_1", false, "conn_1").join();

        // 会话存在时
        assertTrue(sessionManager.hasSession("client_1"));
    }

    @Test
    void testGetActiveSessions() {
        // 创建多个会话
        sessionManager.createOrGetSession("client_1", false, "conn_1").join();
        sessionManager.createOrGetSession("client_2", false, "conn_2").join();

        Collection<ClientSession> activeSessions = sessionManager.getActiveSessions();
        assertEquals(2, activeSessions.size());
    }

    @Test
    void testGetSessionCount() {
        assertEquals(0, sessionManager.getSessionCount());

        // 创建会话
        sessionManager.createOrGetSession("client_1", false, "conn_1").join();
        assertEquals(1, sessionManager.getSessionCount());
    }

    @Test
    void testSessionEventListener() {
        AtomicBoolean sessionCreated = new AtomicBoolean(false);
        AtomicBoolean subscriptionAdded = new AtomicBoolean(false);

        // 添加事件监听器
        SessionManager.SessionEventListener listener = new SessionManager.SessionEventListener() {
            @Override
            public void onSessionCreated(ClientSession session) {
                sessionCreated.set(true);
            }

            @Override
            public void onSessionDestroyed(ClientSession session) {
                // 测试中不使用
            }

            @Override
            public void onSessionStateChanged(ClientSession session, 
                                            ClientSession.SessionState oldState, 
                                            ClientSession.SessionState newState) {
                // 测试中不使用
            }

            @Override
            public void onSubscriptionAdded(ClientSession session, TopicSubscription subscription) {
                subscriptionAdded.set(true);
            }

            @Override
            public void onSubscriptionRemoved(ClientSession session, TopicSubscription subscription) {
                // 测试中不使用
            }

            @Override
            public void onMessageQueued(ClientSession session, QueuedMessage message) {
                // 测试中不使用
            }

            @Override
            public void onMessageDequeued(ClientSession session, QueuedMessage message) {
                // 测试中不使用
            }
        };

        sessionManager.addSessionListener(listener);

        // 创建会话，触发创建事件
        sessionManager.createOrGetSession("client_1", false, "conn_1").join();
        assertTrue(sessionCreated.get());

        // 添加订阅，触发订阅事件
        TopicSubscription subscription = createTestSubscription("test/topic", MqttQos.AT_LEAST_ONCE);
        sessionManager.addSubscription("client_1", subscription).join();
        assertTrue(subscriptionAdded.get());

        // 移除监听器
        sessionManager.removeSessionListener(listener);
    }

    @Test
    void testGetSessionStats() {
        SessionManager.SessionStats stats = sessionManager.getSessionStats();
        assertNotNull(stats);

        // 初始统计信息应该为0
        assertEquals(0, stats.getTotalSessions());
        assertEquals(0, stats.getActiveSessions());
        assertEquals(0, stats.getTotalSubscriptions());
    }

    /**
     * 创建测试用的主题订阅
     */
    private TopicSubscription createTestSubscription(String topicFilter, MqttQos qos) {
        return TopicSubscription.builder()
                .topicFilter(topicFilter)
                .qos(qos)
                .subscribedAt(LocalDateTime.now())
                .build();
    }

    /**
     * 创建测试用的排队消息
     */
    private QueuedMessage createTestMessage(String messageId, String topic, String payloadText) {
        byte[] payloadBytes = payloadText.getBytes(StandardCharsets.UTF_8);
        
        return QueuedMessage.builder()
                .messageId(messageId)
                .topic(topic)
                .payload(payloadBytes)
                .qos(MqttQos.AT_LEAST_ONCE)
                .retain(false)
                .createdAt(LocalDateTime.now())
                .build();
    }
}