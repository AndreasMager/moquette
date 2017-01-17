/*
 * Copyright (c) 2012-2017 The original author or authorsgetRockQuestions()
 * ------------------------------------------------------
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Eclipse Public License v1.0
 * and Apache License v2.0 which accompanies this distribution.
 *
 * The Eclipse Public License is available at
 * http://www.eclipse.org/legal/epl-v10.html
 *
 * The Apache License v2.0 is available at
 * http://www.opensource.org/licenses/apache2.0.php
 *
 * You may elect to redistribute this code under either of these licenses.
 */

package io.moquette.spi.persistence;

import io.moquette.spi.ClientSession;
import io.moquette.spi.ISubscriptionsStore.ClientTopicCouple;
import io.moquette.spi.IMessagesStore;
import io.moquette.spi.IMessagesStore.Message;
import io.moquette.spi.IMessagesStore.StoredMessage;
import io.moquette.spi.ISessionsStore;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import io.moquette.spi.impl.subscriptions.Subscription;
import io.moquette.spi.impl.subscriptions.Topic;
import io.netty.handler.codec.mqtt.MqttQoS;
import org.junit.Before;
import org.junit.Test;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import static com.jayway.awaitility.Awaitility.await;
import static org.assertj.core.api.Assertions.*;

public abstract class AbstractStoreTest {

    public static final String TEST_CLIENT = "TestClient";

    protected ISessionsStore m_sessionsStore;
    protected IMessagesStore m_messagesStore;

    public IMessagesStore.StoredMessage publishToStore;

    @Before
    public void setup()  {
        publishToStore = new IMessagesStore.StoredMessage(
                "Hello".getBytes(),
                MqttQoS.EXACTLY_ONCE,
                "id1/topic");
        publishToStore.setClientID(TEST_CLIENT);
        publishToStore.setRetained(false);
    }

    @Test
    public void overridingSubscriptions() {
        ClientSession session1 = m_sessionsStore.createNewSession("SESSION_ID_1", true);

        // Subscribe on /topic with QOSType.MOST_ONE
        Subscription oldSubscription = new Subscription(session1.clientID, new Topic("/topic"), MqttQoS.AT_MOST_ONCE);
        session1.subscribe(oldSubscription);

        // Subscribe on /topic again that overrides the previous subscription.
        Subscription overridingSubscription = new Subscription(
                session1.clientID,
                new Topic("/topic"),
                MqttQoS.EXACTLY_ONCE);
        session1.subscribe(overridingSubscription);

        // Verify
        List<ClientTopicCouple> subscriptions = m_sessionsStore.listAllSubscriptions();
        assertThat(subscriptions).hasSize(1);
        Subscription sub = m_sessionsStore.getSubscription(subscriptions.get(0));
        assertThat(sub.getRequestedQos()).isEqualTo(overridingSubscription.getRequestedQos());
    }

    @Test
    public void testNextPacketID_notExistingClientSession() {
        int packetId = m_sessionsStore.nextPacketID("NOT_EXISTING_CLI");
        assertThat(packetId).isEqualTo(1);
    }

    @Test
    public void testNextPacketID_existingClientSession() {
        // Force creation of inflight map for the CLIENT session
        int packetId = m_sessionsStore.nextPacketID("CLIENT");
        assertThat(packetId).isEqualTo(1);

        // request a second packetID
        packetId = m_sessionsStore.nextPacketID("CLIENT");
        assertThat(packetId).isEqualTo(2);
    }

    @Test
    public void testNextPacketID() {
        // request a first ID

        int packetId = m_sessionsStore.nextPacketID("CLIENT");
        m_sessionsStore.inFlight("CLIENT", packetId, UUID.randomUUID()); //simulate an inflight
        assertThat(packetId).isEqualTo(1);

        // release the ID
        m_sessionsStore.inFlightAck("CLIENT", packetId);

        // request a second packetID, counter restarts from 0
        packetId = m_sessionsStore.nextPacketID("CLIENT");
        assertThat(packetId).isEqualTo(1);
    }

    @Test
    public void testDropMessagesInSessionCleanAllNotRetainedStoredMessages() {
        m_sessionsStore.createNewSession(TEST_CLIENT, true);
        IMessagesStore.StoredMessage publishToStore = new IMessagesStore.StoredMessage(
                "Hello".getBytes(),
                MqttQoS.EXACTLY_ONCE,
                "/topic");
        publishToStore.setClientID(TEST_CLIENT);
        publishToStore.setRetained(false);
        UUID guid = m_messagesStore.storePublishForFuture(publishToStore);

        // Exercise
        m_messagesStore.dropInFlightMessagesInSession(Arrays.asList(guid));

        // Verify the message store for session is empty.
        IMessagesStore.StoredMessage storedPublish = m_messagesStore.getMessageByGuid(guid);
        assertThat(storedPublish).as("The stored message must'n be present anymore").isNull();
    }

    @Test
    public void testDropMessagesInSessionDoesntCleanAnyRetainedStoredMessages() {
        m_sessionsStore.createNewSession(TEST_CLIENT, true);

        List<Subscription> matcher = Arrays.asList(new Subscription("", new Topic("id1/topic"), MqttQoS.AT_LEAST_ONCE));

        await().until(() -> assertThat(m_messagesStore.searchMatching(matcher))
                .as("The stored must not contain the retained message")
                .isEmpty());

        m_messagesStore.storeRetained(new Topic(publishToStore.getTopic()), publishToStore);

        await().until(() -> assertThat(m_messagesStore.searchMatching(matcher))
                .as("The stored retained message must be present before client's session drop")
                .isNotEmpty());

        // Exercise
        m_messagesStore.dropInFlightMessagesInSession(m_sessionsStore.pendingAck("TestClient"));

        await().until(() ->
            assertThat(m_messagesStore.searchMatching(matcher))
                .as("The stored retained message must be present after client's session drop")
                .isNotEmpty());
    }

    @Test
    public void checkRetained() {
        Message message = new Message("message".getBytes(), MqttQoS.AT_MOST_ONCE, "id1/topic");
        m_messagesStore.storeRetained(new Topic(message.getTopic()), message);

        Message message2 = new Message("message".getBytes(), MqttQoS.AT_MOST_ONCE, "id1/topic2");
        m_messagesStore.storeRetained(new Topic(message2.getTopic()), message2);

        Message message3 = new Message("message".getBytes(), MqttQoS.AT_MOST_ONCE, "id2/topic2");
        m_messagesStore.storeRetained(new Topic(message3.getTopic()), message3);

        Subscription subscription1 = new Subscription("cid", new Topic("id1/#"), MqttQoS.AT_MOST_ONCE);

        Subscription subscription2 = new Subscription("cid", new Topic("id1/topic"), MqttQoS.AT_MOST_ONCE);

        await().until(() -> {
            Map<Subscription, Collection<Message>> result = m_messagesStore
                    .searchMatching(Arrays.asList(subscription1, subscription2));

            assertThat(result).containsOnlyKeys(subscription1, subscription2);

            assertThat(result.get(subscription1)).containsExactlyInAnyOrder(message, message2);
            assertThat(result.get(subscription2)).containsExactlyInAnyOrder(message);
        });

        m_messagesStore.cleanRetained(new Topic("id1/topic2"));

        await().until(() -> {
            Map<Subscription, Collection<Message>> result = m_messagesStore
                    .searchMatching(Arrays.asList(subscription1, subscription2));

            assertThat(result).containsOnlyKeys(subscription1, subscription2);

            assertThat(result.get(subscription1)).containsExactlyInAnyOrder(message);
            assertThat(result.get(subscription2)).containsExactlyInAnyOrder(message);
        });

        m_messagesStore.cleanRetained(new Topic("id1/topic"));

        await().until(() -> {
            Map<Subscription, Collection<Message>> result = m_messagesStore
                    .searchMatching(Arrays.asList(subscription1, subscription2));

            assertThat(result).isEmpty();
        });
    }

    @Test
    public void singleTopicRetained() {
        // This message will be stored in a wrong place id1/
        Message message = new Message("message".getBytes(), MqttQoS.AT_MOST_ONCE, "id1");
        m_messagesStore.storeRetained(new Topic(message.getTopic()), message);

        Subscription subscription1 = new Subscription("cid", new Topic("id1/#"), MqttQoS.AT_MOST_ONCE);

        Subscription subscription2 = new Subscription("cid", new Topic("id1"), MqttQoS.AT_MOST_ONCE);

        await().until(() -> {
            Map<Subscription, Collection<Message>> result = m_messagesStore
                    .searchMatching(Arrays.asList(subscription1, subscription2));

            assertThat(result).containsOnlyKeys(subscription1, subscription2);

            assertThat(result.get(subscription1))
            .containsExactlyInAnyOrder(new Message("message".getBytes(), MqttQoS.AT_MOST_ONCE, "id1"));

            assertThat(result.get(subscription2)).isNotEmpty();
        });

        m_messagesStore.cleanRetained(new Topic("id1"));
        await().until(() -> {
            Map<Subscription, Collection<Message>> result = m_messagesStore
                    .searchMatching(Arrays.asList(subscription1, subscription2));

            assertThat(result).isEmpty();
        });
    }

    @Test
    public void mapToGuid() {
        m_sessionsStore.createNewSession("TestClient", true);
        UUID guid = m_messagesStore.storePublishForFuture(publishToStore);

        // Exercise
        IMessagesStore.StoredMessage storedPublish = m_messagesStore.getMessageByGuid(guid);

        //assertThat(m_messagesStore.getPendingPublishMessages("TestClient")).isEqualTo(1);

        assertThat(storedPublish).isEqualTo(publishToStore);
    }

    @Test
    public void checkSessions() {
        String id = "TestClient2";

        ClientSession s = m_sessionsStore.createNewSession(id, true);

        assertThat(m_sessionsStore.getAllSessions()).doesNotContain(s);

        ClientSession s2 = m_sessionsStore.sessionForClient(id);

        assertThat(m_sessionsStore.getAllSessions()).doesNotContain(s);

        assertThat(s2).isNotEqualTo(s);
        assertThat(s2.clientID).isEqualTo(s.clientID);
        assertThat(s2.isCleanSession()).isEqualTo(s.isCleanSession());
    }

    @Test
    public void checkSubs() {
        String id = "TestClient3";
        Topic t = new Topic("id1/#");

        m_sessionsStore.createNewSession(id, true);

        Subscription subscription1 = new Subscription(id, t, MqttQoS.AT_MOST_ONCE);

        m_sessionsStore.addNewSubscription(subscription1);

        assertThat(m_sessionsStore.contains(id)).isTrue();
        assertThat(m_sessionsStore.getSubscriptions()).containsExactly(subscription1);

        m_sessionsStore.removeSubscription(t, id);

        assertThat(m_sessionsStore.contains(id)).isTrue();
        assertThat(m_sessionsStore.getSubscriptions()).isEmpty();
    }

    @Test
    public void wipeSubs() {
        String id = "TestClient3";
        Topic t = new Topic("id1/#");

        m_sessionsStore.createNewSession(id, true);

        Subscription subscription1 = new Subscription(id, t, MqttQoS.AT_MOST_ONCE);

        m_sessionsStore.addNewSubscription(subscription1);

        assertThat(m_sessionsStore.contains(id)).isTrue();
        assertThat(m_sessionsStore.getSubscriptions()).containsExactly(subscription1);

        m_sessionsStore.wipeSubscriptions(id);

        assertThat(m_sessionsStore.contains(id)).isFalse();
        assertThat(m_sessionsStore.getSubscriptions()).isEmpty();
    }

    @Test
    public void queue() {
        BlockingQueue<StoredMessage> queue = m_sessionsStore.queue("TestClient3");
        queue.add(publishToStore);

        assertThat(m_sessionsStore.queue("TestClient3")).isEqualTo(queue);

        assertThat(m_sessionsStore.queue("TestClient3").peek()).isEqualTo(publishToStore);

        m_sessionsStore.dropQueue("TestClient3");

        assertThat(m_sessionsStore.queue("TestClient3")).isNotEqualTo(queue);
        assertThat(m_sessionsStore.queue("TestClient3")).isEmpty();
    }

    @Test
    public void inFlight() {
        String id = "clientId";

        int messageID = 1;

        assertThat(m_sessionsStore.getInflightMessagesNo(id)).isEqualTo(0);

        UUID guid = m_messagesStore.storePublishForFuture(publishToStore);

        m_sessionsStore.inFlight(id, messageID, guid);

        StoredMessage msg = m_sessionsStore.getInflightMessage(id, messageID);

        assertThat(msg).isEqualTo(publishToStore);

        assertThat(m_sessionsStore.getInflightMessagesNo(id)).isEqualTo(1);

        //Not existing messageID
        assertThat(m_sessionsStore.getInflightMessage(id, -1)).isNull();

        m_sessionsStore.moveInFlightToSecondPhaseAckWaiting(id, messageID);

        assertThat(m_sessionsStore.getInflightMessage(id, messageID)).isNull();
    }

    @Test
    public void inFlightAck() {
        String id = "id10";

        int messageID = m_sessionsStore.nextPacketID(id);

        m_sessionsStore.createNewSession(id, true);

        UUID guid = m_messagesStore.storePublishForFuture(publishToStore);

        m_sessionsStore.inFlight(id, messageID, guid);
        m_sessionsStore.inFlightAck(id, messageID);

        assertThat(m_sessionsStore.getInflightMessage(id, messageID)).isNull();
    }

    @Test
    public void secondPhaseAck() {
        String id = "id10";

        int messageID = m_sessionsStore.nextPacketID(id);

        m_sessionsStore.createNewSession(id, true);

        UUID guid = m_messagesStore.storePublishForFuture(publishToStore);

        m_sessionsStore.inFlight(id, messageID, guid);

        assertThat(m_sessionsStore.getSecondPhaseAckPendingMessages(id)).isEqualTo(0);

        m_sessionsStore.moveInFlightToSecondPhaseAckWaiting(id, messageID);

        assertThat(m_sessionsStore.getInflightMessage(id, messageID)).isNull();
        assertThat(m_sessionsStore.getSecondPhaseAckPendingMessages(id)).isEqualTo(1);

        m_sessionsStore.secondPhaseAcknowledged(id, messageID);
        assertThat(m_sessionsStore.getSecondPhaseAckPendingMessages(id)).isEqualTo(0);

        // Bad data check
        m_sessionsStore.moveInFlightToSecondPhaseAckWaiting("wrong", messageID);
    }
}
