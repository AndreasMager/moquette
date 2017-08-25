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

package io.moquette.persistence;

import io.moquette.spi.ClientSession;
import io.moquette.spi.ISubscriptionsStore.ClientTopicCouple;
import io.moquette.spi.IMessagesStore;
import io.moquette.spi.IMessagesStore.Message;
import io.moquette.spi.IMessagesStore.StoredMessage;
import io.moquette.spi.impl.subscriptions.Subscription;
import io.moquette.spi.impl.subscriptions.Topic;
import io.netty.handler.codec.mqtt.MqttQoS;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import static org.awaitility.Awaitility.await;
import static org.assertj.core.api.Assertions.*;

public abstract class AbstractStoreTest extends MessageStoreTCK {

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
        ClientSession session1 = sessionsStore.createNewSession("SESSION_ID_1", true);

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
        List<ClientTopicCouple> subscriptions = sessionsStore.subscriptionStore().listAllSubscriptions();
        assertThat(subscriptions).hasSize(1);
        Subscription sub = sessionsStore.subscriptionStore().getSubscription(subscriptions.get(0));
        assertThat(sub.getRequestedQos()).isEqualTo(overridingSubscription.getRequestedQos());
    }

    @Test
    public void testNextPacketID_notExistingClientSession() {
        int packetId = sessionsStore.nextPacketID("NOT_EXISTING_CLI");
        assertThat(packetId).isEqualTo(-1);
    }

    @Test
    public void testNextPacketID_existingClientSession() {
        sessionsStore.createNewSession("CLIENT", true);

        // Force creation of inflight map for the CLIENT session
        int packetId = sessionsStore.nextPacketID("CLIENT");
        assertThat(packetId).isEqualTo(1);

        // request a second packetID
        packetId = sessionsStore.nextPacketID("CLIENT");
        assertThat(packetId).isEqualTo(2);
    }

    @Test
    public void testNextPacketID() {
        sessionsStore.createNewSession("CLIENT", true);
        // request a first ID

        IMessagesStore.StoredMessage publishToStore = new IMessagesStore.StoredMessage(
                "Hello".getBytes(),
                MqttQoS.EXACTLY_ONCE,
                "/topic");
        publishToStore.setClientID("CLIENT");
        publishToStore.setRetained(false);

        int packetId = sessionsStore.nextPacketID("CLIENT");
        sessionsStore.inFlight("CLIENT", packetId, publishToStore); //simulate an inflight
        assertThat(packetId).isEqualTo(1);

        // release the ID
        sessionsStore.inFlightAck("CLIENT", packetId);

        // request a second packetID, counter restarts from 0
        packetId = sessionsStore.nextPacketID("CLIENT");
        assertThat(packetId).isEqualTo(1);
    }

    @Test
    public void testDropMessagesInSessionDoesntCleanAnyRetainedStoredMessages() {
        sessionsStore.createNewSession(TEST_CLIENT, true);

        List<Subscription> matcher = Arrays.asList(new Subscription("", new Topic("id1/topic"), MqttQoS.AT_LEAST_ONCE));

        await().untilAsserted(() -> assertThat(messagesStore.searchMatching(matcher))
                .as("The stored must not contain the retained message")
                .isEmpty());

        messagesStore.storeRetained(new Topic(publishToStore.getTopic()), publishToStore);

        await().untilAsserted(() -> assertThat(messagesStore.searchMatching(matcher))
                .as("The stored retained message must be present before client's session drop")
                .isNotEmpty());
    }

    @Test
    public void checkRetained() {
        Message message = new Message("message".getBytes(), MqttQoS.AT_MOST_ONCE, "id1/topic");
        messagesStore.storeRetained(new Topic(message.getTopic()), message);

        Message message2 = new Message("message".getBytes(), MqttQoS.AT_MOST_ONCE, "id1/topic2");
        messagesStore.storeRetained(new Topic(message2.getTopic()), message2);

        Message message3 = new Message("message".getBytes(), MqttQoS.AT_MOST_ONCE, "id2/topic2");
        messagesStore.storeRetained(new Topic(message3.getTopic()), message3);

        Subscription subscription1 = new Subscription("cid", new Topic("id1/#"), MqttQoS.AT_MOST_ONCE);

        Subscription subscription2 = new Subscription("cid", new Topic("id1/topic"), MqttQoS.AT_MOST_ONCE);

        await().untilAsserted(() -> {
            Map<Subscription, Collection<Message>> result = messagesStore
                    .searchMatching(Arrays.asList(subscription1, subscription2));

            assertThat(result).containsOnlyKeys(subscription1, subscription2);

            assertThat(result.get(subscription1)).containsExactlyInAnyOrder(message, message2);
            assertThat(result.get(subscription2)).containsExactlyInAnyOrder(message);
        });

        messagesStore.cleanRetained(new Topic("id1/topic2"));

        await().untilAsserted(() -> {
            Map<Subscription, Collection<Message>> result = messagesStore
                    .searchMatching(Arrays.asList(subscription1, subscription2));

            assertThat(result).containsOnlyKeys(subscription1, subscription2);

            assertThat(result.get(subscription1)).containsExactlyInAnyOrder(message);
            assertThat(result.get(subscription2)).containsExactlyInAnyOrder(message);
        });

        messagesStore.cleanRetained(new Topic("id1/topic"));

        await().untilAsserted(() -> {
            Map<Subscription, Collection<Message>> result = messagesStore
                    .searchMatching(Arrays.asList(subscription1, subscription2));

            assertThat(result).isEmpty();
        });
    }

    @Test
    public void singleTopicRetained() {
        // This message will be stored in a wrong place id1/
        Message message = new Message("message".getBytes(), MqttQoS.AT_MOST_ONCE, "id1");
        messagesStore.storeRetained(new Topic(message.getTopic()), message);

        Subscription subscription1 = new Subscription("cid", new Topic("id1/#"), MqttQoS.AT_MOST_ONCE);

        Subscription subscription2 = new Subscription("cid", new Topic("id1"), MqttQoS.AT_MOST_ONCE);

        await().untilAsserted(() -> {
            Map<Subscription, Collection<Message>> result = messagesStore
                    .searchMatching(Arrays.asList(subscription1, subscription2));

            assertThat(result).containsOnlyKeys(subscription1, subscription2);

            assertThat(result.get(subscription1))
            .containsExactlyInAnyOrder(new Message("message".getBytes(), MqttQoS.AT_MOST_ONCE, "id1"));

            assertThat(result.get(subscription2)).isNotEmpty();
        });

        messagesStore.cleanRetained(new Topic("id1"));
        await().untilAsserted(() -> {
            Map<Subscription, Collection<Message>> result = messagesStore
                    .searchMatching(Arrays.asList(subscription1, subscription2));

            assertThat(result).isEmpty();
        });
    }

    @Test
    public void checkSessions() {
        String id = "TestClient2";

        ClientSession s = sessionsStore.createNewSession(id, true);

        assertThat(sessionsStore.getAllSessions()).doesNotContain(s);

        ClientSession s2 = sessionsStore.sessionForClient(id);

        assertThat(sessionsStore.getAllSessions()).doesNotContain(s);

        assertThat(s2).isNotEqualTo(s);
        assertThat(s2.clientID).isEqualTo(s.clientID);
        assertThat(s2.isCleanSession()).isEqualTo(s.isCleanSession());
    }

    @Test
    public void checkSubs() {
        String id = "TestClient3";
        Topic t = new Topic("id1/#");

        sessionsStore.createNewSession(id, true);

        Subscription subscription1 = new Subscription(id, t, MqttQoS.AT_MOST_ONCE);

        sessionsStore.subscriptionStore().addNewSubscription(subscription1);

        assertThat(sessionsStore.contains(id)).isTrue();
        assertThat(sessionsStore.subscriptionStore().getSubscriptions()).containsExactly(subscription1);

        sessionsStore.subscriptionStore().removeSubscription(t, id);

        assertThat(sessionsStore.contains(id)).isTrue();
        assertThat(sessionsStore.subscriptionStore().getSubscriptions()).isEmpty();
    }

    @Test
    public void wipeSubs() {
        String id = "TestClient33";
        Topic t = new Topic("id1/#");

        sessionsStore.createNewSession(id, true);

        Subscription subscription1 = new Subscription(id, t, MqttQoS.AT_MOST_ONCE);

        sessionsStore.subscriptionStore().addNewSubscription(subscription1);

        assertThat(sessionsStore.contains(id)).isTrue();
        assertThat(sessionsStore.subscriptionStore().getSubscriptions()).containsExactly(subscription1);

        sessionsStore.subscriptionStore().wipeSubscriptions(id);

        assertThat(sessionsStore.contains(id)).isTrue();
        assertThat(sessionsStore.subscriptionStore().getSubscriptions()).isEmpty();
    }

    @Test
    public void queue() {
        String id = "TestClient3";

        sessionsStore.createNewSession(id, true);

        Queue<StoredMessage> queue = sessionsStore.queue(id);
        queue.add(publishToStore);

        assertThat(sessionsStore.queue(id)).isEqualTo(queue);

        assertThat(sessionsStore.queue(id).peek()).isEqualTo(publishToStore);

        sessionsStore.dropQueue(id);

        assertThat(sessionsStore.queue(id)).isEmpty();
    }

    @Ignore
    @Test
    public void inFlight() {
        String id = "clientId";

        sessionsStore.createNewSession(id, true);

        IMessagesStore.StoredMessage publishToStore = new IMessagesStore.StoredMessage(
                "Hello".getBytes(),
                MqttQoS.EXACTLY_ONCE,
                "/topic");
        publishToStore.setClientID(id);
        publishToStore.setRetained(false);

        int messageID = 1;

        assertThat(sessionsStore.getInflightMessagesNo(id)).isEqualTo(0);

        sessionsStore.inFlight(id, messageID, publishToStore);

        StoredMessage msg = sessionsStore.inboundInflight(id, messageID);

        assertThat(msg).isEqualTo(publishToStore);

        // TODO make no sense
        assertThat(sessionsStore.getInflightMessagesNo(id)).isEqualTo(1);

        //Not existing messageID
        assertThat(sessionsStore.inboundInflight(id, -1)).isNull();

        sessionsStore.moveInFlightToSecondPhaseAckWaiting(id, messageID, msg);

        assertThat(sessionsStore.inboundInflight(id, messageID)).isNull();
    }

    @Test
    public void inFlightAck() {
        String id = "id10";

        IMessagesStore.StoredMessage publishToStore = new IMessagesStore.StoredMessage(
                "Hello".getBytes(),
                MqttQoS.EXACTLY_ONCE,
                "/topic");
        publishToStore.setClientID(id);
        publishToStore.setRetained(false);

        int messageID = sessionsStore.nextPacketID(id);

        sessionsStore.createNewSession(id, true);

        sessionsStore.inFlight(id, messageID, publishToStore);
        sessionsStore.inFlightAck(id, messageID);

        assertThat(sessionsStore.inboundInflight(id, messageID)).isNull();
    }

    @Test
    public void secondPhaseAck() {
        String id = "id10";

        IMessagesStore.StoredMessage publishToStore = new IMessagesStore.StoredMessage(
                "Hello".getBytes(),
                MqttQoS.EXACTLY_ONCE,
                "/topic");
        publishToStore.setClientID(id);
        publishToStore.setRetained(false);

        int messageID = sessionsStore.nextPacketID(id);

        sessionsStore.createNewSession(id, true);

        sessionsStore.inFlight(id, messageID, publishToStore);

        assertThat(sessionsStore.getSecondPhaseAckPendingMessages(id)).isEqualTo(0);

        sessionsStore.moveInFlightToSecondPhaseAckWaiting(id, messageID, publishToStore);

        assertThat(sessionsStore.inboundInflight(id, messageID)).isNull();
        assertThat(sessionsStore.getSecondPhaseAckPendingMessages(id)).isEqualTo(1);

        sessionsStore.secondPhaseAcknowledged(id, messageID);
        assertThat(sessionsStore.getSecondPhaseAckPendingMessages(id)).isEqualTo(0);

        // Bad data check
        sessionsStore.moveInFlightToSecondPhaseAckWaiting("wrong", messageID, publishToStore);
    }
}
