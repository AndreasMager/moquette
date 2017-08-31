package io.moquette.persistence;

import io.moquette.spi.*;
import io.moquette.spi.IMessagesStore.Message;
import io.moquette.spi.IMessagesStore.StoredMessage;
import io.moquette.spi.ISubscriptionsStore.ClientTopicCouple;
import io.moquette.spi.impl.subscriptions.Subscription;
import io.moquette.spi.impl.subscriptions.Topic;
import io.netty.buffer.ByteBuf;
import io.netty.handler.codec.mqtt.MqttQoS;
import org.junit.Test;
import com.google.common.collect.Sets;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import static io.moquette.spi.impl.subscriptions.Topic.asTopic;
import static io.netty.handler.codec.mqtt.MqttQoS.AT_MOST_ONCE;
import static io.netty.handler.codec.mqtt.MqttQoS.EXACTLY_ONCE;
import static org.junit.Assert.assertEquals;
import static org.assertj.core.api.Assertions.*;

/**
 * Defines all test that an implementation of a IMessageStore should satisfy.
 * */
public abstract class MessageStoreTCK {

    public static final String TEST_CLIENT = "TestClient";
    protected ISessionsStore sessionsStore;
    protected IMessagesStore messagesStore;

    @Test
    public void testDropMessagesInSessionDoesntCleanAnyRetainedStoredMessages() {
        final ClientSession session = sessionsStore.createNewSession(TEST_CLIENT, true, 0);
        StoredMessage publishToStore = new StoredMessage("Hello".getBytes(), EXACTLY_ONCE, "/topic");
        publishToStore.setClientID(TEST_CLIENT);
        publishToStore.setRetained(true);
        messagesStore.storeRetained(new Topic("/topic"), publishToStore);

        // Exercise
        session.cleanSession();

        Subscription sub = new Subscription("clientId", new Topic("/topic"), MqttQoS.AT_LEAST_ONCE);

        // Verify the message store for session is empty.
        Map<Subscription, Collection<Message>> storedPublish = messagesStore.searchMatching(Arrays.asList(sub));
        assertEquals("The stored retained message must be present after client's session drop",
                storedPublish.get(sub).isEmpty(), false);
    }

    @Test
    public void testStoreRetained() {
        StoredMessage msgStored = new StoredMessage("Hello".getBytes(), MqttQoS.AT_LEAST_ONCE, "/topic");
        msgStored.setClientID(TEST_CLIENT);

        messagesStore.storeRetained(asTopic("/topic"), msgStored);

        Subscription sub = new Subscription("clientId", new Topic("/topic"), MqttQoS.AT_LEAST_ONCE);

        //Verify the message is in the store
        Message msgRetrieved = messagesStore.searchMatching(Arrays.asList(sub)).get(sub).iterator().next();

        final ByteBuf payload = msgRetrieved.getPayload();
        byte[] content = new byte[payload.readableBytes()];
        payload.readBytes(content);
        assertEquals("Hello", new String(content));
    }

    @Test
    public void givenSubscriptionAlreadyStoredIsOverwrittenByAnotherWithSameTopic() {
        ClientSession session1 = sessionsStore.createNewSession("SESSION_ID_1", true, 0);

        // Subscribe on /topic with QOSType.MOST_ONE
        Subscription oldSubscription = new Subscription(session1.clientID, new Topic("/topic"), AT_MOST_ONCE);
        session1.subscribe(oldSubscription);

        // Subscribe on /topic again that overrides the previous subscription.
        Subscription overridingSubscription = new Subscription(session1.clientID, new Topic("/topic"), EXACTLY_ONCE);
        session1.subscribe(overridingSubscription);

        // Verify
        final ISubscriptionsStore subscriptionsStore = sessionsStore.subscriptionStore();
        List<ClientTopicCouple> subscriptions = subscriptionsStore.listAllSubscriptions();
        assertEquals(1, subscriptions.size());
        Subscription sub = subscriptionsStore.getSubscription(subscriptions.get(0));
        assertEquals(overridingSubscription.getRequestedQos(), sub.getRequestedQos());
    }

    @Test
    public void removeSession() {
        String clientID = "SESSION_ID_1";
        sessionsStore.createNewSession(clientID, true, 0);

        assertThat(sessionsStore.contains(clientID)).isTrue();

        sessionsStore.remove(clientID);

        assertThat(sessionsStore.contains(clientID)).isFalse();

        sessionsStore.remove("unknown"); // this should not crash
    }

    @Test
    public void removeSessionS() {
        String clientID = "SESSION_ID_1";
        sessionsStore.createNewSession(clientID, true, 0);

        String clientID2 = "SESSION_ID_2";
        sessionsStore.createNewSession(clientID2, true, 0);

        assertThat(sessionsStore.getAllSessions())
            .extracting(ClientSession::getClientID).containsOnly(clientID, clientID2);

        sessionsStore.remove(Sets.newHashSet(clientID, clientID2));

        assertThat(sessionsStore.getAllSessions()).isEmpty();
    }

    @Test
    public void getClientIDs() {
        String clientID = "SESSION_ID_1";
        sessionsStore.createNewSession(clientID, true, 0);

        String clientID2 = "SESSION_ID_2";
        sessionsStore.createNewSession(clientID2, true, 0);

        assertThat(sessionsStore.getClientIDs()).containsExactlyInAnyOrder(clientID, clientID2);
    }

    @Test
    public void getSessions() {
        String clientID = "SESSION_ID_1";
        sessionsStore.createNewSession(clientID, true, 0);

        String clientID2 = "SESSION_ID_2";
        sessionsStore.createNewSession(clientID2, false, 0);

        Set<ClientSession> result = sessionsStore.getAllSessions();

        assertThat(result).extracting(ClientSession::getClientID).containsOnly(clientID, clientID2);
    }

    @Test
    public void size() {
        assertThat(sessionsStore.size()).isEqualTo(0);

        String clientID = "SESSION_ID_1";
        sessionsStore.createNewSession(clientID, true, 0);

        assertThat(sessionsStore.size()).isEqualTo(1);

        String clientID2 = "SESSION_ID_2";
        sessionsStore.createNewSession(clientID2, false, 0);

        assertThat(sessionsStore.size()).isEqualTo(2);

        sessionsStore.remove(clientID);
        assertThat(sessionsStore.size()).isEqualTo(1);

        sessionsStore.remove(clientID2);
        assertThat(sessionsStore.size()).isEqualTo(0);
    }

    @Test
    public void getExpired() {
        assertThat(sessionsStore.getExpired(0, 0, TimeUnit.MILLISECONDS)).isEmpty();

        String clientID = "SESSION_ID_1";
        sessionsStore.createNewSession(clientID, true, 0);
        assertThat(sessionsStore.getExpired(3, 4, TimeUnit.MILLISECONDS)).isEmpty();

        assertThat(sessionsStore.getExpired(4, 4, TimeUnit.MILLISECONDS)).isEmpty();

        assertThat(sessionsStore.getExpired(5, 4, TimeUnit.MILLISECONDS)).containsOnly(clientID);

        String clientID2 = "SESSION_ID_2";
        sessionsStore.createNewSession(clientID2, true, 1);
        assertThat(sessionsStore.getExpired(3, 4, TimeUnit.MILLISECONDS)).isEmpty();

        assertThat(sessionsStore.getExpired(4, 4, TimeUnit.MILLISECONDS)).isEmpty();

        assertThat(sessionsStore.getExpired(5, 4, TimeUnit.MILLISECONDS)).containsOnly(clientID);

        assertThat(sessionsStore.getExpired(6, 4, TimeUnit.MILLISECONDS)).containsOnly(clientID, clientID2);
    }

    @Test
    public void getOldest() {
        assertThat(sessionsStore.getOldest(0)).isEmpty();
        assertThat(sessionsStore.getOldest(1)).isEmpty();

        String clientID = "SESSION_ID_1";
        sessionsStore.createNewSession(clientID, true, 0);

        assertThat(sessionsStore.getOldest(0)).isEmpty();
        assertThat(sessionsStore.getOldest(1)).containsOnly(clientID);

        String clientID2 = "SESSION_ID_2";
        sessionsStore.createNewSession(clientID2, true, 1);

        assertThat(sessionsStore.getOldest(0)).isEmpty();
        assertThat(sessionsStore.getOldest(1)).containsOnly(clientID);
        assertThat(sessionsStore.getOldest(2)).containsOnly(clientID, clientID2);
    }

    @Test
    public void updateValidity() {
        String clientID = "SESSION_ID_1";
        sessionsStore.createNewSession(clientID, true, 0);
        String clientID2 = "SESSION_ID_2";
        sessionsStore.createNewSession(clientID2, true, 1);

        assertThat(sessionsStore.getOldest(1)).containsOnly(clientID);

        sessionsStore.updateValidity(clientID, 2);

        assertThat(sessionsStore.getOldest(1)).containsOnly(clientID2);

        sessionsStore.updateValidity("unknown", 2); // should not crash
    }
}
