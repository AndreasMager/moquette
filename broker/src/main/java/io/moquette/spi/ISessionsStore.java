/*
 * Copyright (c) 2012-2017 The original author or authors
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

package io.moquette.spi;

import io.moquette.spi.IMessagesStore.Message;
import io.moquette.spi.IMessagesStore.StoredMessage;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * Store used to handle the persistence of the subscriptions tree.
 */
public interface ISessionsStore {

    void initStore();

    ISubscriptionsStore subscriptionStore();

    void updateCleanStatus(String clientID, boolean cleanSession);

    /**
     * @param clientID
     *            the session client ID.
     * @return true iff there are subscriptions persisted with clientID
     */
    boolean contains(String clientID);

    ClientSession createNewSession(String clientID, boolean cleanSession, long now);

    /**
     * @param clientID
     *            the client owning the session.
     * @return the session for the given clientID, null if not found.
     */
    ClientSession sessionForClient(String clientID);

    Message inFlightAck(String clientID, int messageID);

    /**
     * Save the message msg with  messageID, clientID as in flight
     *
     * @param clientID
     *            the client ID
     * @param messageID
     *            the message ID
     * @param msg
     *            the message to put in flight zone
     */
    void inFlight(String clientID, int messageID, Message msg);

    /**
     * Return the next valid packetIdentifier for the given client session.
     *
     * @param clientID
     *            the clientID requesting next packet id.
     * @return the next valid id.
     */
    int nextPacketID(String clientID);

    /**
     * List the published retained messages for the session
     *
     * @param clientID
     *            the client ID owning the queue.
     * @return the queue of messages.
     */
    Queue<StoredMessage> queue(String clientID);

    void dropQueue(String clientID);

    void moveInFlightToSecondPhaseAckWaiting(String clientID, int messageID, Message msg);

    /**
     * @param clientID
     *            the client ID accessing the second phase.
     * @param messageID
     *            the message ID that reached the second phase.
     * @return the guid of message just acked.
     */
    Message secondPhaseAcknowledged(String clientID, int messageID);

    /**
     * Returns the number of inflight messages for the given client ID
     *
     * @param clientID target client.
     * @return count of pending in flight publish messages.
     */
    int getInflightMessagesNo(String clientID);

    /**
     * @return the inflight inbound (PUBREL for Qos2) message.
     * */
    IMessagesStore.StoredMessage inboundInflight(String clientID, int messageID);

    void markAsInboundInflight(String clientID, int messageID, StoredMessage msg);

    /**
     * Returns the size of the session queue for the given client ID
     *
     * @param clientID target client.
     * @return count of enqueued publish messages.
     */
    int getPendingPublishMessagesNo(String clientID);

    /**
     * Returns the number of second-phase ACK pending messages for the given client ID
     *
     * @param clientID target client.
     * @return count of pending in flight publish messages.
     */
    int getSecondPhaseAckPendingMessages(String clientID);

    void cleanSession(String clientID);

    Set<String> getClientIDs();

    void remove(String clientId);

    void updateValidity(String clientID, long now);

    default int size() {
        return getClientIDs().size();
    }

    default void remove(Iterable<String> clientIDs) {
        clientIDs
            .forEach(this::remove);
    }

    /**
     * Returns all the sessions
     *
     * @return the collection of all stored client sessions.
     */
    default Set<ClientSession> getAllSessions() {
        return getClientIDs()
            .stream()
            .map(this::sessionForClient)
            .collect(Collectors.toSet());
    }

    default Set<String> getExpired(long now, long ttl, TimeUnit unit) {
        return getAllSessions()
                .stream()
                .filter(session -> now > session.getLastContact() + unit.toMillis(ttl))
                .map(ClientSession::getClientID)
                .collect(Collectors.toSet());
    }

    default Set<String> getOldest(int count) {
        return getAllSessions()
            .stream()
            .sorted((session, session2) -> Long.compare(session.getLastContact(), session2.getLastContact()))
            .limit(count)
            .map(ClientSession::getClientID)
            .collect(Collectors.toSet());
    }
}
