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

package io.moquette.spi.impl;

import io.moquette.server.ConnectionDescriptorStore;
import io.moquette.spi.ClientSession;
import io.moquette.spi.IMessagesStore.StoredMessage;
import io.netty.handler.codec.mqtt.MqttFixedHeader;
import io.netty.handler.codec.mqtt.MqttMessageType;
import io.netty.handler.codec.mqtt.MqttPublishMessage;
import io.netty.handler.codec.mqtt.MqttPublishVariableHeader;
import io.netty.handler.codec.mqtt.MqttQoS;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import static io.netty.handler.codec.mqtt.MqttQoS.AT_MOST_ONCE;

class PersistentQueueMessageSender {

    private static final Logger LOG = LoggerFactory.getLogger(PersistentQueueMessageSender.class);
    private final ConnectionDescriptorStore connectionDescriptorStore;

    PersistentQueueMessageSender(ConnectionDescriptorStore connectionDescriptorStore) {
        this.connectionDescriptorStore = connectionDescriptorStore;
    }

    public void sendPublish(ClientSession session, StoredMessage msg, MqttQoS qos, boolean retained) {
        String clientId = session.clientID;
        String topic = msg.getTopic();

        int messageId;
        if (qos != MqttQoS.AT_MOST_ONCE) {
            LOG.debug("Adding message to inflight zone. ClientId={}, topic={}", clientId,
                msg.getTopic());
            // set the PacketIdentifier only for QoS > 0
            messageId = session.inFlightAckWaiting(msg.getGuid());
        } else {
            messageId = 0;
        }

        MqttPublishVariableHeader varHeader = new MqttPublishVariableHeader(topic, messageId);

        MqttFixedHeader fixedHeader = new MqttFixedHeader(MqttMessageType.PUBLISH, false, qos, retained, 0);
        MqttPublishMessage pubMessage = new MqttPublishMessage(fixedHeader, varHeader, msg.getMessage());

        if (LOG.isDebugEnabled()) {
            LOG.debug("Sending PUBLISH message. MessageId={}, CId={}, topic={}, qos={}, payload={}", messageId,
                clientId, topic, new String(msg.getPayload().array()));
        } else {
            LOG.info("Sending PUBLISH message. MessageId={}, CId={}, topic={}", messageId, clientId, topic);
        }

        // TODO Use ChannelFuture
        boolean messageDelivered = connectionDescriptorStore.sendMessage(pubMessage, messageId, clientId);

        if (!messageDelivered && qos != AT_MOST_ONCE && !session.isCleanSession()) {
            LOG.warn("PUBLISH message could not be delivered. It will be stored. MessageId={}, CId={}, topic={}, "
                    + "qos={}, cleanSession={}", messageId, clientId, topic, qos, false);
            session.enqueue(msg);
        } else {
            LOG.warn("PUBLISH message could not be delivered. It will be discarded. MessageId={}, CId={}, topic={}, " +
                "qos={}, cleanSession={}", messageId, clientId, topic, qos, true);
        }
    }
}
