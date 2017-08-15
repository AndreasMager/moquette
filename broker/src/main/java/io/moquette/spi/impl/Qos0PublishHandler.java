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

import io.moquette.interception.RxBus;
import io.moquette.interception.messages.InterceptPublishMessage;
import io.moquette.server.netty.NettyUtils;
import io.moquette.spi.IMessagesStore;
import io.moquette.spi.impl.subscriptions.Topic;
import io.moquette.spi.security.IAuthorizator;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.handler.codec.mqtt.MqttMessageBuilders;
import io.netty.handler.codec.mqtt.MqttPublishMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static io.moquette.spi.impl.ProtocolProcessor.asStoredMessage;
import static io.moquette.spi.impl.Utils.readBytesAndRewind;

class Qos0PublishHandler extends QosPublishHandler {

    private static final Logger LOG = LoggerFactory.getLogger(Qos0PublishHandler.class);

    private final IMessagesStore m_messagesStore;
    private final MessagesPublisher publisher;

    public Qos0PublishHandler(IAuthorizator authorizator, IMessagesStore messagesStore,
            MessagesPublisher messagesPublisher, RxBus bus) {
        super(authorizator, bus);
        this.m_messagesStore = messagesStore;
        this.publisher = messagesPublisher;
    }

    void receivedPublishQos0(Channel channel, MqttPublishMessage msg) {
        // verify if topic can be write
        final Topic topic = new Topic(msg.variableHeader().topicName());
        String clientID = NettyUtils.clientID(channel);
        String username = NettyUtils.userName(channel);
        if (!m_authorizator.canWrite(topic, username, clientID)) {
            LOG.error("MQTT client is not authorized to publish on topic. CId={}, topic={}", clientID, topic);
            return;
        }

        // route message to subscribers
        IMessagesStore.StoredMessage toStoreMsg = asStoredMessage(msg);
        toStoreMsg.setClientID(clientID);

        this.publisher.publish2Subscribers(toStoreMsg, topic);

        try {
            byte[] payload = readBytesAndRewind(msg.payload());

            MqttPublishMessage clone = MqttMessageBuilders.publish().qos(msg.fixedHeader().qosLevel())
                    .payload(Unpooled.wrappedBuffer(payload)).retained(msg.fixedHeader().isRetain())
                    .topicName(topic.toString()).build();

            InterceptPublishMessage im = new InterceptPublishMessage(clone, clientID, username);

            bus.publish(im);
        } catch (Throwable t) {
            LOG.error(t.toString(), t);
        }

        if (msg.fixedHeader().isRetain()) {
            if (!msg.payload().isReadable()) {
                m_messagesStore.cleanRetained(topic);
            } else {
                // before wasn't stored
                //MessageGUID guid = m_messagesStore.storePublishForFuture(toStoreMsg);
                m_messagesStore.storeRetained(topic, toStoreMsg);
            }
        }
    }
}
