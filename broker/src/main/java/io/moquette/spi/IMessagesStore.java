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

import io.moquette.spi.impl.subscriptions.Topic;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.handler.codec.mqtt.MqttQoS;
import java.io.Serializable;
import io.moquette.spi.impl.subscriptions.Subscription;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * Defines the SPI to be implemented by a StorageService that handle persistence of messages
 */
public interface IMessagesStore {

    class Message implements Serializable {

        private static final long serialVersionUID = 1755296138639817304L;
        final MqttQoS m_qos;
        final byte[] m_payload;
        final String m_topic;
        private boolean m_retained;

        public Message(byte[] message, MqttQoS qos, String topic) {
            m_qos = qos;
            m_payload = message;
            m_topic = topic;
        }

        public MqttQoS getQos() {
            return m_qos;
        }

        public byte[] getPayloadBytes() {
            return m_payload;
        }

        public ByteBuf getPayload() {
            return Unpooled.copiedBuffer(m_payload);
        }

        public String getTopic() {
            return m_topic;
        }

        public void setRetained(boolean retained) {
            this.m_retained = retained;
        }

        public boolean isRetained() {
            return m_retained;
        }

        @Override
        public String toString() {
            return "PublishEvent{" +
                    ", m_qos=" + m_qos +
                    ", m_topic='" + m_topic + '\'' +
                    '}';
        }

        @Override
        public int hashCode() {
            final int prime = 31;
            int result = 1;
            result = prime * result + Arrays.hashCode(m_payload);
            result = prime * result + ((m_qos == null) ? 0 : m_qos.hashCode());
            result = prime * result + ((m_topic == null) ? 0 : m_topic.hashCode());
            result = prime * result + (m_retained ? 0 : 1);
            return result;
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj)
                return true;
            if (obj == null)
                return false;
            if (getClass() != obj.getClass())
                return false;
            Message other = (Message) obj;
            if (!Arrays.equals(m_payload, other.m_payload))
                return false;
            if (m_qos != other.m_qos)
                return false;
            if (m_retained != other.m_retained)
                return false;
            if (m_topic == null) {
                if (other.m_topic != null)
                    return false;
            } else if (!m_topic.equals(other.m_topic))
                return false;
            return true;
        }
    }

    class StoredMessage extends Message implements Serializable {

        private static final long serialVersionUID = 1755296138639817304L;

        private String m_clientID;
        private UUID m_guid;

        private boolean m_retained;

        public StoredMessage(byte[] message, MqttQoS qos, String topic) {
            super(message, qos, topic);
        }

        public void setGuid(UUID guid) {
            this.m_guid = guid;
        }

        public UUID getGuid() {
            return m_guid;
        }

        public String getClientID() {
            return m_clientID;
        }

        public void setClientID(String m_clientID) {
            this.m_clientID = m_clientID;
        }

        @Override
        public String toString() {
            return "PublishEvent{clientID='" + m_clientID + '\'' + ", m_retain="
                    + m_retained + ", m_qos=" + m_qos + ", m_topic='" + m_topic + '\'' + '}';
        }
    }

    /**
     * Used to initialize all persistent store structures
     */
    void initStore();

    /**
     * Return a list of retained messages that satisfy the condition.
     *
     * @param condition
     *            the condition to match during the search.
     * @return the collection of matching messages.
     */
    Map<Subscription, Collection<Message>> searchMatching(List<Subscription> newSubscriptions);

    void cleanRetained(Topic topic);

    void storeRetained(Topic topic, Message storedMessage);
}
