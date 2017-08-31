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

package io.moquette.server;

import io.moquette.interception.AbstractInterceptHandler;
import io.moquette.interception.messages.InterceptConnectMessage;
import io.moquette.interception.messages.InterceptConnectionLostMessage;
import io.moquette.interception.messages.InterceptDisconnectMessage;
import io.moquette.interception.messages.InterceptPublishMessage;
import io.moquette.server.config.IConfig;
import io.moquette.server.config.MemoryConfig;
import io.reactivex.Observable;
import io.reactivex.functions.Consumer;
import org.fusesource.mqtt.client.*;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.IOException;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

public class ServerIntegrationFuseTest {

    private static final Logger LOG = LoggerFactory.getLogger(ServerIntegrationPahoTest.class);

    Server m_server;
    MQTT m_mqtt;
    BlockingConnection m_subscriber;
    BlockingConnection m_publisher;
    IConfig m_config;

    protected void startServer() throws IOException {
        m_server = new Server();
        final Properties configProps = IntegrationUtils.prepareTestProperties();
        m_config = new MemoryConfig(configProps);
        m_server.startServer(m_config);
    }

    @Before
    public void setUp() throws Exception {
        startServer();

        m_mqtt = new MQTT();
        m_mqtt.setHost("localhost", 1883);
    }

    @After
    public void tearDown() throws Exception {
        if (m_subscriber != null) {
            m_subscriber.disconnect();
        }

        if (m_publisher != null) {
            m_publisher.disconnect();
        }

        m_server.stopServer();
    }

    public <T> Observable<T> observe(Class<T> clazz) {
        return m_server.getProcessor().getBus()
            .getEvents()
            .filter(clazz::isInstance)
            .cast(clazz);
    }

    @SuppressWarnings("CheckReturnValue")
    public <T> Consumer<T> subscribe(Class<T> clazz) {
        @SuppressWarnings("unchecked")
        Consumer<T> foo = mock(Consumer.class);
        observe(clazz).subscribe(foo);
        return foo;
    }

    @Test
    public void checkWillTestamentIsPublishedOnConnectionKill_noRetain() throws Exception {
        LOG.info("checkWillTestamentIsPublishedOnConnectionKill_noRetain");

        Consumer<InterceptConnectMessage> connect = subscribe(InterceptConnectMessage.class);
        Consumer<InterceptPublishMessage> publish = subscribe(InterceptPublishMessage.class);
        Consumer<InterceptConnectionLostMessage> connectionLost = subscribe(InterceptConnectionLostMessage.class);
        Consumer<InterceptDisconnectMessage> disconnect = subscribe(InterceptDisconnectMessage.class);

        String willTestamentTopic = "/will/test";
        String willTestamentMsg = "Bye bye";

        MQTT mqtt = new MQTT();
        mqtt.setHost("localhost", 1883);
        mqtt.setClientId("WillTestamentPublisher");
        mqtt.setWillRetain(false);
        mqtt.setWillMessage(willTestamentMsg);
        mqtt.setWillTopic(willTestamentTopic);
        m_publisher = mqtt.blockingConnection();
        m_publisher.connect();

        m_mqtt.setHost("localhost", 1883);
        m_mqtt.setCleanSession(false);
        m_mqtt.setClientId("Subscriber");
        m_subscriber = m_mqtt.blockingConnection();
        m_subscriber.connect();
        Topic[] topics = new Topic[]{new Topic(willTestamentTopic, QoS.AT_MOST_ONCE)};
        m_subscriber.subscribe(topics);

        // Exercise, kill the publisher connection
        m_publisher.kill();

        // Verify, that the testament is fired
        Message msg = m_subscriber.receive(500, TimeUnit.MILLISECONDS);
        assertNotNull("We should get notified with 'Will' message", msg);
        msg.ack();
        assertEquals(willTestamentMsg, new String(msg.getPayload()));

        verify(connect, times(2)).accept(any());
        verify(publish).accept(any());
        verify(connectionLost).accept(any());
        verify(disconnect, times(0)).accept(any());
    }

    class TestInterceptHandler extends AbstractInterceptHandler {

        TestInterceptHandler() {
            super(m_server);
        }

        @Override
        public String getID() {
            return "test";
        }
    }
}
