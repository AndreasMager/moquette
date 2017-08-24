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

package io.moquette.interception;

import static org.assertj.core.api.Assertions.*;

import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.Test;
import io.moquette.interception.messages.InterceptPublishMessage;
import io.moquette.spi.impl.subscriptions.Topic;
import io.netty.buffer.Unpooled;
import io.netty.handler.codec.mqtt.MqttMessageBuilders;
import io.netty.handler.codec.mqtt.MqttPublishMessage;
import io.netty.handler.codec.mqtt.MqttQoS;

public class RxBusTest {

    @SuppressWarnings("CheckReturnValue")
    @Test
    public void test() {
        AtomicBoolean testRun = new AtomicBoolean(false);

        RxBus bus = new RxBus();

        bus.getEvents()
            .filter(msg -> msg instanceof InterceptPublishMessage)
            .cast(InterceptPublishMessage.class)
            .subscribe(msg -> {
                assertThat(msg.getUsername()).isEqualTo("username");
                assertThat(msg.getTopic().toString()).isEqualTo("topic");
                testRun.set(true);
            });

        MqttPublishMessage msg = MqttMessageBuilders.publish().topicName("topic").qos(MqttQoS.AT_LEAST_ONCE)
                .payload(Unpooled.EMPTY_BUFFER).build();
        bus.publish(new InterceptPublishMessage(msg, "clientID", "username", new Topic("topic")));

        assertThat(testRun.get()).isTrue();
    }
}
