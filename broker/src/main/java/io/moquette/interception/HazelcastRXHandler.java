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

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.ITopic;
import io.moquette.interception.messages.InterceptPublishMessage;
import io.moquette.server.Server;
import io.netty.buffer.ByteBuf;
import io.reactivex.schedulers.Schedulers;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import static io.moquette.spi.impl.Utils.readBytesAndRewind;

public class HazelcastRXHandler {

    private static final Logger LOG = LoggerFactory.getLogger(HazelcastRXHandler.class);

    public HazelcastRXHandler(Server server) {
        HazelcastInstance hz = server.getHazelcastInstance();

        server.getProcessor().getBus().getEvents()
            .filter(msg -> msg instanceof InterceptPublishMessage)
            .cast(InterceptPublishMessage.class)
            .observeOn(Schedulers.single()) // Don't pause netty eventloop thread
            .subscribe(msg ->  onPublish(hz, msg));
    }

    static void onPublish(HazelcastInstance hz, InterceptPublishMessage msg) {
        // TODO ugly, too much array copy
        ByteBuf payload = msg.getPayload();
        byte[] payloadContent = readBytesAndRewind(payload);

        LOG.info("{} publish on {} message: {}", msg.getClientID(), msg.getTopicName(), new String(payloadContent));
        ITopic<HazelcastMsg> topic = hz.getTopic("moquette");
        HazelcastMsg hazelcastMsg = new HazelcastMsg(msg);
        topic.publish(hazelcastMsg);
    }
}