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
import io.moquette.BrokerConstants;
import io.moquette.interception.messages.InterceptPublishMessage;
import io.moquette.server.Server;
import io.reactivex.disposables.Disposable;
import io.reactivex.schedulers.Schedulers;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HazelcastRXHandler {

    private static final Logger LOG = LoggerFactory.getLogger(HazelcastRXHandler.class);

    private final ITopic<HazelcastMsg> topic;

    private final Disposable sub;

    public HazelcastRXHandler(Server server) {
        HazelcastInstance hz = server.getHazelcastInstance();

        String topicName = server.getConfig().getProperty(BrokerConstants.HAZELCAST_TOPIC_NAME) == null
                ? "moquette": server.getConfig().getProperty(BrokerConstants.HAZELCAST_TOPIC_NAME);
        topic = hz.getTopic(topicName);

        sub = server.getProcessor().getBus().getEvents()
            .filter(msg -> msg instanceof InterceptPublishMessage)
            .cast(InterceptPublishMessage.class)
            .observeOn(Schedulers.single()) // Don't pause netty eventloop thread
            .subscribe(msg -> onPublish(topic, msg));
    }

    public Disposable getSub() {
        return sub;
    }

    static void onPublish(ITopic<HazelcastMsg> topic, InterceptPublishMessage msg) {
        HazelcastMsg hazelcastMsg = new HazelcastMsg(msg);
        if (LOG.isDebugEnabled())
            LOG.debug("{} publish on {} message: {}",
                hazelcastMsg.getClientId(), hazelcastMsg.getTopic(), new String(hazelcastMsg.getPayload()));

        topic.publish(hazelcastMsg);
    }
}
