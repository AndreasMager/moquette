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

import com.hazelcast.config.ClasspathXmlConfig;
import com.hazelcast.config.Config;
import com.hazelcast.config.FileSystemXmlConfig;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.ITopic;
import io.moquette.BrokerConstants;
import io.moquette.interception.messages.InterceptPublishMessage;
import io.moquette.server.HazelcastListener;
import io.moquette.server.Server;
import io.moquette.spi.impl.Daemon;
import io.netty.buffer.ByteBuf;
import io.reactivex.disposables.Disposable;
import io.reactivex.schedulers.Schedulers;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import static io.moquette.spi.impl.Utils.readBytesAndRewind;

public class HazelcastRXHandler implements Daemon {

    private static final Logger LOG = LoggerFactory.getLogger(HazelcastRXHandler.class);

    private Disposable sub;

    private Server server;

    public HazelcastRXHandler(Server server) {
    }

    @Override
    public void init(Server server) {
        this.server = server;
        initHZ(server);
    }

    public static void initHZ(Server server) {
        LOG.info("Configuring embedded Hazelcast instance");
        String hzConfigPath = server.getConfig().getProperty(BrokerConstants.HAZELCAST_CONFIGURATION);
        if (hzConfigPath != null) {
            boolean isHzConfigOnClasspath = HazelcastRXHandler.class.getClassLoader().getResource(hzConfigPath) != null;
            try {
                Config hzconfig = isHzConfigOnClasspath
                    ? new ClasspathXmlConfig(hzConfigPath)
                    : new FileSystemXmlConfig(hzConfigPath);
                LOG.info("Starting Hazelcast instance. ConfigurationFile={}", hzconfig);
                server.setHazelcastInstance(Hazelcast.newHazelcastInstance(hzconfig));
            } catch (Exception e) {
                LOG.error(e.toString(), e);
                LOG.warn("Starting Hazelcast instance with default configuration");
                server.setHazelcastInstance(Hazelcast.newHazelcastInstance());
            }
        } else {
            LOG.info("Starting Hazelcast instance with default configuration");
            server.setHazelcastInstance(Hazelcast.newHazelcastInstance());
        }

        LOG.info("Subscribing to Hazelcast topic. TopicName={}", "moquette");
        HazelcastInstance hz = server.getHazelcastInstance();
        ITopic<HazelcastMsg> topic = hz.getTopic("moquette");
        topic.addMessageListener(new HazelcastListener(server));
    }

    @Override
    public void start() {
        HazelcastInstance hz = server.getHazelcastInstance();

        sub = server.getProcessor().getBus().getEvents()
            .filter(msg -> msg instanceof InterceptPublishMessage)
            .cast(InterceptPublishMessage.class)
            .observeOn(Schedulers.single()) // Don't pause netty eventloop thread
            .subscribe(msg ->  onPublish(hz, msg));
    }

    @Override
    public void stop() {
        if (sub != null)
            sub.dispose();
    }

    @Override
    public void destroy() {
        server = null;
        sub = null;
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
