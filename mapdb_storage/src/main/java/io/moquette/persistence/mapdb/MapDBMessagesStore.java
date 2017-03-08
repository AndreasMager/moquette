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

package io.moquette.persistence.mapdb;

import io.moquette.HashColletions;
import io.moquette.spi.IMessagesStore;
import io.moquette.spi.impl.subscriptions.Subscription;
import io.moquette.spi.impl.subscriptions.Topic;
import org.mapdb.DB;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentMap;

/**
 * IMessagesStore implementation backed by MapDB.
 */
class MapDBMessagesStore implements IMessagesStore {

    private static final Logger LOG = LoggerFactory.getLogger(MapDBMessagesStore.class);

    private DB m_db;

    private ConcurrentMap<Topic, Message> m_retainedStore;

    MapDBMessagesStore(DB db) {
        m_db = db;
    }

    @Override
    public void initStore() {
        m_retainedStore = m_db.getHashMap("retained");
        LOG.info("Initialized store");
    }

    @Override
    public Map<Subscription, Collection<Message>> searchMatching(List<Subscription> newSubscriptions) {
        LOG.debug("Scanning retained messages");
        Map<Subscription, Collection<Message>> results = HashColletions.createHashMap(newSubscriptions.size());

        for (Subscription sub : newSubscriptions) {
            for (Map.Entry<Topic, Message> entry : m_retainedStore.entrySet()) {
                Message storedMsg = entry.getValue();

                //TODO this is ugly, it does a linear scan on potential big dataset
                if (entry.getKey().match(sub.getTopicFilter())) {
                    results.computeIfAbsent(sub, k -> new LinkedHashSet<>());
                    results.get(sub).add(storedMsg);
                }
            }
        }

        if (LOG.isTraceEnabled()) {
            LOG.trace("Retained messages have been scanned matchingMessages={}", results);
        }

        return results;
    }

    @Override
    public void cleanRetained(Topic topic) {
        LOG.debug("Cleaning retained messages. Topic={}", topic);
        m_retainedStore.remove(topic);
    }

    @Override
    public void storeRetained(Topic topic, Message storedMessage) {
        LOG.debug("Store retained message for topic={}", topic);
        m_retainedStore.put(topic, storedMessage);
    }
}
