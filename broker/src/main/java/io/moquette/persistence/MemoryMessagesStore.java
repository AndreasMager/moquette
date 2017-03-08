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

package io.moquette.persistence;

import io.moquette.HashColletions;
import io.moquette.spi.impl.subscriptions.Subscription;
import io.moquette.spi.impl.subscriptions.Topic;
import io.moquette.spi.IMessagesStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public class MemoryMessagesStore implements IMessagesStore {

    private static final Logger LOG = LoggerFactory.getLogger(MemoryMessagesStore.class);

    private Map<Topic, Message> m_retainedStore = new ConcurrentHashMap<>();

    MemoryMessagesStore() {
    }

    @Override
    public void initStore() {
    }

    @Override
    public void storeRetained(Topic topic, Message storedMessage) {
        LOG.debug("Store retained message for topic={}", topic);
        m_retainedStore.put(topic, storedMessage);
    }

    @Override
    public Map<Subscription, Collection<Message>> searchMatching(List<Subscription> newSubscriptions) {
        LOG.debug("Scanning retained messages...");
        Map<Subscription, Collection<Message>> results = HashColletions.createHashMap(newSubscriptions.size());

        for (Subscription sub : newSubscriptions) {
            m_retainedStore.forEach((topic, storedMsg) -> {
                // TODO this is ugly, it does a linear scan on potential big dataset
                if (topic.match(sub.getTopicFilter())) {
                    results.computeIfAbsent(sub, k -> new LinkedList<>());
                    results.get(sub).add(storedMsg);
                }
            });
        }

        LOG.trace("The retained messages have been scanned. MatchingMessages = {}.", results);

        return results;
    }

    @Override
    public void cleanRetained(Topic topic) {
        m_retainedStore.remove(topic);
    }
}
