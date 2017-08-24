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

package io.moquette.persistence.h2;

import io.moquette.HashColletions;
import io.moquette.spi.IMessagesStore;
import io.moquette.spi.impl.subscriptions.Subscription;
import io.moquette.spi.impl.subscriptions.Topic;
import org.h2.mvstore.MVMap;
import org.h2.mvstore.MVStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.*;

class H2MessagesStore implements IMessagesStore {

    private static final Logger LOG = LoggerFactory.getLogger(H2MessagesStore.class);

    private final MVStore mvStore;

    private MVMap<Topic, Message> retainedStore;

    public H2MessagesStore(MVStore mvStore) {
        this.mvStore = mvStore;
    }

    @Override
    public void initStore() {
        retainedStore = mvStore.openMap("retained");
        LOG.info("Initialized message H2 store");
    }

    @Override
    public void storeRetained(Topic topic, Message storedMessage) {
        LOG.debug("Store retained message for topic={}", topic);
        retainedStore.put(topic, storedMessage);
    }

    @Override
    public Map<Subscription, Collection<Message>> searchMatching(List<Subscription> newSubscriptions) {
        LOG.debug("Scanning retained messages");
        Map<Subscription, Collection<Message>> results = HashColletions.createHashMap(newSubscriptions.size());

        for (Subscription sub : newSubscriptions) {
            retainedStore.forEach((topic, storedMsg) -> {

                //TODO this is ugly, it does a linear scan on potential big dataset
                if (topic.match(sub.getTopicFilter())) {
                    results.computeIfAbsent(sub, k -> new LinkedHashSet<>());
                    results.get(sub).add(storedMsg);
                }
            });
        }

        if (LOG.isTraceEnabled()) {
            LOG.trace("Retained messages have been scanned matchingMessages={}", results);
        }

        return results;
    }

    @Override
    public void cleanRetained(Topic topic) {
        LOG.debug("Cleaning retained messages. Topic={}", topic);
        retainedStore.remove(topic);
    }
}
