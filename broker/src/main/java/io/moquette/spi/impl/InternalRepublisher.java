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

import io.moquette.spi.ClientSession;
import io.moquette.spi.IMessagesStore;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.BlockingQueue;

class InternalRepublisher {

    private final PersistentQueueMessageSender messageSender;

    InternalRepublisher(PersistentQueueMessageSender messageSender) {
        this.messageSender = messageSender;
    }

    void publishRetained(ClientSession targetSession, Collection<IMessagesStore.StoredMessage> messages) {
        for (IMessagesStore.StoredMessage msg : messages) {
            messageSender.sendPublish(targetSession, msg, msg.getQos(), true);
        }
    }

    void publishStored(ClientSession clientSession, BlockingQueue<IMessagesStore.StoredMessage> publishedEvents) {
        List<IMessagesStore.StoredMessage> storedPublishes = new ArrayList<>();
        publishedEvents.drainTo(storedPublishes);

        for (IMessagesStore.StoredMessage msg : storedPublishes) {
            messageSender.sendPublish(clientSession, msg, msg.getQos(), false);
        }
    }
}
