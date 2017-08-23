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

import io.moquette.BrokerConstants;
import io.moquette.persistence.MessageStoreTCK;
import io.moquette.server.config.IConfig;
import io.moquette.server.config.MemoryConfig;
import org.junit.After;
import org.junit.Before;

import java.util.Properties;
import java.util.concurrent.ScheduledExecutorService;

public class MemoryMessageStoreTest extends MessageStoreTCK {

    MemoryStorageService m_storageService;

    private ScheduledExecutorService scheduler;

    @Before
    public void setUp() throws Exception {
        Properties props = new Properties();
        props.setProperty(BrokerConstants.PERSISTENT_STORE_PROPERTY_NAME, BrokerConstants.DEFAULT_PERSISTENT_PATH);
        IConfig conf = new MemoryConfig(props);
        m_storageService = new MemoryStorageService(conf, scheduler);
        m_storageService.initStore();
        messagesStore = m_storageService.messagesStore();
        sessionsStore = m_storageService.sessionsStore();
    }

    @After
    public void tearDown() {
        if (m_storageService != null) {
            m_storageService.close();
        }
    }
}
