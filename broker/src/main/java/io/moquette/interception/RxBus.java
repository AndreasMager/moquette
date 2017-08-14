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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import io.reactivex.Observable;
import io.reactivex.subjects.PublishSubject;

public class RxBus {

    private static final Logger LOG = LoggerFactory.getLogger(RxBus.class);

    private PublishSubject<Object> subject = PublishSubject.create();

    public void publish(Object object) {
        subject.onNext(object);
    }

    public void publishSafe(Object object) {
        try {
            publish(object);
        } catch (Throwable t) {
            LOG.error(t.toString(), t);
        }
    }

    public Observable<Object> getEvents() {
        return subject;
    }
}
