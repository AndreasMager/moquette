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
package io.moquette.spi.impl.subscriptions;

import java.util.Objects;

/**
 * Internal use only class.
 * */
public final class Tokens {

    static final String EMPTY = "";
    static final String MULTI = "#";
    static final String SINGLE = "+";

    static boolean match(String t1, String t2) {
        if (Objects.equals(t2, MULTI) || Objects.equals(t2, SINGLE))
            return false;

        if (Objects.equals(t1, MULTI) || Objects.equals(t1, SINGLE))
            return true;

        return Objects.equals(t1, t2);
    }

    private Tokens() {
    }
}
