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

package io.moquette;

import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.function.IntFunction;

public final class HashColletions {

    private static final int DEFAULT_INITIAL_CAPACITY = 16;
    private static final float DEFAULT_LOAD_FACTOR = 0.75f;

    public static <T> T create(IntFunction<T> create, int size) {
        return create.apply(Math.max(
                (int) (size / DEFAULT_LOAD_FACTOR) + 1,
                DEFAULT_INITIAL_CAPACITY));
    }

    public static <K, V> HashMap<K, V> createHashMap(int size) {
        return create(HashMap::new, size);
    }

    public static <K, V> LinkedHashMap<K, V> createLinkedHashMap(int size) {
        return create(LinkedHashMap::new, size);
    }

    public static <E> HashSet<E> createHashSet(int size) {
        return create(HashSet::new, size);
    }

    private HashColletions() {
    }
}
