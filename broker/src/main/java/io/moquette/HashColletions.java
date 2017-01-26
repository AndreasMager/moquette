package io.moquette;

import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.function.IntFunction;

public class HashColletions {

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
}
