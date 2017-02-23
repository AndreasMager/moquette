package io.moquette.spi.impl.subscriptions;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

public final class Topics {

    public static List<Topic> asList(String... topics) {
        return Arrays.asList(topics).stream().map(topic -> new Topic(topic)).collect(Collectors.toList());
    }

    public static Topic[] asArray(String... topics) {
        return asList(topics).toArray(new Topic[topics.length]);
    }

    private Topics() {
    }
}
