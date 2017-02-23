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

import org.assertj.core.api.AbstractAssert;
import org.assertj.core.api.Assertions;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class TopicTest {

    @Test
    public void testParseTopic() {
        assertThatTopic("finance/stock/ibm").containsToken("finance", "stock", "ibm");

        assertThatTopic("/finance/stock/ibm").containsToken(Tokens.EMPTY, "finance", "stock", "ibm");

        assertThatTopic("/").containsToken(Tokens.EMPTY, Tokens.EMPTY);
    }

    @Test
    public void testParseTopicMultiValid() {
        assertThatTopic("finance/stock/#").containsToken("finance", "stock", Tokens.MULTI);

        assertThatTopic("#").containsToken(Tokens.MULTI);
    }

    @Test
    public void testValidationProcess() {
        // TopicMultiInTheMiddle
        assertThatTopic("finance/#/closingprice").isInValid();

        // MultiNotAfterSeparator
        assertThatTopic("finance#").isInValid();

        // TopicMultiNotAlone
        assertThatTopic("/finance/#closingprice").isInValid();

        // SingleNotAferSeparator
        assertThatTopic("finance+").isInValid();

        assertThatTopic("finance/+").isValid();
    }

    @Test
    public void testParseTopicSingleValid() {
        assertThatTopic("finance/stock/+").containsToken("finance", "stock", Tokens.SINGLE);

        assertThatTopic("+").containsToken(Tokens.SINGLE);

        assertThatTopic("finance/+/ibm").containsToken("finance", Tokens.SINGLE, "ibm");
    }

    @Test
    public void testMatchTopics_simple() {
        assertThatTopic("/").matches("/");
        assertThatTopic("/finance").matches("/finance");
    }

    @Test
    public void testMatchTopics_multi() {
        assertThatTopic("finance").matches("#");
        assertThatTopic("finance").matches("finance/#");
        assertThatTopic("finance/stock").matches("finance/#");
        assertThatTopic("finance/stock/ibm").matches("finance/#");
    }

    @Test
    public void testMatchTopics_single() {
        assertThatTopic("finance").matches("+");
        assertThatTopic("finance/stock").matches("finance/+");
        assertThatTopic("finance").doesNotMatch("finance/+");
        assertThatTopic("/finance").matches("/+");
        assertThatTopic("/finance").doesNotMatch("+");
        assertThatTopic("/finance").matches("+/+");
        assertThatTopic("/finance/stock/ibm").matches("/finance/+/ibm");
        assertThatTopic("/").matches("+/+");
        assertThatTopic("sport/").matches("sport/+");
        assertThatTopic("/finance/stock").doesNotMatch("+");
    }

    @Test
    public void rogerLightMatchTopics() {
        assertThatTopic("foo/bar").matches("foo/bar");
        assertThatTopic("foo/bar").matches("foo/+");
        assertThatTopic("foo/bar/baz").matches("foo/+/baz");
        assertThatTopic("foo/bar/baz").matches("foo/+/#");
        assertThatTopic("foo/bar/baz").matches("#");

        assertThatTopic("foo").doesNotMatch("foo/bar");
        assertThatTopic("foo/bar/baz").doesNotMatch("foo/+");
        assertThatTopic("foo/bar/bar").doesNotMatch("foo/+/baz");
        assertThatTopic("fo2/bar/baz").doesNotMatch("foo/+/#");

        assertThatTopic("/foo/bar").matches("#");
        assertThatTopic("/foo/bar").matches("/#");
        assertThatTopic("foo/bar").doesNotMatch("/#");

        assertThatTopic("foo//bar").matches("foo//bar");
        assertThatTopic("foo//bar").matches("foo//+");
        assertThatTopic("foo///baz").matches("foo/+/+/baz");
        assertThatTopic("foo/bar/").matches("foo/bar/+");
    }

    public static TopicAssert assertThatTopic(String topic) {
        return new TopicAssert(new Topic(topic));
    }

    @Test
    public void exceptHeadToken() {
        assertEquals(Topic.asTopic("token"), Topic.asTopic("/token").exceptHeadToken());
        assertEquals(Topic.asTopic("a/b"), Topic.asTopic("/a/b").exceptHeadToken());
    }


    public static TopicAssert assertThat(Topic topic) {
        return new TopicAssert(topic);
    }

    static class TopicAssert extends AbstractAssert<TopicAssert, Topic> {

        TopicAssert(Topic actual) {
            super(actual, TopicAssert.class);
        }

        public TopicAssert matches(String topic) {
            Assertions.assertThat(actual.match(new Topic(topic))).isTrue();

            return myself;
        }

        public TopicAssert doesNotMatch(String topic) {
            Assertions.assertThat(actual.match(new Topic(topic))).isFalse();

            return myself;
        }

        public TopicAssert containsToken(Object... tokens) {
            Assertions.assertThat(actual.getTokens()).containsExactly(asArray(tokens));

            return myself;
        }

        private String[] asArray(Object... l) {
            String[] tokens = new String[l.length];
            for (int i = 0; i < l.length; i++) {
                Object o = l[i];
                if (o instanceof String) {
                    tokens[i] = (String) o;
                } else {
                    tokens[i] = new String(o.toString());
                }
            }

            return tokens;
        }

        public TopicAssert isValid() {
            Assertions.assertThat(actual.isValid()).isTrue();

            return myself;
        }

        public TopicAssert isInValid() {
            Assertions.assertThat(actual.isValid()).isFalse();

            return myself;
        }
    }
}
