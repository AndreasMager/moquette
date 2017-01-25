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
        assertThat(new Topic("finance/stock/ibm")).containsToken("finance", "stock", "ibm");

        assertThat(new Topic("/finance/stock/ibm")).containsToken(Tokens.EMPTY, "finance", "stock", "ibm");

        assertThat(new Topic("/")).containsToken(Tokens.EMPTY, Tokens.EMPTY);
    }

    @Test
    public void testParseTopicMultiValid() {
        assertThat(new Topic("finance/stock/#")).containsToken("finance", "stock", Tokens.MULTI);

        assertThat(new Topic("#")).containsToken(Tokens.MULTI);
    }

    @Test
    public void testValidationProcess() {
        // TopicMultiInTheMiddle
        assertThat(new Topic("finance/#/closingprice")).isInValid();

        // MultiNotAfterSeparator
        assertThat(new Topic("finance#")).isInValid();

        // TopicMultiNotAlone
        assertThat(new Topic("/finance/#closingprice")).isInValid();

        // SingleNotAferSeparator
        assertThat(new Topic("finance+")).isInValid();

        assertThat(new Topic("finance/+")).isValid();
    }

    @Test
    public void testParseTopicSingleValid() {
        assertThat(new Topic("finance/stock/+")).containsToken("finance", "stock", Tokens.SINGLE);

        assertThat(new Topic("+")).containsToken(Tokens.SINGLE);

        assertThat(new Topic("finance/+/ibm")).containsToken("finance", Tokens.SINGLE, "ibm");
    }

    @Test
    public void testMatchTopics_simple() {
        assertThat(new Topic("/")).matches("/");
        assertThat(new Topic("/finance")).matches("/finance");
    }

    @Test
    public void testMatchTopics_multi() {
        assertThat(new Topic("finance")).matches("#");
        assertThat(new Topic("finance")).matches("finance/#");
        assertThat(new Topic("finance/stock")).matches("finance/#");
        assertThat(new Topic("finance/stock/ibm")).matches("finance/#");
    }

    @Test
    public void testMatchTopics_single() {
        assertThat(new Topic("finance")).matches("+");
        assertThat(new Topic("finance/stock")).matches("finance/+");
        assertThat(new Topic("finance")).doesNotMatch("finance/+");
        assertThat(new Topic("/finance")).matches("/+");
        assertThat(new Topic("/finance")).doesNotMatch("+");
        assertThat(new Topic("/finance")).matches("+/+");
        assertThat(new Topic("/finance/stock/ibm")).matches("/finance/+/ibm");
        assertThat(new Topic("/")).matches("+/+");
        assertThat(new Topic("sport/")).matches("sport/+");
        assertThat(new Topic("/finance/stock")).doesNotMatch("+");
    }

    @Test
    public void rogerLightMatchTopics() {
        assertThat(new Topic("foo/bar")).matches("foo/bar");
        assertThat(new Topic("foo/bar")).matches("foo/+");
        assertThat(new Topic("foo/bar/baz")).matches("foo/+/baz");
        assertThat(new Topic("foo/bar/baz")).matches("foo/+/#");
        assertThat(new Topic("foo/bar/baz")).matches("#");

        assertThat(new Topic("foo")).doesNotMatch("foo/bar");
        assertThat(new Topic("foo/bar/baz")).doesNotMatch("foo/+");
        assertThat(new Topic("foo/bar/bar")).doesNotMatch("foo/+/baz");
        assertThat(new Topic("fo2/bar/baz")).doesNotMatch("foo/+/#");

        assertThat(new Topic("/foo/bar")).matches("#");
        assertThat(new Topic("/foo/bar")).matches("/#");
        assertThat(new Topic("foo/bar")).doesNotMatch("/#");

        assertThat(new Topic("foo//bar")).matches("foo//bar");
        assertThat(new Topic("foo//bar")).matches("foo//+");
        assertThat(new Topic("foo///baz")).matches("foo/+/+/baz");
        assertThat(new Topic("foo/bar/")).matches("foo/bar/+");
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
