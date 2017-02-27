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

import java.io.Serializable;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Topic implements Serializable {

    private static final Logger LOG = LoggerFactory.getLogger(Topic.class);

    private static final long serialVersionUID = 2438799283749822L;

    public static int rootLength = 1;

    private final String topic;

    private transient List<String> tokens;

    private transient boolean valid;

    private transient String root;

    private transient String tail;

    /**
     * Factory method
     * */
    public static Topic asTopic(String s) {
        return new Topic(s);
    }

    public Topic(String topic) {
        this.topic = topic;
    }

    Topic(List<String> tokens) {
        this.tokens = tokens;
        this.topic = String.join("/", tokens);
        this.valid = true;
    }

    public Topic(String root, String tail) {
        this.root = root;
        this.tail = tail;
        this.topic = root + "/" + tail;
    }

    public List<String> getTokens() {
        if (tokens == null) {
            try {
                tokens = parseTopic(topic);
                valid = true;
            } catch (ParseException e) {
                valid = false;
                LOG.error("Error parsing the topic: {}, message: {}", topic, e.getMessage());
            }
        }

        return tokens;
    }

    private List<String> parseTopic(String topic) throws ParseException {
        List<String> res = new ArrayList<>();
        String[] splitted = topic.split("/");

        if (splitted.length == 0) {
            res.add(Tokens.EMPTY);
        }

        if (topic.endsWith("/")) {
            // Add a fictious space
            String[] newSplitted = new String[splitted.length + 1];
            System.arraycopy(splitted, 0, newSplitted, 0, splitted.length);
            newSplitted[splitted.length] = "";
            splitted = newSplitted;
        }

        for (int i = 0; i < splitted.length; i++) {
            String s = splitted[i];
            if (s.isEmpty()) {
                // if (i != 0) {
                // throw new ParseException("Bad format of topic, expetec topic name between
                // separators", i);
                // }
                res.add(Tokens.EMPTY);
            } else if (s.equals("#")) {
                // check that multi is the last symbol
                if (i != splitted.length - 1) {
                    throw new ParseException(
                            "Bad format of topic, the multi symbol (#) has to be the last one after a separator",
                            i);
                }
                res.add(Tokens.MULTI);
            } else if (s.contains("#")) {
                throw new ParseException("Bad format of topic, invalid subtopic name: " + s, i);
            } else if (s.equals("+")) {
                res.add(Tokens.SINGLE);
            } else if (s.contains("+")) {
                throw new ParseException("Bad format of topic, invalid subtopic name: " + s, i);
            } else {
                res.add(s);
            }
        }

        return Collections.unmodifiableList(res);
    }

    public String headToken() {
        final List<String> tokens = getTokens();
        if (tokens.isEmpty()) {
            //TODO UGLY use Optional
            return null;
        }
        return tokens.get(0);
    }

    public boolean isEmpty() {
        final List<String> tokens = getTokens();
        return tokens == null || tokens.isEmpty();
    }

    /**
     * @return a new Topic corresponding to this less than the head token
     * */
    public Topic exceptHeadToken() {
        List<String> tokens = getTokens();
        if (tokens.isEmpty()) {
            return new Topic(Collections.emptyList());
        }
        List<String> tokensCopy = new ArrayList<>(tokens);
        tokensCopy.remove(0);
        return new Topic(tokensCopy);
    }

    public boolean isValid() {
        if (tokens == null)
            getTokens();

        return valid;
    }

    /**
     * Verify if the 2 topics matching respecting the rules of MQTT Appendix A
     *
     * @param subscriptionTopic
     *            the topic filter of the subscription
     * @return true if the two topics match.
     */
    // TODO reimplement with iterators or with queues
    public boolean match(Topic subscriptionTopic) {
        List<String> msgTokens = getTokens();
        List<String> subscriptionTokens = subscriptionTopic.getTokens();
        int i = 0;
        for (; i < subscriptionTokens.size(); i++) {
            String subToken = subscriptionTokens.get(i);
            if (subToken != Tokens.MULTI && subToken != Tokens.SINGLE) {
                if (i >= msgTokens.size()) {
                    return false;
                }
                String msgToken = msgTokens.get(i);
                if (!msgToken.equals(subToken)) {
                    return false;
                }
            } else {
                if (subToken == Tokens.MULTI) {
                    return true;
                }
                if (subToken == Tokens.SINGLE) {
                    // skip a step forward
                }
            }
        }
        // if last token was a SINGLE then treat it as an empty
        // if (subToken == Token.SINGLE && (i - msgTokens.size() == 1)) {
        // i--;
        // }
        return i == msgTokens.size();
    }

    public String getRoot() {
        if (root == null)
            initRoot();

        return root;
    }

    private void initRoot() {
        if (getTokens().size() < rootLength)
            LOG.warn("Topic with rootlength={} has no Tail: {}", rootLength, topic);

        this.root = String.join("/", getTokens().subList(0, Math.min(rootLength, getTokens().size())));
    }

    public String getTail() {
        if (tail == null)
            initTail();

        return tail;
    }

    private void initTail() {
        if (getTokens().size() < rootLength) {
            LOG.warn("Topic with rootlength={} has no Tail: {}", rootLength, topic);
            this.tail = "";
            return;
        }

        this.tail = String.join("/", getTokens().subList(rootLength, getTokens().size()));
    }

    @Override
    public String toString() {
        return topic;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        Topic other = (Topic) obj;

        return Objects.equals(this.topic, other.topic);
    }

    @Override
    public int hashCode() {
        return topic.hashCode();
    }

}
