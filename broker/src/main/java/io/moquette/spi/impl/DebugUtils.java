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

import java.util.stream.IntStream;
import org.apache.commons.codec.binary.Hex;
import io.netty.buffer.ByteBuf;

final class DebugUtils {

    public static boolean isNotPrintableAscii(byte value) {
        return value < 32;
    }

    public static boolean isNotPrintableAscii(int value) {
        return value < 32;
    }

    public static IntStream intStream(byte[] array) {
        return IntStream.range(0, array.length).map(idx -> array[idx]);
    }

    public static String payload2Str(ByteBuf content) {
        return payload2Str(content.copy().array());
    }

    public static String payload2Str(byte[] content) {
        boolean notPrintAble = intStream(content)
            .filter(DebugUtils::isNotPrintableAscii)
            .findAny()
            .isPresent();

        if (notPrintAble)
            return "0x" + Hex.encodeHexString(content);

        return new String(content);
    }

    private DebugUtils() {
    }
}
