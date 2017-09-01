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

import static org.assertj.core.api.Assertions.*;
import org.junit.Test;

public class DebugUtilsTest {

    @Test
    public void isNotPrintableAscii() {
        assertThat(DebugUtils.isNotPrintableAscii((byte) 0x00)).isTrue();
        assertThat(DebugUtils.isNotPrintableAscii('\n')).isTrue();

        assertThat(DebugUtils.isNotPrintableAscii(' ')).isFalse();
        assertThat(DebugUtils.isNotPrintableAscii('a')).isFalse();
        assertThat(DebugUtils.isNotPrintableAscii('z')).isFalse();
        assertThat(DebugUtils.isNotPrintableAscii('A')).isFalse();
        assertThat(DebugUtils.isNotPrintableAscii('Z')).isFalse();
    }

    @Test
    public void payload2Str() {
        assertThat(DebugUtils.payload2Str(new byte[] {})).isEmpty();

        assertThat(DebugUtils.payload2Str(new byte[] {(byte) 0x00 , (byte) 0x01})).isEqualTo("0x0001");

        assertThat(DebugUtils.payload2Str("Hello".getBytes())).isEqualTo("Hello");
    }
}
