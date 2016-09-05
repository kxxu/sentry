/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.sentry.core.db;

import com.google.common.base.CharMatcher;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * Created by kxxu on 2016/9/1.
 */
public class TestChar {
    CharMatcher charMatcher;

    @Before
    public void setup() {
        charMatcher = CharMatcher.is('`');
    }

    @Test
    public void testChar() {
        String input = "hello world";
        assertEquals("hello world", charMatcher.removeFrom(input));
    }

    @Test
    public void removeChar() {
        String input = "`test`";
        assertEquals("test", charMatcher.removeFrom(input));
    }

    @Test
    public void removeOneChar() {
        String input = "`test";
        assertEquals("test", charMatcher.removeFrom(input));
    }

    @Test
    public void removeNull() {
        String input = null;
        System.out.println(charMatcher.removeFrom(input));
    }
}
