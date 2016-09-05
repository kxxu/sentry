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
package org.apache.sentry.log.jdo;

import org.apache.sentry.log.model.HiveLog;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by kxxu on 2016/9/2.
 */
public class HiveLogAppenderTest {

    HiveLog.Builder builder;
    @Before
    public void setup() {
        builder = new HiveLog.Builder();
    }

    @Test
    public void testLogger() {
        Logger Log = LoggerFactory.getLogger(HiveLogAppenderTest.class);
        Log.info("hello world");
    }

    @Test
    public void testJdbc() {
        Logger Log = LoggerFactory.getLogger("jdbc");
        Log.info("{}:{}", "create", createLog());
    }

    @Test
    public void testDelete() {
        Logger Log = LoggerFactory.getLogger("jdbc");
        Log.info("{}:{}", "drop", createLog());
    }

    public HiveLog createLog() {
        builder.setQueryId("hive_query").setServer("bj").setTableName("test").setProject("test_123").setUser("kxxu").setDescription("create table");
        return builder.build();
    }

}
