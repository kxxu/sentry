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
package com.iflytek.odeon.service;

import org.apache.hadoop.conf.Configuration;
import org.apache.sentry.provider.db.service.persistent.SentryStore;
import org.junit.Before;
import org.junit.Test;

import java.util.Set;

/**
 * Created by Administrator on 2016/11/4.
 */
public class SentryStoreTest {

    @Before
    public void setup() {
        Configuration.addDefaultResource("sentry-site.xml");
    }

    @Test
    public void test() {
        Configuration configuration = new Configuration();
        try {
            SentryStore sentryStore = new SentryStore(configuration);
            Set<String> names = sentryStore.getAllRoleNames();
            System.out.println(names);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
