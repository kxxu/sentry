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
package org.apache.sentry.log;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

/**
 * Created by admin on 2016/8/26.
 */
public class DateTest {
    public static void main(String[] args) throws Exception {
        LoadingCache<String,String> cache = CacheBuilder.newBuilder().expireAfterWrite(2, TimeUnit.SECONDS)
                                        .build(new CacheLoader<String, String>() {
                                            @Override
                                            public String load(String key) throws Exception {
                                                return key + ": " + System.currentTimeMillis();
                                            }
                                        });
        String value = cache.get("world");
        System.out.println("value: " + value);
        Thread.sleep(1 * 1000);
        System.out.println("sleep 1s, value: " + cache.get("world"));
        System.out.println(cache.get("world"));
        Thread.sleep(1 * 1000);
        System.out.println("sleep 2s, value: " + cache.get("world"));
    }
}
