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
package org.apache.sentry.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BufferedFSInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FsUrlStreamHandlerFactory;
import org.apache.hadoop.fs.Path;
import org.junit.Before;
import org.junit.Test;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.MalformedURLException;
import java.net.URL;

/**
 * Created by kxxu on 2016/9/23.
 */
public class HdfsConfTest {

    @Before
    public void setup() {
//        Configuration.addDefaultResource("hdfs-site.xml");
//        Configuration.addDefaultResource("core-site.xml");
//        Configuration.addDefaultResource("mapred-site.xml");
    }

    @Test
    public void testReadFile() throws IOException {
        FileSystem fileSystem = FileSystem.get(new Configuration());
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(
                fileSystem.open(new Path("/user/sentry/conf/hive-sentry-site.xml"))))){
            String line;
            while ((line = reader.readLine()) != null) {
                System.out.println(line);
            }
        }
    }

    @Test
    public void testSentryAuthConf() throws MalformedURLException {
        String conf = "hdfs://hfa-alpha0001.maple.cn:8020/user/sentry/conf/hive-sentry-site.xml";
        URL.setURLStreamHandlerFactory(new FsUrlStreamHandlerFactory());
        URL url = new URL(conf);
        Configuration configuration = new Configuration();
        configuration.addResource(url);
        System.out.println(configuration.get("sentry.service.client.server.rpc-address"));

    }

    @Test
    public void testLocalSentryConf() throws MalformedURLException {
        URL.setURLStreamHandlerFactory(new FsUrlStreamHandlerFactory());
        String conf = "file:///github/sentry/sentry-hdfs/sentry-hdfs-namenode-plugin/src/test/resources/hdfs-sentry.xml";

        Configuration configuration2 = new Configuration();
        configuration2.addResource(new URL(conf));
//        configuration2.addResource(conf);
        System.out.println(configuration2.get("sentry.hdfs-plugin.path-prefixes"));
        System.out.println(configuration2.get("dfs.namenode.servicerpc-address"));
    }
}
