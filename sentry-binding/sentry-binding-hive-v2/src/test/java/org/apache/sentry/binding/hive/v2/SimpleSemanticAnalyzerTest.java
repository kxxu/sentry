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
package org.apache.sentry.binding.hive.v2;

import org.apache.hadoop.hive.ql.plan.HiveOperation;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HiveAuthzPluginException;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.sentry.binding.hive.v2.util.SimpleSemanticAnalyzer;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.mockito.Mockito.mock;
import static org.powermock.api.mockito.PowerMockito.mockStatic;
import static org.powermock.api.mockito.PowerMockito.when;

/**
 * Created by kxxu on 2016/9/19.
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest({SessionState.class})
public class SimpleSemanticAnalyzerTest {
    SessionState seessionState;
    @Before
    public void setup() {
        seessionState = mock(SessionState.class);
        when(seessionState.getCurrentDatabase()).thenReturn("test");
        mockStatic(SessionState.class);
        when(SessionState.get()).thenReturn(seessionState);
    }

    @Test
    public void testCreateTable() throws HiveAuthzPluginException {
        SimpleSemanticAnalyzer analyzer = new SimpleSemanticAnalyzer(HiveOperation.CREATETABLE,
                "create  table re(id int)");
        assert analyzer.getCurrentDb().equals("test");
        assert analyzer.getCurrentTb().equals("re");
        System.out.println(analyzer.getCurrentTb());

    }

    @Test
    public void testExplainCreateTable() throws HiveAuthzPluginException {
        SimpleSemanticAnalyzer analyzer = new SimpleSemanticAnalyzer(HiveOperation.CREATETABLE,
                "explain create table re(id int)");
        assert analyzer.getCurrentDb().equals("test");
        assert "re".equals(analyzer.getCurrentTb());
        System.out.println(analyzer.getCurrentTb());
    }

    @Test
    public void testDescTable() throws HiveAuthzPluginException {
        SimpleSemanticAnalyzer analyzer = new SimpleSemanticAnalyzer(HiveOperation.DESCTABLE,
                "describe re");
        assert "re".equals(analyzer.getCurrentTb());
    }

    @Test
    public void testExplain() {
        String explain = "^explain\\s+";
        Pattern pattern = Pattern.compile(explain, Pattern.CASE_INSENSITIVE);
        Matcher matcher = pattern.matcher("EXPLAIN create table value(key int)");
        assert matcher.find();
        System.out.println(matcher.replaceFirst(""));
    }

    @Test
    public void testDesc() {
        String desc = "^(desc|describe)\\s+((EXTENDED|FORMATTED)\\s+)?([A-Za-z0-9._]+)";
        Pattern pattern = Pattern.compile(desc);
        Matcher matcher = pattern.matcher("desc test");
        assert matcher.find();
        String tableName = matcher.group(matcher.groupCount());
        assert "test".equals(tableName);
        matcher = pattern.matcher("describe test");
        assert matcher.find();
    }
}
