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

import com.google.common.collect.ImmutableMap;
import org.apache.sentry.log.model.HiveLog;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.jdo.*;
import java.util.Properties;

/**
 * Created by kxxu on 2016/8/30.
 */
public class HiveLogTest {
    public static final ImmutableMap<String, String> SENTRY_STORE_DEFAULTS =
            ImmutableMap.<String, String>builder()
//                    .put("datanucleus.connectionPoolingType", "BoneCP")
                    .put("datanucleus.connectionPoolingType", "c3p0")
                    .put("datanucleus.validateTables", "false")
                    .put("datanucleus.validateColumns", "false")
                    .put("datanucleus.validateConstraints", "false")
                    .put("datanucleus.storeManagerType", "rdbms")
                    .put("datanucleus.schema.autoCreateAll", "true")
                    .put("datanucleus.autoCreateSchema", "false")
                    .put("datanucleus.fixedDatastore", "true")
                    .put("datanucleus.autoStartMechanismMode", "checked")
                    .put("datanucleus.transactionIsolation", "read-committed")
//                    .put("datanucleus.cache.level2", "false")
                    .put("datanucleus.cache.level2.type", "none")
//                    .put("datanucleus.identifierFactory", "hivelog")
                    .put("datanucleus.rdbms.useLegacyNativeValueStrategy", "true")
                    .put("datanucleus.plugin.pluginRegistryBundleCheck", "LOG")
                    .put("javax.jdo.PersistenceManagerFactoryClass",
                            "org.datanucleus.api.jdo.JDOPersistenceManagerFactory")
                    .put("javax.jdo.option.DetachAllOnCommit", "true")
                    .put("javax.jdo.option.NonTransactionalRead", "false")
                    .put("javax.jdo.option.NonTransactionalWrite", "false")
                    .put("javax.jdo.option.Multithreaded", "true")
                    .build();
    PersistenceManagerFactory pmf;
    Properties prop = new Properties();
    HiveLog.Builder builder = new HiveLog.Builder();

    @Before
    public void setup() {
        prop.putAll(SENTRY_STORE_DEFAULTS);
        prop.setProperty("javax.jdo.option.ConnectionURL", "jdbc:mysql://172.16.154.120:3306/hueDB");
        prop.setProperty("javax.jdo.option.ConnectionUserName", "root");
        prop.setProperty("javax.jdo.option.ConnectionPassword", "iflytek");
        prop.setProperty("javax.jdo.option.ConnectionDriverName", "com.mysql.jdbc.Driver");
        prop.setProperty("datanucleus.NontransactionalRead", "false");
        prop.setProperty("datanucleus.NontransactionalWrite", "false");
        prop.setProperty("datanucleus.autoCreateTables", "true");
        prop.setProperty("datanucleus.autoCreateSchema", "true");
//        prop.setProperty("datanucleus.schema.autoCreateAll", "true");
        prop.setProperty("datanucleus.fixedDatastore", "false");
        prop.setProperty("datanucleus.identifier.case", "LowerCase");

    }

    @Test
    public void testManager() {
        pmf = JDOHelper.getPersistenceManagerFactory(prop);
        System.out.println(pmf);
    }

    @Test
    public void testCreate() {
        pmf = JDOHelper.getPersistenceManagerFactory(prop);
        PersistenceManager pm = pmf.getPersistenceManager();
        Transaction currentTransaction = pm.currentTransaction();
        currentTransaction.begin();
        pm.makePersistent(createLog());
        pm.currentTransaction().commit();
        pm.close();
    }

    @Test
    public void testDelete() {
        pmf = JDOHelper.getPersistenceManagerFactory(prop);
        PersistenceManager pm = pmf.getPersistenceManager();
        Transaction currentTransaction = pm.currentTransaction();
        currentTransaction.begin();
        Query query = pm.newQuery(HiveLog.class);
        query.setFilter("this.project == t && this.tableName == y");
        query.declareParameters("java.lang.String t,java.lang.String y");
        query.setUnique(true);
        Object value = query.execute("test_123", "test");
        pm.deletePersistent(value);
        pm.currentTransaction().commit();
        pm.close();
    }

    public HiveLog createLog() {
        builder.setQueryId("hive_query").setTableName("test").setProject("test_123").setUser("kxxu").setDescription("create table");
        return builder.build();
    }
}
