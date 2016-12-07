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
package org.apache.sentry.log.constants;

import com.google.common.collect.ImmutableMap;

/**
 * Created by kxxu on 2016/9/2.
 */
public interface JDOConstants {
    ImmutableMap<String, String> HIVELOG_STORE_DEFAULTS = ImmutableMap.<String, String>builder()
//            .put("datanucleus.connectionPoolingType", "c3p0")
            .put("datanucleus.connectionPoolingType", "BoneCP")
            .put("datanucleus.validateTables", "false")
            .put("datanucleus.validateColumns", "false")
            .put("datanucleus.validateConstraints", "false")
            .put("datanucleus.storeManagerType", "rdbms")
            .put("datanucleus.schema.autoCreateAll", "true")
//            .put("datanucleus.autoCreateSchema", "false")
            .put("datanucleus.fixedDatastore", "false")
            .put("datanucleus.autoStartMechanismMode", "checked")
            .put("datanucleus.transactionIsolation", "read-committed")
            .put("datanucleus.cache.level2.type", "none")
            .put("datanucleus.rdbms.useLegacyNativeValueStrategy", "true")
            .put("datanucleus.plugin.pluginRegistryBundleCheck", "LOG")
            .put("javax.jdo.PersistenceManagerFactoryClass",
                    "org.datanucleus.api.jdo.JDOPersistenceManagerFactory")
            .put("javax.jdo.option.DetachAllOnCommit", "true")
            .put("javax.jdo.option.NonTransactionalRead", "false")
            .put("javax.jdo.option.NonTransactionalWrite", "false")
            .put("javax.jdo.option.Multithreaded", "true")
            .put("javax.jdo.option.ConnectionDriverName", "com.mysql.jdbc.Driver")
            .put("datanucleus.NontransactionalRead", "false")
            .put("datanucleus.NontransactionalWrite", "false")
            .put("datanucleus.autoCreateTables", "true")
            .put("datanucleus.autoCreateSchema", "true")
            .put("datanucleus.identifier.case", "LowerCase")
            .build();

}
