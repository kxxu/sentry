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
package org.apache.sentry.binding;

import com.google.common.base.Joiner;
import com.google.common.collect.Sets;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.hooks.Entity;
import org.apache.hadoop.hive.ql.hooks.ReadEntity;
import org.apache.hadoop.hive.ql.hooks.WriteEntity;
import org.apache.hadoop.hive.ql.parse.HiveSemanticAnalyzerHookContext;
import org.apache.hadoop.hive.ql.plan.HiveOperation;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HiveAuthzContext;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HiveOperationType;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HivePrivilegeObject;
import org.apache.logging.slf4j.Log4jLoggerFactory;
import org.apache.sentry.log.CreateTableLog;
import org.apache.sentry.log.appender.JDBCTableAppender;
import org.apache.sentry.log.model.HiveLog;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Set;

/**
 * Created by admin on 2016/8/24.
 */
public class AuditLog {
    private static final Logger AuditLogger = LoggerFactory.getLogger("audit");
    private static final Logger HiveJdbcLogger = LoggerFactory.getLogger("hivelog");
    private static final Logger HiveLogLogger = LoggerFactory.getLogger("tablelog");
    private static final Logger Log = LoggerFactory.getLogger(AuditLog.class);
    private static final String EntitySplitter= ";";
    private static final Joiner EntityJoiner = Joiner.on(";").skipNulls();
    private AuditLog(){}

    public static void logAuditEvent(String serverName, HiveSemanticAnalyzerHookContext context, HiveOperation operation) {
        String queryId = context.getConf().get(HiveConf.ConfVars.HIVEQUERYID.toString());

        Log.debug("query id:{}, user:{}, operation: {}, input: {}, output: {}, command: {}", new Object[]{
                queryId,context.getUserName(), operation, context.getInputs(),
                context.getOutputs(), context.getCommand()});
        Set<String> src = Sets.newHashSet();
        for (ReadEntity readEntity : context.getInputs()) {
            if (isChildTabForView(readEntity) || isDummyEntity(readEntity)) {
                continue;
            }
            if (readEntity.getTable() != null) {
                src.add(readEntity.getTable().getDbName() + "." + readEntity.getTable().getTableName());
            }
        }
        Set<String> dest = Sets.newHashSet();
        for (WriteEntity writeEntity : context.getOutputs()) {
            if (writeEntity.getTable() != null) {
                dest.add(writeEntity.getTable().getDbName() + "." + writeEntity.getTable().getTableName());
            }
        }

        AuditLogger.info("query_id={}|user={}|operation={}|src={}|dst={}|command={}", new Object[]{
                queryId, context.getUserName(), operation.getOperationName(),
                EntityJoiner.join(src), EntityJoiner.join(dest), context.getCommand().replaceAll("\\s+", " ")});
    }

    public static void logAuditLog(HiveOperationType hiveOpType, List<HivePrivilegeObject> inputHObjs,
                                   List<HivePrivilegeObject> outputHObjs, HiveAuthzContext context,
                                   String serverName, String user) {
//        CreateTableLog.Builder builder = new CreateTableLog.Builder();
        HiveLog.Builder builder = new HiveLog.Builder();
//        String queryId = context.getConf().get(HiveConf.ConfVars.HIVEQUERYID.toString());
        if (hiveOpType == HiveOperationType.CREATETABLE || hiveOpType == HiveOperationType.CREATETABLE_AS_SELECT) {
            Log.debug("create table, input: {}, output: {}", new Object[]{inputHObjs,
                    outputHObjs});
            if (outputHObjs.size() > 0) {
                HivePrivilegeObject table = outputHObjs.get(outputHObjs.size() - 1);
                Log.debug("[create table]table privilege, db: {}, table: {}", table.getDbname(),
                        table.getObjectName());
//                builder.setQueryId("").setTableName(table.getObjectName())
//                        .setCreateUser(user)
//                        .setProject(table.getDbname()).setTableDescription(context.getCommandString());
                builder.setTableName(table.getObjectName()).setUser(user)
                        .setProject(table.getDbname())
                        .setOperation("create")
                        .setServer(serverName)
                        .setDescription(context.getCommandString().replaceAll("\\s+", ""));
//                HiveJdbcLogger.info(builder.build().toString());
                HiveLogLogger.info("{}", builder.build());
            }
        } else if (hiveOpType == HiveOperationType.DROPTABLE){
            Log.debug("drop table, input: {}, output: {}", new Object[]{
                    inputHObjs, outputHObjs});
            if (inputHObjs.size() > 0) {
                HivePrivilegeObject table = inputHObjs.get(inputHObjs.size() - 1);
//                builder.setQueryId("").setTableName(table.getObjectName())
//                        .setCreateUser(user)
//                        .setProject(table.getDbname()).setTableDescription("drop");
//                HiveJdbcLogger.info(builder.build().toString());
                builder.setOperation("drop")
                        .setProject(table.getDbname())
                        .setTableName(table.getObjectName())
                        .setUser(user).setServer(serverName);
                HiveLogLogger.info("{}", builder.build());
            }
        }
    }


    static void writeHiveLog() {
//        HiveLog.Builder builder = new HiveLog.Builder();
//        builder.setQueryId(queryId).setServer(serverName).setUser(context.getUserName());
//        if (HiveOperation.CREATETABLE == operation || HiveOperation.CREATETABLE_AS_SELECT == operation) {
//            String[] objSplit = writeEntityStr.split("\\.");
//            builder.setProject(objSplit[0])
//                    .setTableName(objSplit[1]).setDescription(serverName);
//            HiveJdbcLogger.info("{}:{}", "create", builder.build());
//        } else if (HiveOperation.DROPTABLE == operation) {
//            String[] objSplit = writeEntityStr.split("\\.");
//            builder.setProject(objSplit[0])
//                    .setTableName(objSplit[1]).setDescription(serverName);
//            HiveJdbcLogger.info("{}:{}", "drop", builder.build());
//
//        }
    }




    private static boolean isDummyEntity(ReadEntity readEntity) {
        return readEntity.isDummy();
    }

    private static boolean isChildTabForView(ReadEntity readEntity) {
        if (!readEntity.getType().equals(Entity.Type.TABLE) && !readEntity.getType().equals(Entity.Type.PARTITION)) {
            return false;
        }
        if (readEntity.getParents() != null && readEntity.getParents().size() > 0) {
            for (ReadEntity parentEntity : readEntity.getParents()) {
                if (!parentEntity.getType().equals(Entity.Type.TABLE)) {
                    return false;
                }
            }
            return true;
        } else {
            return false;
        }
    }

}
