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

/**
 * Created by admin on 2016/8/24.
 */
public class AuditLog {
    private static final Logger AuditLogger = LoggerFactory.getLogger("audit");
    private static final Logger HiveJdbcLogger = LoggerFactory.getLogger("hivelog");
    private static final Logger Log = LoggerFactory.getLogger(AuditLog.class);
    private static final String EntitySplitter= ";";
    private AuditLog(){}

    public static void logAuditEvent(String serverName, HiveSemanticAnalyzerHookContext context, HiveOperation operation) {
        String queryId = context.getConf().get(HiveConf.ConfVars.HIVEQUERYID.toString());

        Log.debug("query id:{}, user:{}, operation: {}, input: {}, output: {}, command: {}", new Object[]{
                queryId,context.getUserName(), operation, context.getInputs(),
                context.getOutputs(), context.getCommand()});
        StringBuilder stringBuilder = new StringBuilder();
        for (ReadEntity readEntity : context.getInputs()) {
            if (isChildTabForView(readEntity) || isDummyEntity(readEntity)) {
                continue;
            }
            stringBuilder.append(readEntity.getDatabase());
            if (readEntity.getTable() != null) {
                stringBuilder.append(".");
                stringBuilder.append(readEntity.getTable().getTableName());
            }
            stringBuilder.append(EntitySplitter);
        }
        String readEntityStr = "";
        if (stringBuilder.length() > 0) {
            readEntityStr = stringBuilder.deleteCharAt(stringBuilder.length() - 1).toString();
            stringBuilder.delete(0, stringBuilder.length());
        }
        for (WriteEntity writeEntity : context.getOutputs()) {
            if (writeEntity.getTable() != null) {
                stringBuilder.append(writeEntity.getTable().getDbName());
                stringBuilder.append(".");
                stringBuilder.append(writeEntity.getTable().getTableName());
                stringBuilder.append(";");
            }
        }
        String writeEntityStr = "";
        if (stringBuilder.length() > 0) {
            writeEntityStr = stringBuilder.deleteCharAt(stringBuilder.length() - 1).toString();
        }
        AuditLogger.info("query_id={}|user={}|operation={}|src={}|dst={}|command={}", new Object[]{
                queryId, context.getUserName(), operation.getOperationName(),
                readEntityStr, writeEntityStr, context.getCommand()});
    }

    public static void logAuditLog(HiveOperationType hiveOpType, List<HivePrivilegeObject> inputHObjs,
                                   List<HivePrivilegeObject> outputHObjs, HiveAuthzContext context,
                                   String serverName, String user) {
//        String queryId = context.getConf().get(HiveConf.ConfVars.HIVEQUERYID.toString());
        if (hiveOpType == HiveOperationType.CREATETABLE || hiveOpType == HiveOperationType.CREATETABLE_AS_SELECT) {
            CreateTableLog.Builder builder = new CreateTableLog.Builder();
            Log.debug("create table, input: {}, output: {}", new Object[]{inputHObjs,
                    outputHObjs});
            if (outputHObjs.size() > 0) {
                HivePrivilegeObject table = outputHObjs.get(outputHObjs.size() - 1);
                builder.setQueryId("").setTableName(table.getObjectName())
                        .setCreateUser(user)
                        .setProject(table.getDbname()).setTableDescription(context.getCommandString());
                HiveJdbcLogger.info(builder.build().toString());
            }
        } else if (hiveOpType == HiveOperationType.DROPTABLE){
            CreateTableLog.Builder builder = new CreateTableLog.Builder();
            Log.debug("drop table, input: {}, output: {}", new Object[]{
                    inputHObjs, outputHObjs});
            if (inputHObjs.size() > 0) {
                HivePrivilegeObject table = inputHObjs.get(inputHObjs.size() - 1);
                builder.setQueryId("").setTableName(table.getObjectName())
                        .setCreateUser(user)
                        .setProject(table.getDbname()).setTableDescription("drop");
                HiveJdbcLogger.info(builder.build().toString());
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
