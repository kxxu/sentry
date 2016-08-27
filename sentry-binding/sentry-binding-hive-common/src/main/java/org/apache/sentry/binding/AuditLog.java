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

import org.apache.hadoop.hive.ql.hooks.Entity;
import org.apache.hadoop.hive.ql.hooks.ReadEntity;
import org.apache.hadoop.hive.ql.hooks.WriteEntity;
import org.apache.hadoop.hive.ql.parse.HiveSemanticAnalyzerHookContext;
import org.apache.hadoop.hive.ql.plan.HiveOperation;
import org.apache.sentry.log.CreateTableLog;
import org.apache.sentry.log.appender.JDBCTableAppender;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by admin on 2016/8/24.
 */
public class AuditLog {
    private static final Logger AuditLogger = LoggerFactory.getLogger("audit");
    private static final Logger HiveJdbcLogger = LoggerFactory.getLogger("jdbc");
    private static final Logger Log = LoggerFactory.getLogger(AuditLog.class);
    private static final JDBCTableAppender appender = new JDBCTableAppender();
    private static final String EntitySplitter= ";";
    private AuditLog(){}

    public static void logAuditEvent(HiveSemanticAnalyzerHookContext context, HiveOperation operation) {
        Log.debug("user:{}, operation: {}, input: {}, output: {}, command: {}", new Object[]{context.getUserName(), operation, context.getInputs(),
                context.getOutputs(), context.getCommand()});
        StringBuilder stringBuilder = new StringBuilder();
        for (ReadEntity readEntity : context.getInputs()) {
            if (isChildTabForView(readEntity) || isDummyEntity(readEntity)) {
                continue;
            }
            stringBuilder.append(readEntity.getDatabase());
            stringBuilder.append(".");
            stringBuilder.append(readEntity.getTable().getTableName());
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
        AuditLogger.info("user={}|operation={}|src={}|dst={}|command={}", new Object[]{context.getUserName(), operation.getOperationName(),
                readEntityStr, writeEntityStr, context.getCommand()});
        if (HiveOperation.CREATETABLE == operation || HiveOperation.CREATETABLE_AS_SELECT == operation) {
            String[] objSplit = writeEntityStr.split("\\.");
            CreateTableLog.Builder builder = new CreateTableLog.Builder();
            builder.setCreateUser(context.getUserName()).setProject(objSplit[0])
                    .setTableName(objSplit[1]).setTableDescription("post hook");
            String tableLog = builder.build().toString();
//            AuditLogger.info("create user table: {}", builder.build().toString());
            try {
                appender.writeDB(tableLog);
            } catch (Exception e) {
                e.printStackTrace();
            }
//            HiveJdbcLogger.info(tableLog);
        }
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
