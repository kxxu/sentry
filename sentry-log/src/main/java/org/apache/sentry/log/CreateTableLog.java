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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.MoreObjects;
import com.google.common.base.Throwables;

/**
 * Created by admin on 2016/8/25.
 */
public class CreateTableLog {
    private static final ObjectMapper objectMapper = new ObjectMapper();
    private final String tableName;
    private final String project;
    private final String createUser;
    private final String tableDescription;
    private final long createdTime;

    public CreateTableLog(String tableName, String project, String createUser, String tableDescription, long createdTime) {
        this.tableName = tableName;
        this.project = project;
        this.createUser = createUser;
        this.tableDescription = tableDescription;
        this.createdTime = createdTime;
    }

    public static ObjectMapper getObjectMapper() {
        return objectMapper;
    }

    public String getTableName() {
        return tableName;
    }

    public String getProject() {
        return project;
    }

    public String getCreateUser() {
        return createUser;
    }

    public String getTableDescription() {
        return tableDescription;
    }

    public long getCreatedTime() {
        return createdTime;
    }

    @Override
    public String toString() {
        try {
            return objectMapper.writeValueAsString(this);
        } catch (Exception ex) {
            System.out.println(MoreObjects.toStringHelper(this)
                    .add("tableName", tableName)
                    .add("project", project)
                    .add("createUser", createUser)
                    .add("tableDescription", tableDescription)
                    .add("createdTime", createdTime)
                    .add("exception", Throwables.getStackTraceAsString(ex))
                    .toString());
        }
        return null;
    }
    public static class Builder{
        private String tableName;
        private String project;
        private String createUser;
        private String tableDescription;

        public String getTableName() {
            return tableName;
        }

        public Builder setTableName(String tableName) {
            this.tableName = tableName;
            return this;
        }

        public String getProject() {
            return project;
        }

        public Builder setProject(String project) {
            this.project = project;
            return this;
        }

        public String getCreateUser() {
            return createUser;
        }

        public Builder setCreateUser(String createUser) {
            this.createUser = createUser;
            return this;
        }

        public String getTableDescription() {
            return tableDescription;
        }

        public Builder setTableDescription(String tableDescription) {
            this.tableDescription = tableDescription;
            return this;
        }

        public CreateTableLog build() {
            return new CreateTableLog(tableName, project, createUser, tableDescription, System.currentTimeMillis());
        }
    }
}
