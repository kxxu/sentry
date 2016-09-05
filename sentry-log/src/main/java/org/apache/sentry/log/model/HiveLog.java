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
package org.apache.sentry.log.model;

import com.google.common.base.MoreObjects;
import com.google.common.base.Objects;

import javax.jdo.annotations.NotPersistent;
import javax.jdo.annotations.PersistenceCapable;
import java.sql.Timestamp;

/**
 * Created by kxxu on 2016/8/30.
 */
@PersistenceCapable
public class HiveLog {
    @NotPersistent
    private String queryId;
    private String tableName;
    private String project;
    private String user;
    private String server;
    private Timestamp createdTime;
    private String description;

    public HiveLog(String tableName, String project, String user, String description) {
        this.tableName = tableName;
        this.project = project;
        this.user = user;
        this.description = description;
    }

    public String getQueryId() {
        return queryId;
    }

    public HiveLog setQueryId(String queryId) {
        this.queryId = queryId;
        return this;
    }

    public String getTableName() {
        return tableName;
    }

    public HiveLog setTableName(String tableName) {
        this.tableName = tableName;
        return this;
    }

    public String getProject() {
        return project;
    }

    public HiveLog setProject(String project) {
        this.project = project;
        return this;
    }

    public String getUser() {
        return user;
    }

    public HiveLog setUser(String user) {
        this.user = user;
        return this;
    }

    public Timestamp getCreatedTime() {
        return createdTime;
    }

    public HiveLog setCreatedTime(Timestamp createdTime) {
        this.createdTime = createdTime;
        return this;
    }

    public String getDescription() {
        return description;
    }

    public HiveLog setDescription(String description) {
        this.description = description;
        return this;
    }

    public String getServer() {
        return server;
    }

    public HiveLog setServer(String server) {
        this.server = server;
        return this;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        HiveLog hiveLog = (HiveLog) o;
        return Objects.equal(tableName, hiveLog.tableName) &&
                Objects.equal(project, hiveLog.project);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(tableName, project);
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
                .add("queryId", queryId)
                .add("tableName", tableName)
                .add("project", project)
                .add("user", user)
                .add("server", server)
                .add("description", description)
                .toString();
    }
    public static class Builder{
        private String queryId;
        private String tableName;
        private String project;
        private String user;
        private String server;
        private String description;

        public Builder setQueryId(String queryId) {
            this.queryId = queryId;
            return this;
        }

        public Builder setTableName(String tableName) {
            this.tableName = tableName;
            return this;
        }

        public Builder setProject(String project) {
            this.project = project;
            return this;
        }

        public Builder setUser(String user) {
            this.user = user;
            return this;
        }

        public Builder setServer(String server) {
            this.server = server;
            return this;
        }

        public Builder setDescription(String description) {
            this.description = description;
            return this;
        }


        public HiveLog build() {
            Timestamp timestamp = new Timestamp(System.currentTimeMillis());
            HiveLog log = new HiveLog(tableName, project, user, server);
            log.setQueryId(queryId);
            log.setServer(server);
            log.setCreatedTime(timestamp);
            return log;
        }
    }
}
