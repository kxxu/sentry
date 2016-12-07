/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.sentry.binding.hive.v2.metastore;

import com.google.common.base.Strings;
import com.google.common.base.Throwables;
import com.google.common.collect.Lists;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.AclEntry;
import org.apache.hadoop.fs.permission.AclEntryScope;
import org.apache.hadoop.fs.permission.AclStatus;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.PrincipalType;
import org.apache.hadoop.hive.metastore.events.AlterTableEvent;
import org.apache.hadoop.hive.metastore.events.CreateDatabaseEvent;
import org.apache.hadoop.hive.metastore.events.CreateTableEvent;
import org.apache.hadoop.hive.metastore.events.DropTableEvent;
import org.apache.hadoop.hive.shims.Utils;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.sentry.binding.metastore.SentryMetastorePostEventListenerBase;
import org.apache.sentry.log.model.HiveLog;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.StringWriter;
import java.net.URI;
import java.util.List;

/**
 * Created by Administrator on 2016/10/31.
 */
public class SentryMetastoreLogEventListener extends SentryMetastorePostEventListenerBase {
    private static final Logger EventLog = LoggerFactory.getLogger("tablelog");
    private static final Logger LOGGER = LoggerFactory.getLogger(SentryMetastoreLogEventListener.class);
    private FileSystem fileSystem;
    private FsPermission permission = new FsPermission("771");


    public SentryMetastoreLogEventListener(Configuration config) {
        super(config);
        try {
            Configuration configuration = new Configuration();
            String defaultFs = configuration.get(FileSystem.FS_DEFAULT_NAME_KEY);
            LOGGER.info("get default fs: {}", defaultFs);
            fileSystem = FileSystem.get(new URI(defaultFs), configuration, "hdfs");
        } catch (Exception e) {
            throw new IllegalStateException("can not get hadoop filesystem configuration");
        }
    }

    @Override
    public void onCreateDatabase(CreateDatabaseEvent dbEvent) throws MetaException {
        if (!dbEvent.getStatus()) {
            LOGGER.warn("[meta post event]create database {} failed", dbEvent.getDatabase().getName());
            return;
        }
        try {
            LOGGER.info("[create database]db name: {}, owner: {}, owner type: {}, uri: {}",
                    new Object[]{dbEvent.getDatabase().getName(),
                            getUserName(dbEvent.getDatabase().getOwnerName()),
                            dbEvent.getDatabase().getOwnerType(),
                            dbEvent.getDatabase().getLocationUri()});
            modifyPermission(dbEvent.getDatabase().getLocationUri(),
                    getUserName(dbEvent.getDatabase().getOwnerName()),
                    dbEvent.getDatabase().getName());
        } catch (Exception e) {
            LOGGER.error("[create database]modify database uri({}) permission error {}", dbEvent.getDatabase().getLocationUri(),
                    Throwables.getStackTraceAsString(e));
            throw new MetaException("[create database]modify permission error " + e.getMessage());
        }
    }

    @Override
    public void onCreateTable(CreateTableEvent tableEvent) throws MetaException {
        if (!tableEvent.getStatus()) {
            LOGGER.info("[meta post event]create table {} error",
                    tableEvent.getTable().getDbName() + "." + tableEvent.getTable().getTableName());
            return;
        }


        String userName = getUserName(tableEvent.getTable().getOwner());
        HiveLog.Builder builder = new HiveLog.Builder();
        builder.setOperation("create")
                .setServer(server.getName())
                .setProject(tableEvent.getTable().getDbName())
                .setTableName(tableEvent.getTable().getTableName())
                .setUser(userName);
        HiveLog hiveLog = builder.build();
        LOGGER.info("{}", hiveLog);
        EventLog.info("{}", hiveLog);
        if (!Strings.isNullOrEmpty(tableEvent.getTable().getSd().getLocation())) {
            try {

                modifyPermission(tableEvent.getTable().getSd().getLocation(), userName,
                        tableEvent.getTable().getDbName());
            } catch (Exception e) {
                LOGGER.error("[create table]modify table uri({}) failed {}",
                        tableEvent.getTable().getSd().getLocation(),
                        Throwables.getStackTraceAsString(e));
                throw new MetaException("[create table]modify permission error " + e.getMessage());
            }
        }
    }

    @Override
    public void onDropTable(DropTableEvent tableEvent) throws MetaException {
        if (!tableEvent.getStatus()) {
            LOGGER.info("[meta post event]drop table {} error",
                    tableEvent.getTable().getDbName() + "." + tableEvent.getTable().getTableName());
            return;
        }
        HiveLog.Builder builder = new HiveLog.Builder();
        builder.setOperation("drop")
                .setServer(server.getName())
                .setProject(tableEvent.getTable().getDbName())
                .setTableName(tableEvent.getTable().getTableName())
                .setUser(getUserName(getUserName(tableEvent.getTable().getOwner())));
        HiveLog hiveLog = builder.build();
        LOGGER.info("{}", hiveLog);
        EventLog.info("{}", hiveLog);

    }

    @Override
    public void onAlterTable(AlterTableEvent tableEvent) throws MetaException {
        if (!tableEvent.getStatus()) {
            LOGGER.warn("[meta post event]alter table error, old table: {}, new table: {}",
                    tableEvent.getOldTable().getDbName() + "." + tableEvent.getOldTable().getTableName(),
                    tableEvent.getNewTable().getDbName() + "." + tableEvent.getNewTable().getTableName());
            return;
        }
        String userName = getUserName(tableEvent.getNewTable().getOwner());
        HiveLog.Builder builder = new HiveLog.Builder();
        builder.setOperation("alter")
                .setServer(server.getName())
                .setProject(tableEvent.getNewTable().getDbName())
                .setOldTableName(tableEvent.getOldTable().getTableName())
                .setTableName(tableEvent.getNewTable().getTableName())
                .setUser(userName);
        HiveLog hiveLog = builder.build();
        LOGGER.info("{}", hiveLog);
        EventLog.info("{}", hiveLog);
        try {
            if (!Strings.isNullOrEmpty(tableEvent.getNewTable().getSd().getLocation())) {
                modifyPermission(tableEvent.getNewTable().getSd().getLocation(), userName,
                        tableEvent.getNewTable().getDbName());
//                moveAclFeature(tableEvent);
            }
        } catch (Exception e) {
            LOGGER.error(String.format("[alter table]modify new table permission error, new table: %s.%s, uri: %s, error: %s",
                    tableEvent.getNewTable().getDbName(), tableEvent.getNewTable().getTableName(),
                    tableEvent.getNewTable().getSd().getLocation(),
                    Throwables.getStackTraceAsString(e)));
            throw new MetaException("[alter table]modify permission error " + e.getMessage());
        }
    }

    //修改表的组和用户
    void modifyPermission(String uri, String user, String group) throws IOException {
        if (fileSystem != null && !Strings.isNullOrEmpty(uri)
                && !Strings.isNullOrEmpty(user) && !Strings.isNullOrEmpty(group)) {
            Path path = new Path(uri);
            LOGGER.info("[modify permission]path: {}, owner: {}, group: {}",
                    new Object[]{path, user, group});
            modifyPermission(path, user, group);
        } else {
            LOGGER.error("[modify permission]argument invalid,uri: {}, user: {}, group: {}",
                    new Object[]{uri, user, group});
        }
    }

    //迁移表的acl
    void moveAclFeature(AlterTableEvent tableEvent) throws IOException {
        if (tableEvent.getOldTable() != null && tableEvent.getNewTable() != null) {
            String oldLocation = tableEvent.getOldTable().getSd().getLocation();
            String newLocation = tableEvent.getNewTable().getSd().getLocation();
            if (!Strings.isNullOrEmpty(oldLocation) && !Strings.isNullOrEmpty(newLocation) &&
                    !oldLocation.equals(newLocation)) {
                Path oldPath = new Path(oldLocation);
                AclStatus oldStatus = fileSystem.getAclStatus(oldPath);
                if (oldStatus != null) {
                    List<AclEntry> aclEntryList = Lists.newArrayList();
                    for (AclEntry aclEntry : oldStatus.getEntries()) {
                        if (aclEntry.getScope() == AclEntryScope.ACCESS) {
                            aclEntryList.add(aclEntry);
                        }
                    }
                    if (!aclEntryList.isEmpty()) {
                        fileSystem.setAcl(new Path(newLocation), aclEntryList);
                        fileSystem.removeAclEntries(oldPath, aclEntryList);
                    }
                }
            }
        }
    }

    void modifyPermission(Path path, String user, String group) throws IOException {
        if (path == null) {
            return;
        }
        FileStatus pathStatus = fileSystem.getFileStatus(path);
        if (pathStatus != null) {
            fileSystem.setOwner(path, user, group);
            fileSystem.setPermission(path, permission);
        } else {
            return;
        }
        if (pathStatus.isDirectory()) {
            FileStatus[] fileStatuses = fileSystem.listStatus(path);
            if (fileStatuses == null || fileStatuses.length == 0) {
                return;
            } else {
                for (FileStatus item : fileStatuses) {
                    modifyPermission(item.getPath(), user, group);
                }
            }
        }

    }

    String getUserName(PrincipalType principalType, String name) throws MetaException {
        if (principalType == PrincipalType.GROUP) {
            return getUserName(null);
        } else {
            return getUserName(name);
        }
    }


    String getUserName(String userName) throws MetaException {
        if (Strings.isNullOrEmpty(userName)) {
            try {
                UserGroupInformation ugi = Utils.getUGI();
                String value = ugi.getUserName();
                if (!Strings.isNullOrEmpty(value)) {
                    userName = value;
                } else {
                    LOGGER.error("[ugi] can not get user name");
                }
            } catch (Exception ex) {
                LOGGER.error("[get ugi info]get ugi error {}", Throwables.getStackTraceAsString(ex));
            }
        }
        if (Strings.isNullOrEmpty(userName)) {
            LOGGER.warn("[user name]user name is null");
        }
        return userName;
    }


}
