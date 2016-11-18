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

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.UnresolvedLinkException;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.server.namenode.AclFeature;
import org.apache.hadoop.hdfs.server.namenode.AuthorizationProvider;
import org.apache.hadoop.hdfs.server.namenode.INode;
import org.apache.hadoop.security.AccessControlException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Map;
import java.util.Set;

/**
 * Created by kxxu on 2016/9/8.
 */
public class DirSentryAuthorizationProvider extends AuthorizationProvider implements Configurable {
    private static final Logger LOGGER = LoggerFactory.getLogger(DirSentryAuthorizationProvider.class);
    private static final short DEFAUTL_DIR_PERMISSION = new FsPermission("005").toShort();
    private static final short DIR_EXECUTE = new FsPermission("001").toShort();
    private boolean started;
    private Configuration conf;
    private AuthorizationProvider defaultAuthzProvider;
    @Override
    public void setConf(Configuration configuration) {
        this.conf = configuration;
    }

    @Override
    public Configuration getConf() {
        return conf;
    }

    @Override
    public void start() {
        if (started) {
            throw new IllegalStateException("Provider already started");
        }
        started = true;
        try {
            if (!conf.getBoolean(DFSConfigKeys.DFS_NAMENODE_ACLS_ENABLED_KEY, false)) {
                LOGGER.error("HDFS ACLs must be enabled");
                throw new RuntimeException("HDFS ACLs must be enabled");
            }
            defaultAuthzProvider = AuthorizationProvider.get();
            defaultAuthzProvider.start();
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
    }

    @Override
    public void checkPermission(String user, Set<String> groups, INodeAuthorizationInfo[] nodes, int snapshotId,
                                boolean doCheckOwner, FsAction ancestorAccess, FsAction parentAccess, FsAction access,
                                FsAction subAccess, boolean ignoreEmptyDir) throws AccessControlException, UnresolvedLinkException {
        INode[] inodes = (INode[]) nodes;
        int inodeIndex = inodes.length - 1;
        while (inodeIndex >= 0 && inodes[inodeIndex] == null) {
            inodeIndex--;
        }
        INodeAuthorizationInfo inodeInfo = nodes[inodeIndex];
        FsPermission inodePermission = defaultAuthzProvider.getFsPermission(inodeInfo, snapshotId);
        LOGGER.debug("check permission path: {}, permission: {}", nodes[inodeIndex].getFullPathName(),
                inodePermission);
        if (inodeInfo.isDirectory() && (inodePermission.toShort() & DIR_EXECUTE) != 0) {
            FsPermission newPermission = new FsPermission((short) (inodePermission.toShort() | DEFAUTL_DIR_PERMISSION));
            setPermission(inodeInfo, newPermission);
            try {
                LOGGER.debug("check dir permission: {}, new permission: {}", inodePermission, newPermission);
                defaultAuthzProvider.checkPermission(user, groups, nodes, snapshotId, doCheckOwner, ancestorAccess,
                        parentAccess, access, subAccess, ignoreEmptyDir);
            } finally {
                setPermission(inodeInfo, inodePermission);
            }
        } else {
            defaultAuthzProvider.checkPermission(user, groups, nodes, snapshotId, doCheckOwner, ancestorAccess,
                    parentAccess,access, subAccess, ignoreEmptyDir);
        }
    }

    @Override
    public void setSnaphottableDirs(Map<INodeAuthorizationInfo, Integer>
                                            snapshotableDirs) {
        defaultAuthzProvider.setSnaphottableDirs(snapshotableDirs);
    }

    @Override
    public void addSnapshottable(INodeAuthorizationInfo dir) {
        defaultAuthzProvider.addSnapshottable(dir);
    }

    @Override
    public void removeSnapshottable(INodeAuthorizationInfo dir) {
        defaultAuthzProvider.removeSnapshottable(dir);
    }

    @Override
    public void createSnapshot(INodeAuthorizationInfo dir, int snapshotId)
            throws IOException {
        defaultAuthzProvider.createSnapshot(dir, snapshotId);
    }

    @Override
    public void removeSnapshot(INodeAuthorizationInfo dir, int snapshotId)
            throws IOException {
        defaultAuthzProvider.removeSnapshot(dir, snapshotId);
    }

    @Override
    public void setUser(INodeAuthorizationInfo iNodeAuthorizationInfo, String s) {
        defaultAuthzProvider.setUser(iNodeAuthorizationInfo, s);
    }

    @Override
    public String getUser(INodeAuthorizationInfo iNodeAuthorizationInfo, int i) {
        return defaultAuthzProvider.getUser(iNodeAuthorizationInfo, i);
    }

    @Override
    public void setGroup(INodeAuthorizationInfo iNodeAuthorizationInfo, String s) {
        defaultAuthzProvider.setGroup(iNodeAuthorizationInfo, s);
    }

    @Override
    public String getGroup(INodeAuthorizationInfo iNodeAuthorizationInfo, int i) {
        return defaultAuthzProvider.getGroup(iNodeAuthorizationInfo, i);
    }

    @Override
    public void setPermission(INodeAuthorizationInfo iNodeAuthorizationInfo, FsPermission fsPermission) {
        defaultAuthzProvider.setPermission(iNodeAuthorizationInfo, fsPermission);
    }

    @Override
    public FsPermission getFsPermission(INodeAuthorizationInfo iNodeAuthorizationInfo, int i) {
        return defaultAuthzProvider.getFsPermission(iNodeAuthorizationInfo, i);
    }

    @Override
    public AclFeature getAclFeature(INodeAuthorizationInfo iNodeAuthorizationInfo, int i) {
        return defaultAuthzProvider.getAclFeature(iNodeAuthorizationInfo, i);
    }

    @Override
    public void removeAclFeature(INodeAuthorizationInfo iNodeAuthorizationInfo) {
        defaultAuthzProvider.removeAclFeature(iNodeAuthorizationInfo);
    }

    @Override
    public void addAclFeature(INodeAuthorizationInfo iNodeAuthorizationInfo, AclFeature aclFeature) {
        defaultAuthzProvider.addAclFeature(iNodeAuthorizationInfo, aclFeature);
    }

    @Override
    public void stop() {
        defaultAuthzProvider.stop();
        defaultAuthzProvider = null;
    }
}
