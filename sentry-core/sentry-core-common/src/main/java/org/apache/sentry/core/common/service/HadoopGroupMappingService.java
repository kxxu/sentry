/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.sentry.core.common.service;

import java.io.IOException;
import java.net.URL;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import com.google.common.base.Throwables;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.GroupMappingServiceProvider;
import org.apache.hadoop.security.Groups;

import com.google.common.collect.Lists;
import org.apache.hadoop.security.LdapGroupsMapping;
import org.apache.hadoop.security.ShellBasedUnixGroupsMapping;
import org.apache.sentry.core.common.exception.SentryGroupNotFoundException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HadoopGroupMappingService implements GroupMappingService {
  private static final Logger LOGGER = LoggerFactory.getLogger(HadoopGroupMappingService.class);

  private static Configuration hadoopConf;
  private final Groups groups;
//  private final Configuration myconf;

  public HadoopGroupMappingService(Groups groups) {
    this.groups = groups;
  }

  public HadoopGroupMappingService(Configuration conf, String resource) {
    if (hadoopConf == null) {
      synchronized (HadoopGroupMappingService.class) {
        if (hadoopConf == null) {
          // clone the current config and add resource path
          hadoopConf = new Configuration();
          hadoopConf.addResource(conf);
          if (!StringUtils.isEmpty(resource)) {
            hadoopConf.addResource(resource);
          }
        }
      }
    }
    URL uri = Thread.currentThread().getContextClassLoader().getResource("/etc/hadoop/conf/core-site.xml");
    LOGGER.info("hadoop core site: ", uri);
    LOGGER.info("get hadoop config: {}", conf);
    LOGGER.info("group class name: {}", hadoopConf.get("hadoop.security.group.mapping"));
    LOGGER.info("group name: {}", hadoopConf.getClass("hadoop.security.group.mapping",
            ShellBasedUnixGroupsMapping.class, GroupMappingServiceProvider.class));
//    conf.setClass("hadoop.security.group.mapping", LdapGroupsMapping.class, GroupMappingServiceProvider.class);
    this.groups = Groups.getUserToGroupsMappingService(conf);
    LOGGER.info("ldap url: {}", hadoopConf.get("hadoop.security.group.mapping.ldap.url"));
    LOGGER.info("ldap user: {}, password: {}", hadoopConf.get("hadoop.security.group.mapping.ldap.bind.user"),
            hadoopConf.get("hadoop.security.group.mapping.ldap.bind.password"));

  }

  @Override
  public Set<String> getGroups(String user) {
    List<String> groupList = null;
    try {
      groupList = groups.getGroups(user);
    } catch (IOException e) {
      if (e.getMessage() != null && e.getMessage().contains("No groups found for user")) {
          LOGGER.error("sentry group service got IOException: {}", Throwables.getStackTraceAsString(e));
      } else {
          LOGGER.error("sentry got IOException, message: {}", e.getMessage());
          throw new SentryGroupNotFoundException("Unable to obtain groups for " + user, e);
      }
    }
    if (groupList == null) {
      groupList = Lists.newArrayList();
      LOGGER.warn("sentry unable to obtain groups for {}", user);
    }
    LOGGER.info("user: {}, sentry group: {}", user, groupList);
//    if (groupList == null || groupList.isEmpty()) {
//      throw new SentryGroupNotFoundException("Unable to obtain groups for " + user);
//    }
    return new HashSet<String>(groupList);
  }
}
