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

package org.apache.sentry.provider.common;

import java.io.IOException;
import java.net.URL;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.GroupMappingServiceProvider;
import org.apache.hadoop.security.Groups;
import org.apache.hadoop.security.ShellBasedUnixGroupsMapping;
import org.apache.sentry.core.common.Model;
import org.apache.sentry.core.common.service.GroupMappingService;
import org.apache.sentry.core.common.service.HadoopGroupMappingService;
import org.apache.sentry.policy.common.PolicyEngine;

import com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HadoopGroupResourceAuthorizationProvider extends
  ResourceAuthorizationProvider {

  // if set to true in the Configuration, constructs a new Group object
  // for the GroupMappingService rather than using Hadoop's static mapping.
  public static final String CONF_PREFIX = HadoopGroupResourceAuthorizationProvider.class.getName();
  public static final String USE_NEW_GROUPS = CONF_PREFIX + ".useNewGroups";
  private static final Logger LOGGER = LoggerFactory.getLogger(HadoopGroupResourceAuthorizationProvider.class);


  // resource parameter present so that other AuthorizationProviders (e.g.
  // LocalGroupResourceAuthorizationProvider) has the same constructor params.
  public HadoopGroupResourceAuthorizationProvider(String resource, PolicyEngine policy,
      Model model) throws IOException {
    this(new Configuration(), resource, policy, model);
  }

  public HadoopGroupResourceAuthorizationProvider(Configuration conf, String resource, //NOPMD
      PolicyEngine policy, Model model) throws IOException {
    this(policy, new HadoopGroupMappingService(getGroups(conf)), model);
  }

  @VisibleForTesting
  public HadoopGroupResourceAuthorizationProvider(PolicyEngine policy,
      GroupMappingService groupService, Model model) {
    super(policy, groupService, model);
  }

  private static Groups getGroups(Configuration conf) {
    LOGGER.info("get groups conf: {}", conf);
    LOGGER.info("group class name: {}", conf.get("hadoop.security.group.mapping"));
    LOGGER.info("group name: {}", conf.getClass("hadoop.security.group.mapping",
            ShellBasedUnixGroupsMapping.class, GroupMappingServiceProvider.class));
    URL url = Thread.currentThread().getContextClassLoader().getResource("core-site.xml");
    LOGGER.info("core site xml: {}", url);
    LOGGER.info("ldap url: {}", conf.get("hadoop.security.group.mapping.ldap.url"));
    LOGGER.info("ldap user: {}, password: {}", conf.get("hadoop.security.group.mapping.ldap.bind.user"),
            conf.get("hadoop.security.group.mapping.ldap.bind.password"));
    if (conf.getBoolean(USE_NEW_GROUPS, false)) {
      return new Groups(conf);
    } else {
      return Groups.getUserToGroupsMappingService(conf);
    }
  }
}
