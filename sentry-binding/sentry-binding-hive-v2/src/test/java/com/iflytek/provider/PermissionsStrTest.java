package com.iflytek.provider;

import com.google.common.collect.Lists;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.ql.plan.HiveOperation;
import org.apache.sentry.binding.hive.authz.HiveAuthzPrivileges;
import org.apache.sentry.binding.hive.v2.HiveAuthzPrivilegesMapV2;
import org.apache.sentry.core.common.ActiveRoleSet;
import org.apache.sentry.core.common.Model;
import org.apache.sentry.core.common.Subject;
import org.apache.sentry.core.common.service.GroupMappingService;
import org.apache.sentry.core.model.db.*;
import org.apache.sentry.policy.common.PolicyEngine;
import org.apache.sentry.provider.common.AuthorizationProvider;
import org.apache.sentry.provider.common.HadoopGroupResourceAuthorizationProvider;
import org.apache.sentry.provider.db.service.thrift.SentryPolicyServiceClient;
import org.apache.sentry.provider.db.service.thrift.SentryPolicyServiceClientDefaultImpl;
import org.apache.sentry.service.thrift.PoolClientInvocationHandler;
import org.apache.sentry.service.thrift.SentryServiceClientFactory;
import org.junit.Before;
import org.junit.Test;

import java.lang.reflect.Proxy;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;

import static org.mockito.Mockito.mock;

/**
 * Created by admin on 2016/8/9.
 */
public class PermissionsStrTest {
    AuthorizationProvider provider;

    @Before
    public void setup() {
        PolicyEngine engine = mock(PolicyEngine.class);
        GroupMappingService groupMappingService = mock(GroupMappingService.class);
        Model model = mock(Model.class);
        provider = new HadoopGroupResourceAuthorizationProvider(engine, groupMappingService, model);
    }


    @Test
    public void testPermissionStr() {
        Subject subject = new Subject("kxxu");
        HiveAuthzPrivileges privileges = HiveAuthzPrivilegesMapV2.getHiveAuthzPrivileges(HiveOperation.CREATETABLE);
        List<DBModelAuthorizable> list = Lists.newArrayList();
        list.add(new Server("server1"));
        list.add(new Database("default"));
        list.add(new Table("test"));
        for (Map.Entry<DBModelAuthorizable.AuthorizableType, EnumSet<DBModelAction>> entry : privileges.getInputPrivileges().entrySet()) {
            provider.hasAccess(subject, list, entry.getValue(), mock(ActiveRoleSet.class));
        }
    }

    @Test
    public void testCreatPoxy() throws Exception {
        Configuration conf = new Configuration();
        SentryPolicyServiceClient client = (SentryPolicyServiceClient) Proxy
                .newProxyInstance(SentryPolicyServiceClientDefaultImpl.class.getClassLoader(),
                        SentryPolicyServiceClientDefaultImpl.class.getInterfaces(),
                        new PoolClientInvocationHandler(conf));
        System.out.println(client);

    }
}
