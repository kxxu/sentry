package com.iflytek.odeon.service;

import org.apache.hadoop.conf.Configuration;
import org.apache.sentry.provider.db.service.persistent.SentryStore;
import org.junit.Before;
import org.junit.Test;

import java.util.Set;

/**
 * Created by Administrator on 2016/11/4.
 */
public class SentryStoreTest {

    @Before
    public void setup() {
        Configuration.addDefaultResource("sentry-site.xml");
    }

    @Test
    public void test() {
        Configuration configuration = new Configuration();
        try {
            SentryStore sentryStore = new SentryStore(configuration);
            Set<String> names = sentryStore.getAllRoleNames();
            System.out.println(names);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
