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
package org.apache.sentry.log.appender;

import com.google.common.base.Throwables;
import org.apache.logging.log4j.core.Filter;
import org.apache.logging.log4j.core.Layout;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.appender.AbstractAppender;
import org.apache.logging.log4j.core.appender.AppenderLoggingException;
import org.apache.logging.log4j.core.config.plugins.Plugin;
import org.apache.logging.log4j.core.config.plugins.PluginAttribute;
import org.apache.logging.log4j.core.config.plugins.PluginElement;
import org.apache.logging.log4j.core.config.plugins.PluginFactory;
import org.apache.sentry.log.constants.JDOConstants;
import org.apache.sentry.log.model.HiveLog;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.jdo.*;
import java.io.Serializable;
import java.util.Properties;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Created by kxxu on 2016/9/2.
 */
@Plugin(
        name="SentryTableLog",
        category = "Core",
        elementType = "appender",
        printObject = true
)
public class HiveLogAppender extends AbstractAppender {
    private static final Logger Log = LoggerFactory.getLogger(HiveLogAppender.class);
    private PersistenceManagerFactory pmf;
    private Properties props;
    private final ReadWriteLock rwLock = new ReentrantReadWriteLock();
    private final Lock readLock = rwLock.readLock();

    protected HiveLogAppender(String name, String url, String username, String password, String ignore, Filter filter) {
        super(name, filter, null, Boolean.parseBoolean(ignore));
        props = new Properties();
        props.putAll(JDOConstants.HIVELOG_STORE_DEFAULTS);
        props.setProperty("javax.jdo.option.ConnectionURL", url);
        props.setProperty("javax.jdo.option.ConnectionUserName", username);
        props.setProperty("javax.jdo.option.ConnectionPassword", password);
//        props.setProperty("javax.jdo.option.ConnectionDriverName", "com.mysql.jdbc.Driver");
        pmf = JDOHelper.getPersistenceManagerFactory(props);
        LOGGER.info("hive log appender init, name: {}, url: {}", name, url);
    }

    @Override
    public void append(LogEvent logEvent) {
        readLock.lock();
        try {
            Object[] args = logEvent.getMessage().getParameters();
            Log.info("get args: {}", args);
            if (args != null && args.length != 1) {
                Log.warn("arguments is not 1, message: " + logEvent.getMessage().getFormattedMessage());
                return;
            }
            HiveLog hiveLog = (HiveLog) args[0];
            switch (hiveLog.getOperation()) {
                case "create":
                    createLog(hiveLog);
                    break;
                case "drop":
                    deleteLog(hiveLog);
                    break;
                default:
                    Log.warn("not a valid log type, message: " + logEvent.getMessage().getFormattedMessage());
                    break;
            }
        } catch (Exception ex) {
            LOGGER.error("execute sql error: {}", Throwables.getStackTraceAsString(ex) );
            if (!ignoreExceptions()) {
                throw new AppenderLoggingException(ex);
            }
        } finally {
            readLock.unlock();
        }

    }

    void createLog(HiveLog log) {
        PersistenceManager pm = pmf.getPersistenceManager();
        Transaction currentTransaction = pm.currentTransaction();
        currentTransaction.begin();
        pm.makePersistent(log);
        pm.currentTransaction().commit();
        pm.close();
    }

    void deleteLog(HiveLog log) {
        PersistenceManager pm = pmf.getPersistenceManager();
        Transaction currentTransaction = pm.currentTransaction();
        currentTransaction.begin();
        Query query = pm.newQuery(HiveLog.class);
        query.setFilter("this.project == t && this.tableName == y && this.server==s");
        query.declareParameters("java.lang.String t,java.lang.String y, java.lang.String s");
        query.setUnique(true);
        Object value = query.execute(log.getProject(), log.getTableName(), log.getServer());
        LOGGER.info("jdo query result: {}", value);
        pm.deletePersistent(value);
        pm.currentTransaction().commit();
        pm.close();
    }

    @PluginFactory
    public static HiveLogAppender createAppender(@PluginAttribute("name") String name,
                                                 @PluginAttribute("url") String url,
                                                 @PluginAttribute("username") String username,
                                                 @PluginAttribute("password") String password,
                                                 @PluginAttribute("ignoreExceptions") String ignore,
                                                 @PluginElement("Filter") Filter filter) {
        return new HiveLogAppender(name, url, username, password, ignore, filter);
    }
}
