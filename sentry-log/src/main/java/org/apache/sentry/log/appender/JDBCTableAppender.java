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

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.logging.log4j.core.Filter;
import org.apache.logging.log4j.core.Layout;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.appender.AbstractAppender;
import org.apache.logging.log4j.core.appender.AppenderLoggingException;
import org.apache.logging.log4j.core.config.plugins.Plugin;
import org.apache.logging.log4j.core.config.plugins.PluginAttribute;
import org.apache.logging.log4j.core.config.plugins.PluginElement;
import org.apache.logging.log4j.core.config.plugins.PluginFactory;
import org.apache.sentry.log.appender.db.jdbc.ColumnDesc;
import org.apache.sentry.log.util.ConnectionPool;

import java.io.IOException;
import java.io.Serializable;
import java.sql.*;
import java.util.Map;

/**
 * Created by admin on 2016/8/24.
 */
@Plugin(
        name="JDBCTable",
        category = "Core",
        elementType = "appender",
        printObject = true
)
public class JDBCTableAppender extends AbstractAppender {
//    private static final Logger Log = LoggerFactory.getLogger("org.apache.sentry");
    private String sqlStr;
    private ColumnDesc[] columnConfig;
    private PreparedStatement statement;
    private String description;
    Connection con;
    private ObjectMapper objectMapper = new ObjectMapper();

    public JDBCTableAppender() {
        super("", null, null);
        String url="jdbc:mysql://172.16.154.120:3306/hueDB?user=root&password=iflytek";
        try {
            con = DriverManager.getConnection(url);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    protected JDBCTableAppender(String name, String tableName, ColumnDesc[] columnConfig, Filter filter, boolean ignoreException) {
        super(name, filter, null, ignoreException);
        this.columnConfig = columnConfig;
        sqlStr = parseSql(tableName);
        System.out.println("write sql str: " + sqlStr);
    }

    @Override
    public void append(LogEvent logEvent) {
        try {
            System.out.println("append log event: " +  logEvent);
            String event = logEvent.getMessage().getFormattedMessage();
            statement = ConnectionPool.getConnection().prepareStatement(sqlStr);
            Map<String, String> eventMap = objectMapper.readValue(event, Map.class);
            for (int i = 0; i < columnConfig.length; i++) {
                columnConfig[i].setStatementValue(statement, i + 1, eventMap.get(columnConfig[i].getProperty()));
            }
            statement.execute();
//            Log.info("statement execute: {}", statement);
        } catch (Exception ex) {
            throw new AppenderLoggingException("Failed to insert record for log event in JDBC manager: " + ex.getMessage(), ex);
        }
    }

    public void writeDB(String json) throws Exception {
        String sql = "insert into filebrowser_hive_table( `table_name`,project,project_owner,created_time,table_description ) values ( ?,?,?,?,? )";
        statement = con.prepareStatement(sql);
        Map<String, Object> eventMap = objectMapper.readValue(json, Map.class);

        statement.setString(1, eventMap.get("tableName").toString());
        statement.setString(2, eventMap.get("project").toString());
        statement.setString(3, eventMap.get("createUser").toString());
        statement.setString(5, eventMap.get("tableDescription").toString());
        statement.setTimestamp(4, new Timestamp((Long) eventMap.get("createdTime")));
        statement.execute();
        System.out.println("write " + json + " to db");
    }

    @PluginFactory
    public static JDBCTableAppender createAppender(@PluginAttribute("name") String name, @PluginAttribute("ignoreExceptions") String ignore,
                                                   @PluginElement("Filter") Filter filter, @PluginAttribute("tableName")String tableName,
                                                   @PluginElement("ColumnConfigs")ColumnDesc[] columnConfig) {
        System.out.println("create jdbc appender");
//        Log.info("create jdbc appender");
        return new JDBCTableAppender(name, tableName, columnConfig, filter, Boolean.getBoolean(ignore));
    }

    private String parseSql(String tableName) {
        StringBuilder columnPart = new StringBuilder();
        StringBuilder valuePart = new StringBuilder();
        for(int i = 0; i < columnConfig.length; i++) {
            columnPart.append(columnConfig[i].getColumnName());
            valuePart.append("?");
            if (i != columnConfig.length - 1) {
                columnPart.append(",");
                valuePart.append(",");
            }
        }
        return "insert into " + tableName + "( " + columnPart + " )" + " values ( " + valuePart + " )";
    }

}
