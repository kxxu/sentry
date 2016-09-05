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
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.appender.AbstractAppender;
import org.apache.logging.log4j.core.appender.AppenderLoggingException;
import org.apache.logging.log4j.core.config.plugins.Plugin;
import org.apache.logging.log4j.core.config.plugins.PluginAttribute;
import org.apache.logging.log4j.core.config.plugins.PluginElement;
import org.apache.logging.log4j.core.config.plugins.PluginFactory;
import org.apache.sentry.log.appender.db.jdbc.ColumnDesc;
import org.apache.sentry.log.util.ConnectionPool;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.Timestamp;
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
    private String deleteSql = "delete from filebrowser_hive_table where table_name=? and project=?";



    protected JDBCTableAppender(String name, String tableName, ColumnDesc[] columnConfig, Filter filter, boolean ignoreException) {
        super(name, filter, null, ignoreException);
        this.columnConfig = columnConfig;
        sqlStr = parseSql(tableName);
    }

    @Override
    public void append(LogEvent logEvent) {
        try {
            logEvent.getMessage().getFormattedMessage();
            String event = logEvent.getMessage().getFormattedMessage();
                        LOGGER.info("get event: {}", event);
            Map<String, String> eventMap = objectMapper.readValue(event, Map.class);
            try (Connection connection = ConnectionPool.getConnection()){
                if ("drop".equalsIgnoreCase(eventMap.get("tableDescription"))) {
                    statement = connection.prepareStatement(deleteSql);
                    statement.setString(1, eventMap.get("tableName"));
                    statement.setString(2, eventMap.get("project"));
                } else {
                    statement = connection.prepareStatement(sqlStr);
                    for (int i = 0; i < columnConfig.length; i++) {
                        columnConfig[i].setStatementValue(statement, i + 1, eventMap.get(columnConfig[i].getProperty()));
                    }
                }
                statement.execute();
            }
        } catch (Exception ex) {
            throw new AppenderLoggingException("Failed to insert record for log event in JDBC manager: " + ex.getMessage(), ex);
        }
    }

    @PluginFactory
    public static JDBCTableAppender createAppender(@PluginAttribute("name") String name, @PluginAttribute("ignoreExceptions") String ignore,
                                                   @PluginElement("Filter") Filter filter, @PluginAttribute("tableName")String tableName,
                                                   @PluginElement("ColumnConfigs")ColumnDesc[] columnConfig) {
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
