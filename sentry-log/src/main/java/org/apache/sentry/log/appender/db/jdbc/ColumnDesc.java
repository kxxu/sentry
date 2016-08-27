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
package org.apache.sentry.log.appender.db.jdbc;

import com.google.common.base.Strings;
import org.apache.logging.log4j.core.config.Configuration;
import org.apache.logging.log4j.core.config.plugins.Plugin;
import org.apache.logging.log4j.core.config.plugins.PluginAttribute;
import org.apache.logging.log4j.core.config.plugins.PluginConfiguration;
import org.apache.logging.log4j.core.config.plugins.PluginFactory;
import org.apache.sentry.log.handler.DefaultHandler;
import org.apache.sentry.log.handler.TypeHandler;

import java.sql.PreparedStatement;
import java.sql.SQLException;

/**
 * Created by admin on 2016/8/24.
 */
@Plugin(
        name = "ColumnDesc",
        category = "Core",
        printObject = true
)
public class ColumnDesc {
    private static final TypeHandler DEFAUTLHANDLER = new DefaultHandler();
    private String columnName;
    private String property;
    private TypeHandler handler;

    public ColumnDesc(String columnName, String property, String handler) {
        this.columnName = columnName;
        this.property = property;
        if (Strings.isNullOrEmpty(handler)) {
            this.handler = DEFAUTLHANDLER;
        } else {
            try {
                Class<?> clazz = Class.forName(handler);
                this.handler = (TypeHandler) clazz.newInstance();
            } catch (ClassNotFoundException e) {
                e.printStackTrace();
            } catch (InstantiationException e) {
                e.printStackTrace();
            } catch (IllegalAccessException e) {
                e.printStackTrace();
            }
        }
    }

    public String getColumnName() {
        return columnName;
    }

    public ColumnDesc setColumnName(String columnName) {
        this.columnName = columnName;
        return this;
    }

    public void setStatementValue(PreparedStatement statement, int index, Object value) throws SQLException {
        handler.setNonNullParameter(statement, index, value);
    }

    public String getProperty() {
        return property;
    }

    public ColumnDesc setProperty(String property) {
        this.property = property;
        return this;
    }

    @PluginFactory
    public static ColumnDesc createColumnDesc(@PluginConfiguration Configuration configuration,
                                              @PluginAttribute("name") String name,
                                              @PluginAttribute("property") String property,
                                              @PluginAttribute("handler") String handler) {
        return new ColumnDesc(name, property, handler);
    }
}
