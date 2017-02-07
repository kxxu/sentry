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
package com.iflytek.odeon;

import org.apache.hadoop.hive.ql.exec.UDF;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

/**
 * Created by Administrator on 2016/11/23.
 */
public class DateUdf extends UDF {

    public String evaluate(String date) {
        DateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
        try
        {
            return getMondayOfWeek(sdf.parse(date));
        }
        catch (ParseException e) {}
        return null;
    }

    public String getMondayOfWeek(Date date)
    {
        DateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
        Calendar cal = Calendar.getInstance();
        cal.setTime(date);

        cal.setFirstDayOfWeek(2);

        int d = cal.get(7);
        if (d == 1) {
            cal.add(5, cal.getFirstDayOfWeek() - 8);
        } else {
            cal.add(5, cal.getFirstDayOfWeek() - d);
        }
        String monday = sdf.format(cal.getTime());
        return monday;
    }
}
