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
