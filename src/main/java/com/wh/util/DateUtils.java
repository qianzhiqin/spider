package com.wh.util;

import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * Created by Administrator on 2017/10/21 0021.
 */
public class DateUtils {
    public static final String DATEFORMAT = "yyyy-MM-dd HH:mm:ss";
    public static String now() {
        SimpleDateFormat sf = new SimpleDateFormat(DATEFORMAT);
        return sf.format(new Date());
    }
}
