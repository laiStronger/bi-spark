package com.youanmi.bi.demo.utils;


import java.io.IOException;
import java.util.Properties;

/**
 * @author sunxiaolong
 */
public class PropertyUtils {
    private PropertyUtils() {

    }

    private static Properties p = new Properties();

    static {
        try {
            p.load(PropertyUtils.class.getResourceAsStream("/bi.local.properties"));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private static String getProperty(String key) {
        return p.getProperty(key);
    }

    public static Integer getInteger(String key) {
        return Integer.valueOf(getProperty(key));
    }

    public static String getString(String key) {
        return p.getProperty(key);
    }
}
