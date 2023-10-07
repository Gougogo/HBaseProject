package com.goumi.guliweibo.constants;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;

/**
 * @version 1.0
 * @auther GouMi
 */
public class Constants {

    public static Configuration CONFIGURATION = HBaseConfiguration.create();

    public static String NAMESPACE = "weibo";

    public static String CONTENT_TABLE = "weibo:content";
    public static String CONTENT_TABLE_CF = "info";
    public static int CONTENT_TABLE_VERSIONS = 1;

    public static String RELATION_TABLE = "weibo:relation";
    public static String RELATION_TABLE_CF1 = "attends";
    public static String RELATION_TABLE_CF2 = "fans";
    public static int RELATION_TABLE_VERSIONS = 1;

    public static String INBOX_TABLE = "weibo:inbox";
    public static String INBOX_TABLE_CF = "info";
    public static int INBOX_TABLE_VERSIONS = 2;

}
