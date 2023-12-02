package com.goumi.guliweibo.test;

import com.goumi.ddl.HBaseDDL;
import com.goumi.guliweibo.dao.HBaseDao;

import java.io.IOException;

/**
 * @version 1.0
 * @auther GouMi
 */
public class test {
    public static void main(String[] args) throws IOException {
        //HBaseDDL.createNameSpace("weibo");

        //HBaseDDL.createTable("weibo", "relation", 1, "attends","fans");
        //HBaseDDL.createTable("weibo", "content", 1, "info");

        //HBaseDDL.addColFamilyOfTable("weibo", "relation", "fans");
        //HBaseDDL.createTable("weibo", "inbox", 2, "info");
        //HBaseDDL.deleteTable("weibo", "relation");
        //HBaseDao.publishWeiBo("1004", "second msg send.");


        //HBaseDao.addAttends("1002", "1004", "1005");
        //HBaseDao.getAttendsContent("1002");
    }
}