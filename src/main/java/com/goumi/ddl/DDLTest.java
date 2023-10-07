package com.goumi.ddl;

import com.goumi.utils.HBaseConnection;

import java.io.IOException;

/**
 * @version 1.0
 * @auther GouMi
 */
public class DDLTest {
    public static void main(String[] args) throws IOException {
        //HBaseDDL.createNameSpace("ddl_namespace");

        //HBaseDDL.createTable("bigdata", "testcreatetab", "info");
        //HBaseDDL.modifyTable("bigdata", "testcreatetab", "info", 3);
        HBaseDDL.deleteTable("default", "createtbl");
        HBaseConnection.closeConnection();
    }
}
