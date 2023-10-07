package com.goumi.dml;

import com.goumi.utils.HBaseConnection;

import java.io.IOException;

/**
 * @version 1.0
 * @auther GouMi
 */
public class DMLTest {
    public static void main(String[] args) throws IOException {
        //HBaseDML.putCell("bigdata", "student", "1003", "info", "name", "gouyunhua2");
        //HBaseDML.getCells("bigdata", "student", "1003", "info", "name");
        HBaseDML.scanRows("bigdata", "student", "1001", "1004");
        //HBaseDML.filterScan("bigdata", "student", "1001", "1004", "info", "name", "gouyunhua2");
        //HBaseDML.deleteColumn("bigdata", "student", "1003", "info", "name");
        HBaseConnection.closeConnection();
    }
}
