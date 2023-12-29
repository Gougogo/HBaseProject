package com.goumi.utils;

import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;

import java.io.IOException;

/**
 * @version 1.0
 * @auther GouMi
 */
public class HBaseConnection {
    public static Connection connection;
    static{
        try {
            connection = ConnectionFactory.createConnection();
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    public static void closeConnection() throws IOException {
        if (connection != null)
            connection.close();
    }
}
