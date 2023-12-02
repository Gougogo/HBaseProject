package com.goumi.ddl;

/**
 * @version 1.0
 * @auther GouMi
 */

import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

public class CreateTable {
    public static void main(String[] args) throws IOException {
        // HBase 配置
        org.apache.hadoop.conf.Configuration config = HBaseConfiguration.create();

        // HBase 连接
        try (Connection connection = ConnectionFactory.createConnection(config);
             Admin admin = connection.getAdmin()) {

            // 表名
            TableName tableName = TableName.valueOf("your_table_name");

            // 指定分区范围
            byte[] startKey = Bytes.toBytes("start_key");
            byte[] endKey = Bytes.toBytes("end_key");
            int numRegions = 5; // 你想要的分区数

            // 创建表
            createTableWithRegions(admin, tableName, startKey, endKey, numRegions);
            System.out.println("Table created successfully.");
        }
    }

    private static void createTableWithRegions(Admin admin, TableName tableName, byte[] startKey, byte[] endKey, int numRegions) throws IOException {
        // 计算分区的步长
        byte[][] splitKeys = Bytes.split(startKey, endKey, numRegions - 1);

        // 创建表描述符
        HTableDescriptor tableDescriptor = new HTableDescriptor(tableName);

        // 列族
        HColumnDescriptor columnFamily = new HColumnDescriptor("your_column_family");
        tableDescriptor.addFamily(columnFamily);

        // 添加分区键
        for (byte[] splitKey : splitKeys) {
            admin.createTable(tableDescriptor, startKey, splitKey, numRegions);
            startKey = Bytes.add(splitKey, new byte[]{1}); // 移到下一个分区的起始键
        }
    }
}

