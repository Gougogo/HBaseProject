package com.goumi.dml;

import com.goumi.utils.HBaseConnection;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.CompareOperator;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.ColumnValueFilter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

/**
 * @version 1.0
 * @auther GouMi
 */
public class HBaseDML {
    public static Connection connection = HBaseConnection.connection;

    /***
     * 插入数据
     * @param namespace 命名空间
     * @param tablename 表名
     * @param rowKey    主键
     * @param columnFamily  列族
     * @param columnName    列
     * @param value 值
     */
    public static void putCell(String namespace,
                               String tablename,
                               String rowKey,
                               String columnFamily,
                               String columnName,
                               String value) throws IOException {
        Table table = connection.getTable(TableName.valueOf(namespace, tablename));
        Put put = new Put(Bytes.toBytes(rowKey));
        put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(columnName), Bytes.toBytes(value));
        try {
            table.put(put);
        } catch (IOException e) {
            e.printStackTrace();
        }
        System.out.println("插入成功");

        table.close();
    }

    /***
     * 读取数据
     * @param namespace
     * @param tablename
     * @param rowKey
     * @param columnFamily
     * @param columnName
     */
    public static void getCells(String namespace,
                                String tablename,
                                String rowKey,
                                String columnFamily,
                                String columnName) throws IOException {
        Table table = connection.getTable(TableName.valueOf(namespace, tablename));
        Get get = new Get(Bytes.toBytes(rowKey));
        get.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(columnName));
        get.readAllVersions();
        Result result = null;
        try {
            result = table.get(get);
            Cell[] cells = result.rawCells();
            for (Cell cell : cells) {
                String s = new String(CellUtil.cloneValue(cell));
                System.out.println(s);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

        table.close();
    }

    /***
     * 扫描数据
     * @param namespace
     * @param tableName
     * @param startRow
     * @param stopRow
     */
    public static void scanRows(String namespace, String tableName, String startRow, String stopRow) throws IOException {
        Table table = connection.getTable(TableName.valueOf(namespace, tableName));
        Scan scan = new Scan();
        scan.withStartRow(Bytes.toBytes(startRow));
        scan.withStopRow(Bytes.toBytes(stopRow));
        try {
            ResultScanner scanner = table.getScanner(scan);
            for (Result result : scanner) {
                Cell[] cells = result.rawCells();
                for (Cell cell : cells) {
                    String s = new String(CellUtil.cloneValue(cell));
                    System.out.println(s);
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

        table.close();

    }


    /**
     *
     * @param namespace
     * @param tableName
     * @param startRow
     * @param stopRow
     * @param columnFamily
     * @param columnName
     * @param value
     * @throws IOException
     */
    public static void filterScan(String namespace,
                                  String tableName,
                                  String startRow,
                                  String stopRow,
                                  String columnFamily,
                                  String columnName,
                                  String value) throws IOException {
        Table table = connection.getTable(TableName.valueOf(namespace, tableName));
        Scan scan = new Scan();
        scan.withStartRow(Bytes.toBytes(startRow));
        scan.withStopRow(Bytes.toBytes(stopRow));
        FilterList filterList = new FilterList();
        filterList.addFilter(new ColumnValueFilter(
                Bytes.toBytes(columnFamily),
                Bytes.toBytes(columnName),
                CompareOperator.EQUAL,
                Bytes.toBytes(value)
        ));
        scan.setFilter(filterList);
        try {
            ResultScanner scanner = table.getScanner(scan);
            for (Result result : scanner) {
                Cell[] cells = result.rawCells();
                for (Cell cell : cells) {
                    String s = new String(CellUtil.cloneValue(cell));
                    System.out.println(s);
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

        table.close();
    }

    /**
     *
     * @param namespace
     * @param tableName
     * @param rowKey
     * @param columnFamily
     * @param columnName
     */
    public static void deleteColumn(String namespace,
                                    String tableName,
                                    String rowKey,
                                    String columnFamily,
                                    String columnName) throws IOException {
        Table table = connection.getTable(TableName.valueOf(namespace, tableName));
        Delete delete = new Delete(Bytes.toBytes(rowKey));
        delete.addColumn(Bytes.toBytes(columnFamily),Bytes.toBytes(columnName));
        try {
            table.delete(delete);
        } catch (IOException e) {
            e.printStackTrace();
        }

        System.out.println("删除某行某列成功");

        table.close();
    }
}






























