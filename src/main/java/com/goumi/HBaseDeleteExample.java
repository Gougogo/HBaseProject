package com.goumi;

/**
 * @version 1.0
 * @auther GouMi
 */
import com.goumi.utils.HBaseConnection;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * 循环删除列值info:bs=03的数据
 */
public class HBaseDeleteExample {
    public static Connection connection = HBaseConnection.connection;
    public static void main(String[] args) {
        try {
            // 创建表名对象
            TableName tableName = TableName.valueOf("student");

            // 创建表实例
            Table table = connection.getTable(tableName);

            // 创建扫描器对象
            Scan scan = new Scan();

            // 添加列族和列的过滤器
            scan.addColumn(Bytes.toBytes("info"), Bytes.toBytes("bs"));
            scan.setFilter(new SingleColumnValueFilter(
                    Bytes.toBytes("info"),
                    Bytes.toBytes("bs"),
                    CompareFilter.CompareOp.EQUAL,
                    Bytes.toBytes("03")
            ));

            // 获取扫描结果的迭代器
            ResultScanner scanner = table.getScanner(scan);

            // 遍历扫描结果并删除对应的行
            for (Result result : scanner) {
                System.out.println(Bytes.toString(result.getRow()));
                Delete delete = new Delete(result.getRow());
                table.delete(delete);
            }

            // 关闭资源
            scanner.close();
            table.close();
            connection.close();

            System.out.println("删除操作完成。");

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
