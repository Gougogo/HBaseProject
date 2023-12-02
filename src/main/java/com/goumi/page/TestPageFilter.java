package com.goumi.page;

/**
 * @version 1.0
 * @auther GouMi
 */

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.exceptions.DeserializationException;
import org.apache.hadoop.hbase.filter.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONObject;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class TestPageFilter {
    private static final String TABLE_NAME = "user";

    private static final String COLUMN_FAMILY_BASE = "base";
    private static final String COLUMN_FAMILY_ADDRESS = "address";

    private static final String COLUMN_USERNAME = "username";
    private static final String COLUMN_PASSWORD = "password";
    private static final String COLUMN_HOME = "home";
    private static final String COLUMN_OFFICE = "office";

    private Connection connection;

    public static void main(String[] args) throws Exception {
        Configuration config = HBaseConfiguration.create();
        Connection connection = ConnectionFactory.createConnection(config);

        TestPageFilter t = new TestPageFilter(connection);
        //t.test();

        for (int i = 0; i < 5; i++) {
            //System.out.println("第"+ i + "页\n");

        }

        t.testPage();


        connection.close();
    }

    public TestPageFilter(Connection connection) {
        this.connection = connection;
    }

/*    public JSONObject divicePage(String tableName, String startRow,
                                 String endRow, String lastRowKey, int num) throws IOException {

        Filter filter = new PageFilter(num);//每页展示条数
        byte[] lastRow = null;
        Table table = connection.getTable(TableName.valueOf(TABLE_NAME));
        Scan scan = new Scan();
        scan.setFilter(filter);

        if (lastRowKey != null) {
            lastRow = lastRowKey.getBytes();
            // 注意这里添加了POSTFIX操作，不然死循环了
            //因为hbase的row是字典序列排列的，因此上一次的lastrow需要添加额外的0表示新的开始。另外startKey的那一行是包含在scan里面的
            byte[] start = Bytes.add(lastRow, Bytes.toBytes("POSTFIX"));
            scan.withStartRow(start);
        }else{
            scan.withStartRow(startRow.getBytes());
        }

        byte[] end = endRow.getBytes();
        scan.withStopRow(end);

        ResultScanner rs = table.getScanner(scan);
        Result r = null;
        JSONObject json = new JSONObject();
        JSONArray array = new JSONArray();
        while ((r = rs.next()) != null) {
            lastRow = r.getRow();
            System.out.println(Bytes.toString(lastRow));
            List<Cell> cells = r.listCells();
            JSONObject record = new JSONObject();
            for(int i=0;i<cells.size();i++){
                String key = Bytes.toString(CellUtil.cloneQualifier(cells.get(i)));
                String value = Bytes.toString(CellUtil.cloneValue(cells.get(i)));
                record.put(key, value);
            }
            array.add(record);
        }
        rs.close();

        json.put("last_row", Bytes.toString(lastRow));
        json.put("data", array);
        System.out.println(array.size());
        return json;

    }*/

    private void testPage(){
        try (Table table = connection.getTable(TableName.valueOf(TABLE_NAME))) {
            int pageSize = 100;
            byte[] startRow = Bytes.toBytes("row_0"); // 起始行的 RowKey，这里假设为 0

            Scan scan = new Scan();
            Filter pageFilter = new PageFilter(pageSize);
            scan.setFilter(pageFilter);
            scan.withStartRow(startRow);

            ResultScanner scanner = table.getScanner(scan);
            Result[] results = scanner.next(10);
            while (results != null && results.length > 0) {
                for (Result result : results) {
                    // 处理每一行结果
                    byte[] rowKey = result.getRow();
                    // ...
                    printRow(result);
                }

                System.out.println("==========================");
                results = scanner.next(10);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void testOpp() throws IOException {
        Table table = connection.getTable(TableName.valueOf(TABLE_NAME));

        Scan scan = new Scan();

        // 设置分页参数
        int pageSize = 10;
        int startRow = 0;

        // 设置分页过滤器
        PageFilter pageFilter = new PageFilter(pageSize);
        scan.setFilter(pageFilter);

        ResultScanner resultScanner = table.getScanner(scan);
        Result result;
        int count = 0;

        // 遍历查询结果
        while ((result = resultScanner.next()) != null) {
            if (count < startRow) {
                count++;
                continue;
            }

            // 处理查询结果
            byte[] value = result.getValue(Bytes.toBytes("base"), Bytes.toBytes("user"));
            System.out.println("Value: " + Bytes.toString(value));

            count++;
            if (count >= startRow + pageSize) {
                break;
            }
        }

        resultScanner.close();
        table.close();
        connection.close();
    }

    private void test() throws IOException, DeserializationException {
        createTable();
        prepare();

        Filter filter = new PageFilter(10);
        Table table = connection.getTable(TableName.valueOf(TABLE_NAME));

        byte[] lastRow = null;
        while(true) {
            Scan scan = new Scan();
            scan.setFilter(filter);
            if (lastRow != null) {
                scan.withStartRow(lastRow, false);
            }
            ResultScanner resultScanner = table.getScanner(scan);
            Iterator<Result> it = resultScanner.iterator();
            int count = 0;
            while (it.hasNext()) {
                Result result = it.next();
                printRow(result);
                lastRow = result.getRow();
                count ++;
            }
            resultScanner.close();
            if (count == 0) {
                break;
            }
        }
        table.close();

        deleteTable();
    }

    private void createTable() throws IOException {
        Admin admin = connection.getAdmin();

        try {
            TableDescriptor tableDesc = TableDescriptorBuilder.newBuilder(TableName.valueOf(TABLE_NAME))
                    .addColumnFamily(ColumnFamilyDescriptorBuilder.newBuilder(Bytes.toBytes(COLUMN_FAMILY_BASE)).build())
                    .addColumnFamily(ColumnFamilyDescriptorBuilder.newBuilder(Bytes.toBytes(COLUMN_FAMILY_ADDRESS)).build())
                    .build();
            admin.createTable(tableDesc);
        } finally {
            admin.close();
        }
    }

    private void deleteTable() throws IOException {
        Admin admin = connection.getAdmin();

        try {
            admin.disableTable(TableName.valueOf(TABLE_NAME));
            admin.deleteTable(TableName.valueOf(TABLE_NAME));
        } finally {
            admin.close();
        }
    }

    private void prepare() throws IOException {
        Table table = connection.getTable(TableName.valueOf(TABLE_NAME));

        List<Row> actions = new ArrayList<Row>();
        for (int i = 0; i < 100; i++) {
            Put put = new Put(Bytes.toBytes("row_" + i));
            put.addColumn(Bytes.toBytes(COLUMN_FAMILY_BASE), Bytes.toBytes(COLUMN_USERNAME), Bytes.toBytes("user_" + i));
            put.addColumn(Bytes.toBytes(COLUMN_FAMILY_BASE), Bytes.toBytes(COLUMN_PASSWORD), Bytes.toBytes("password_" + i));
            put.addColumn(Bytes.toBytes(COLUMN_FAMILY_ADDRESS), Bytes.toBytes(COLUMN_HOME), Bytes.toBytes("home_" + i));
            put.addColumn(Bytes.toBytes(COLUMN_FAMILY_ADDRESS), Bytes.toBytes(COLUMN_OFFICE), Bytes.toBytes("office_" + i));
            actions.add(put);
        }
        Object[] results = new Object[actions.size()];

        try {
            table.batch(actions, results);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        table.close();
    }

    private void filter(Filter filter) throws IOException {
        Table table = connection.getTable(TableName.valueOf(TABLE_NAME));
        Scan scan = new Scan();
        scan.setFilter(filter);
        ResultScanner resultScanner = table.getScanner(scan);
        Iterator<Result> it = resultScanner.iterator();
        while (it.hasNext()) {
            Result result = it.next();
            printRow(result);
        }
        resultScanner.close();
        table.close();
    }

    private void printRow(Result result) {
        if (Bytes.toString(result.getRow()) != null) {
            StringBuilder sb = new StringBuilder();
            sb.append(Bytes.toString(result.getRow()));
            sb.append("[");
            sb.append("base:username=" + Bytes.toString(result.getValue(Bytes.toBytes("base"), Bytes.toBytes("username"))));
            sb.append(", base:password=" + Bytes.toString(result.getValue(Bytes.toBytes("base"), Bytes.toBytes("password"))));
            sb.append(", address:home=" + Bytes.toString(result.getValue(Bytes.toBytes("address"), Bytes.toBytes("home"))));
            sb.append(", address:office=" + Bytes.toString(result.getValue(Bytes.toBytes("address"), Bytes.toBytes("office"))));
            sb.append("]");
            System.out.println(sb.toString());
        }
    }
}
