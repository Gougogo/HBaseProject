package com.goumi.guliweibo.dao;

/**
 * @version 1.0
 * @auther GouMi
 */

import com.goumi.dml.HBaseDML;
import com.goumi.guliweibo.bean.Message;
import com.goumi.guliweibo.constants.Constants;
import com.goumi.utils.HBaseConnection;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.RowFilter;
import org.apache.hadoop.hbase.util.Bytes;

import javax.rmi.CORBA.Util;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/***
 * 1,发布微博
 * 2,删除微博
 * 3,关注微博
 * 4,取关用户
 * 5,获取用户微博详情
 * 6,获取用户初始化页面
 */
public class HBaseDao {
    /**
     * 发布微博
     * @param uid
     * @param content
     * @throws IOException
     */
    public static void publishWeiBo(String uid, String content) throws IOException {
        Connection connection = HBaseConnection.connection;
        Table contTable = connection.getTable(TableName.valueOf(Constants.CONTENT_TABLE));
        //将内容表新增一条数据
        long ts = System.currentTimeMillis();
        Put put = new Put(Bytes.toBytes(uid +"_"+ ts));
        put.addColumn(Bytes.toBytes(Constants.CONTENT_TABLE_CF), Bytes.toBytes("content"), Bytes.toBytes(content));
        contTable.put(put);

        //拿到关系表中当前uid对应的粉丝数据
        Table relationTable = connection.getTable(TableName.valueOf(Constants.RELATION_TABLE));
        Get get = new Get(Bytes.toBytes(uid));
        get.addFamily(Bytes.toBytes("fans"));
        Result result = relationTable.get(get);
        ArrayList<Put> inboxPuts = new ArrayList<>();
        for (Cell cell : result.rawCells()) {
            Put inboxPut = new Put(CellUtil.cloneValue(cell));
            inboxPut.addColumn(Bytes.toBytes(Constants.INBOX_TABLE_CF), Bytes.toBytes(uid), Bytes.toBytes(content));
            inboxPuts.add(inboxPut);
        }

        //更新粉丝数据中间表
        Table inboxTable = connection.getTable(TableName.valueOf(Constants.INBOX_TABLE));
        if (inboxPuts.size()>0){
            for (Put inboxPut : inboxPuts) {
                inboxTable.put(inboxPut);
            }
        }
        relationTable.close();
        contTable.close();
        inboxTable.close();
        connection.close();
    }

    /**
     * 例如：
     * 1：在relation表中增加A的attends为B
     * 2：在relation表中增加B的fans为A
     * 3：在收件箱表中为A增加B的最近三条数据
     *
     * @param uid
     * @param attends
     */
    public static void addAttends(String uid, String... attends) throws IOException {

        if (attends.length <= 0) {
            System.out.println("没有参数");
        }
        Connection connection = HBaseConnection.connection;

        Table relationTable = connection.getTable(TableName.valueOf(Constants.RELATION_TABLE));
        Get get = new Get(Bytes.toBytes(uid));
        get.addFamily(Bytes.toBytes("fans"));
        Result result = relationTable.get(get);

        //1：在relation表中增加A的attends为B
        //2：在relation表中增加B的fans为A
        for (Cell cell : result.rawCells()) {
            String s = new String(CellUtil.cloneValue(cell));
            System.out.println(s);
            for (String attend : attends) {
                //判断attends是否已经是uid的关注者
                if (attend.equals(s)) {
                    System.out.println("已经存在");
                } else {
                    Put put = new Put(Bytes.toBytes(uid));
                    put.addColumn(Bytes.toBytes(Constants.RELATION_TABLE_CF1), Bytes.toBytes(attend), Bytes.toBytes(attend));
                    put.addColumn(Bytes.toBytes(Constants.RELATION_TABLE_CF2), Bytes.toBytes(uid), Bytes.toBytes(uid));
                    relationTable.put(put);
                }
            }
        }

        //3：在收件箱表中为uid=A增加attends为BCD的最近三条数据
        //3.1首先应该将BCD最近三条数据查出来
        //然后创一个A的put对象，但有3列为BCD，value为对应的值
        //但有可能BCD都没有发布过微博
        Table contentTable = connection.getTable(TableName.valueOf(Constants.CONTENT_TABLE));
        Put inboxPut = new Put(Bytes.toBytes(uid));
        for (String attend : attends) {
            Scan scan = new Scan();
            scan.withStartRow(Bytes.toBytes(attend + "_"));
            scan.withStopRow(Bytes.toBytes(attend + "|"));
            ResultScanner scanner = contentTable.getScanner(scan);
            long ts = System.currentTimeMillis();
            for (Result result1 : scanner) {
                inboxPut.addColumn(Bytes.toBytes(Constants.INBOX_TABLE_CF), Bytes.toBytes(attend), ts++, result1.getRow());
            }
        }

        if (!inboxPut.isEmpty()) {
            Table inboxTable = connection.getTable(TableName.valueOf(Constants.INBOX_TABLE));
            inboxTable.put(inboxPut);
            inboxTable.close();
        }

        relationTable.close();
        contentTable.close();
        connection.close();
    }

    /**
     *获取关注的人的微博
     * a、从微博收件箱中获取所关注的用户的微博 RowKey
     * b、根据获取的 RowKey，得到微博内容
     * @param uid
     * @return
     */
    public static List<Message> getAttendsContent(String uid) throws IOException {
        //get attends from relationTable
        Connection connection = HBaseConnection.connection;

        Table inboxTable = connection.getTable(TableName.valueOf(Constants.INBOX_TABLE));
        Get get = new Get(Bytes.toBytes(uid));
        //get.addFamily(Bytes.toBytes("attends"));
        Result result = inboxTable.get(get);

        List<byte[]> rowkeys = new ArrayList<>();
        List<Message> messages = new ArrayList<Message>();

        for (Cell cell : result.rawCells()) {
            String s = new String(CellUtil.cloneValue(cell));
            System.out.println("s=" + s);
            rowkeys.add(CellUtil.cloneValue(cell));

            Get get1 = new Get(Bytes.toBytes(s));
            Table contentTable = connection.getTable(TableName.valueOf(Constants.CONTENT_TABLE));
            Result result1 = contentTable.get(get1);
            for (Cell rawCell : result1.rawCells()) {
                Message message = new Message();
                String rowKey = Bytes.toString(CellUtil.cloneRow(rawCell));
                String userid = rowKey.substring(0, rowKey.indexOf("_"));
                //System.out.println(rowKey);

                String timestamp = rowKey.substring(rowKey.indexOf("_") + 1);

                String content = Bytes.toString(CellUtil.cloneValue(rawCell));

                //System.out.println(userid+"_"+timestamp+"_"+content);
                message.setContent(content);
                message.setTimestamp(timestamp);
                message.setUid(userid);
                messages.add(message);
            }
        }
        return messages;
    }

    /**
     * remove attends
     * 1:remove attends of uid from relation table
     * 2:remove fans of attends from relation table
     * 3:delete attends's content of uid in inbox table
     * @param uid
     * @param attends
     */
    public void removeAttends(String uid, String... attends) throws IOException {
        for (String attend : attends) {
            HBaseDML.deleteColumn(Constants.NAMESPACE, Constants.RELATION_TABLE, uid, Constants.RELATION_TABLE_CF1, attend);
            HBaseDML.deleteColumn(Constants.NAMESPACE, Constants.RELATION_TABLE, attend, Constants.RELATION_TABLE_CF2, uid);
            HBaseDML.deleteColumn(Constants.NAMESPACE, Constants.INBOX_TABLE, uid, Constants.INBOX_TABLE_CF, attend);
        }
    }
}
