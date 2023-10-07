package com.goumi.ddl;

import com.goumi.utils.HBaseConnection;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;


/**
 * @version 1.0
 * @auther GouMi
 */
public class HBaseDDL {
    public static Connection connection = HBaseConnection.connection;

    /***
     * 创建命名空间
     * @param namespace
     * @throws IOException
     */
    public static void createNameSpace(String namespace) throws IOException {
        Admin admin = connection.getAdmin();
        NamespaceDescriptor.Builder namespaceBuilder = NamespaceDescriptor.create(namespace);
        namespaceBuilder.addConfiguration("user", "hadoop");
        try {
            admin.createNamespace(namespaceBuilder.build());
        } catch (IOException e) {
            System.out.println("namespace has exist.");
            e.printStackTrace();
        }
        admin.close();
    }

    /***
     * 判断表是否存在
     * @param nameSpace 命名空间
     * @param tableName 表名
     * @return
     * @throws IOException
     */
    private static boolean isTableExist(String nameSpace, String tableName) throws IOException {
        //在HBase中管理、访问表需要先创建HBaseAdmin对象
        HBaseAdmin admin = (HBaseAdmin) connection.getAdmin();

        boolean b = false;
        try {
            b = admin.tableExists(TableName.valueOf(nameSpace, tableName));
        } catch (IOException e) {
            e.printStackTrace();
        }

        return b;
    }

    /***
     * 创建表
     * @param tableName
     * @param nameSpace
     * @param columnFamily
     * @throws IOException
     */
    public static void createTable(String nameSpace, String tableName, int versions, String... columnFamily) throws IOException {
        if (columnFamily.length == 0){
            throw new RuntimeException("至少要有一个列族");
        }
        HBaseAdmin admin = (HBaseAdmin) connection.getAdmin();
        //判断表是否存在
        if(isTableExist(nameSpace, tableName)){
            System.out.println("表" + tableName + "已存在");
        }else{
            //创建表属性对象,表名需要转字节
            TableDescriptorBuilder tableDescriptorBuilder = TableDescriptorBuilder.newBuilder(TableName.valueOf(nameSpace, tableName));
            //创建多个列族
            for(String cf : columnFamily){
                ColumnFamilyDescriptorBuilder columnFamilyDescriptorBuilder = ColumnFamilyDescriptorBuilder.newBuilder(Bytes.toBytes(cf));
                columnFamilyDescriptorBuilder.setMaxVersions(versions);
                tableDescriptorBuilder.setColumnFamily(columnFamilyDescriptorBuilder.build());
            }
            //根据对表的配置，创建表
            try {
                admin.createTable(tableDescriptorBuilder.build());
            } catch (IOException e) {
                e.printStackTrace();
            }
            System.out.println("表" + tableName + "创建成功！");
            admin.close();
        }
    }

    /***
     * 修改表中列族版本
     * @param namespace
     * @param tableName
     * @param columnFamily
     * @param version
     */
    public static void modifyVersionOfTable(String namespace, String tableName, String columnFamily, int version) throws IOException {
        Admin admin = connection.getAdmin();
        if (!isTableExist(namespace, tableName))
            throw new RuntimeException("表不存在");


        TableDescriptor descriptor = admin.getDescriptor(TableName.valueOf(namespace, tableName));
        TableDescriptorBuilder tableDescriptorBuilder = TableDescriptorBuilder.newBuilder(descriptor);
        ColumnFamilyDescriptorBuilder columnFamilyDescriptorBuilder = ColumnFamilyDescriptorBuilder.newBuilder(Bytes.toBytes(columnFamily));
        columnFamilyDescriptorBuilder.setMaxVersions(version);
        tableDescriptorBuilder.modifyColumnFamily(columnFamilyDescriptorBuilder.build());
        try {
            admin.modifyTable(tableDescriptorBuilder.build());
        } catch (IOException e) {
            e.printStackTrace();
        }
        System.out.println("表修改成功");
        admin.close();
    }

    /**
     * 增加表的列族
     * @param namespace
     * @param tableName
     * @param columnFamily
     * @throws IOException
     */
    public static void addColFamilyOfTable(String namespace, String tableName, String columnFamily) throws IOException {
        Admin admin = connection.getAdmin();
        if (!isTableExist(namespace, tableName))
            throw new RuntimeException("表不存在");

        TableDescriptor descriptor = admin.getDescriptor(TableName.valueOf(namespace, tableName));
        TableDescriptorBuilder tableDescriptorBuilder = TableDescriptorBuilder.newBuilder(descriptor);
        ColumnFamilyDescriptorBuilder columnFamilyDescriptorBuilder = ColumnFamilyDescriptorBuilder.newBuilder(Bytes.toBytes(columnFamily));
        tableDescriptorBuilder.setColumnFamily(columnFamilyDescriptorBuilder.build());
        try {
            admin.modifyTable(tableDescriptorBuilder.build());
        } catch (IOException e) {
            e.printStackTrace();
        }
        System.out.println("表修改成功");
        admin.close();
    }

    /***
     * 删除表
     * @param namespace
     * @param tableName
     * @return true表示成功
     */
    public static boolean deleteTable(String namespace, String tableName) throws IOException {
        if (!isTableExist(namespace, tableName)){
            throw new RuntimeException("要删除的表不存在");
        }

        Admin admin = connection.getAdmin();
        TableName tableName1 = TableName.valueOf(namespace, tableName);

        try {
            admin.disableTable(tableName1);
            admin.deleteTable(tableName1);
        } catch (IOException e) {
            e.printStackTrace();
            System.out.println("删除失败");
            return false;
        }
        System.out.println("删除成功");
        admin.close();

        return true;
    }
}
