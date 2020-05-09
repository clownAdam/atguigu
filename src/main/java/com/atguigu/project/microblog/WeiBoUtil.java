package com.atguigu.project.microblog;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.RowFilter;
import org.apache.hadoop.hbase.filter.SubstringComparator;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * @author clown
 */
public class WeiBoUtil {
    private static Configuration configuration = HBaseConfiguration.create();

    static {
        configuration.set("hbase.zookeeper.quorum", "192.168.233.101");
    }

    /**
     * 创建命名空间
     */
    public static void createNamespace(String namespace) throws IOException {
        //创建连接
        Connection connection = ConnectionFactory.createConnection(configuration);
        Admin admin = connection.getAdmin();
        //创建namespace描述器
        NamespaceDescriptor namespaceDescriptor = NamespaceDescriptor.create(namespace).build();
        //创建操作
        admin.createNamespace(namespaceDescriptor);
        /*关闭资源*/
        admin.close();
        connection.close();
    }

    /**
     * 创建表
     */
    public static void createTable(String tableName, int versions, String... cfs) throws IOException {
        //创建连接
        Connection connection = ConnectionFactory.createConnection(configuration);
        Admin admin = connection.getAdmin();
        /*构建表描述器*/
        HTableDescriptor hTableDescriptor = new HTableDescriptor(TableName.valueOf(tableName));
        //循环添加列族
        for (String cf : cfs) {
            HColumnDescriptor hColumnDescriptor = new HColumnDescriptor(cf);
            hColumnDescriptor.setMaxVersions(versions);
            hTableDescriptor.addFamily(hColumnDescriptor);
        }
        admin.createTable(hTableDescriptor);
        /*关闭资源*/
        admin.close();
        connection.close();
    }

    /**
     * 发布微博
     * 1、更新微博内容表数据
     * 2、更新收件箱表数据
     * (1)获取当前操作人的fanse
     * (2)在收件箱表中依次更新数据
     *
     * @param uid
     * @param content
     * @throws IOException
     */
    public static void createData(String uid, String content) throws IOException {
        /*获取连接*/
        Connection connection = ConnectionFactory.createConnection(configuration);
        /*获取表对象*/
        Table contTable = connection.getTable(TableName.valueOf(Constant.CONTENT));
        Table relaTable = connection.getTable(TableName.valueOf(Constant.RELATIONS));
        Table inboxTable = connection.getTable(TableName.valueOf(Constant.INBOX));
        long timeMillis = System.currentTimeMillis();
        String rowKey = uid + "_" + timeMillis;
        /*获取put对象*/
        Put put = new Put(Bytes.toBytes(rowKey));
        /*往内容表添加数据*/
        put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("content"), Bytes.toBytes(content));
        contTable.put(put);
        //获取关系表中的fans
        Get get = new Get(Bytes.toBytes(uid));
        get.addFamily(Bytes.toBytes("fans"));
        Result result = relaTable.get(get);
        Cell[] cells = result.rawCells();
        if (cells.length <= 0) {
            return;
        }
        /*更新fans收件箱表*/
        List<Put> puts = new ArrayList<>();
        for (Cell cell : cells) {
            byte[] cloneQualifier = CellUtil.cloneQualifier(cell);
            Put inboxPut = new Put(cloneQualifier);
            inboxPut.addColumn(Bytes.toBytes("info"), Bytes.toBytes(uid), timeMillis, Bytes.toBytes(rowKey));
            puts.add(inboxPut);
        }
        inboxTable.put(puts);
        inboxTable.close();
        relaTable.close();
        contTable.close();
        connection.close();
    }

    /**
     * 关注用户
     * 1、用户关系表
     * (1)添加操作人的attends
     * (2)添加被操做人的fans
     * 2.收件箱表
     * (1)在微博内容中获取被关注者的3条数据(rowkey)
     * (2)在收件箱表中添加操作人的关注者信息
     *
     * @param uid
     * @param uids
     */
    public static void addAttend(String uid, String... uids) throws IOException {
        /*获取连接*/
        Connection connection = ConnectionFactory.createConnection(configuration);
        /*获取表对象*/
        Table contTable = connection.getTable(TableName.valueOf(Constant.CONTENT));
        Table relaTable = connection.getTable(TableName.valueOf(Constant.RELATIONS));
        Table inboxTable = connection.getTable(TableName.valueOf(Constant.INBOX));
        //创建操作者put对象
        Put relaPut = new Put(Bytes.toBytes(uid));
        ArrayList<Put> puts = new ArrayList<>();
        for (String s : uids) {
            relaPut.addColumn(Bytes.toBytes("attends"), Bytes.toBytes(s), Bytes.toBytes(s));
            //创建被关注者的put对象
            Put fansPut = new Put(Bytes.toBytes(s));
            fansPut.addColumn(Bytes.toBytes("fans"), Bytes.toBytes(uid), Bytes.toBytes(uid));
            puts.add(fansPut);
        }
        puts.add(relaPut);
        relaTable.put(puts);

        /*获取内容表中被关注着的rowkey*/
        Put inboxPut = new Put(Bytes.toBytes(uid));
        for (String s : uids) {
            Scan scan = new Scan(Bytes.toBytes(s), Bytes.toBytes(s + "|"));
            ResultScanner results = contTable.getScanner(scan);
            for (Result result : results) {
                String rowKey = Bytes.toString(result.getRow());
                String[] split = rowKey.split("_");
                byte[] row = result.getRow();
                inboxPut.addColumn(Bytes.toBytes("info"), Bytes.toBytes(s), Long.parseLong(split[1]), row);
            }
        }
        inboxTable.put(inboxPut);
        inboxTable.close();
        relaTable.close();
        contTable.close();
        connection.close();
    }

    /**
     * 取关用户
     * 1、用户关系表
     * (1)删除操作者关注列族的待取关用户
     * (2)删除待取关用户的fans列族的操作者
     * 2、收件箱表
     * (1)删除操作者的待取关用户的信息
     */
    public static void deleteAttend(String uid, String... uids) throws IOException {
        /*获取连接对象*/
        Connection connection = ConnectionFactory.createConnection(configuration);
        /*获取表对象*/
        Table relaTable = connection.getTable(TableName.valueOf(Constant.RELATIONS));
        Table inboxTable = connection.getTable(TableName.valueOf(Constant.INBOX));
        /*获取操作者的删除对象*/
        Delete relaDel = new Delete(Bytes.toBytes(uid));
        ArrayList<Delete> deletes = new ArrayList<>();
        for (String s : uids) {
            relaDel.addColumns(Bytes.toBytes("attends"), Bytes.toBytes(s));
            /*创建被取关者删除对象*/
            Delete fansDel = new Delete(Bytes.toBytes(s));
            fansDel.addColumns(Bytes.toBytes("fans"), Bytes.toBytes(uid));
            deletes.add(fansDel);
        }
        deletes.add(relaDel);
        /*执行删除操作*/
        relaTable.delete(deletes);
        /*收件箱表操作*/
        Delete inboxDel = new Delete(Bytes.toBytes(uid));
        for (String s : uids) {
            inboxDel.addColumns(Bytes.toBytes("info"), Bytes.toBytes(s));
        }
        /*执行收件箱表删除操作*/
        inboxTable.delete(inboxDel);
        /*释放资源*/
        inboxTable.close();
        relaTable.close();
        connection.close();
    }

    /**
     * 获取微博内容(初始化页面)
     */
    public static void getInit(String uid) throws IOException {
        /*获取连接*/
        Connection connection = ConnectionFactory.createConnection(configuration);
        /*获取表对象*/
        Table inboxTable = connection.getTable(TableName.valueOf(Constant.INBOX));
        Table contTable = connection.getTable(TableName.valueOf(Constant.CONTENT));
        /*获取收件箱表数据*/
        Get get = new Get(Bytes.toBytes(uid));
        /*设置获取最大版本的数量*/
        get.setMaxVersions();
        Result result = inboxTable.get(get);
        Cell[] cells = result.rawCells();
        ArrayList<Get> gets = new ArrayList<>();
        /*遍历返回内容并将其封装成内容的get对象*/
        for (Cell cell : cells) {
            Get contGet = new Get(CellUtil.cloneValue(cell));
            gets.add(contGet);
        }
        /*根据收件箱表获取值到内容表获取实际微博内容*/
        Result[] results = contTable.get(gets);
        for (Result result1 : results) {
            Cell[] cells1 = result1.rawCells();
            for (Cell cell : cells1) {
                System.out.println("RK:" + Bytes.toString(CellUtil.cloneRow(cell))
                        + ",Content:" + Bytes.toString(CellUtil.cloneValue(cell))
                );
            }
        }
        /*关闭资源*/
        inboxTable.close();
        contTable.close();
        connection.close();
    }

    /**
     * 获取微博内容(查看某个人微博)
     */
    public static void getData(String uid) throws IOException {
        /*获取连接*/
        Connection connection = ConnectionFactory.createConnection(configuration);
        /*获取表对象*/
        Table table = connection.getTable(TableName.valueOf(Constant.CONTENT));
        /*扫描*/
        Scan scan = new Scan();
        RowFilter rowFilter = new RowFilter(CompareFilter.CompareOp.EQUAL, new SubstringComparator(uid + "-"));
        scan.setFilter(rowFilter);
        /*遍历打印*/
        ResultScanner results = table.getScanner(scan);
        for (Result result : results) {
            Cell[] cells = result.rawCells();
            for (Cell cell : cells) {
                byte[] bytes = CellUtil.cloneRow(cell);
                System.out.println("RK:" + Bytes.toString(CellUtil.cloneRow(cell))
                        + ",Content:" + Bytes.toString(CellUtil.cloneValue(cell))
                );
            }
        }
        /*关闭资源*/
        table.close();
        connection.close();
    }
}
