package com.atguigu.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

/**
 * Hbase API study
 * @author clown
 */
public class TestHbase {
    private static Admin admin = null;
    private static Connection connection = null;
    private static Configuration configuration = null;

    static {
        //hbase配置文件
        configuration = HBaseConfiguration.create();
        configuration.set("hbase.zookeeper.quorum", "bd-101");
        configuration.set("hbase.zookeeper.property.clientPort", "2181");
        /*获取连接对象*/
        try {
            connection = ConnectionFactory.createConnection(configuration);
            admin = connection.getAdmin();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private static void close(Connection connection, Admin admin) {
        if (admin != null) {
            try {
                admin.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        if (connection != null) {
            try {
                connection.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    /**
     * 旧API,1.判断表是否存在
     */
    public boolean tableExist(String tableName) throws IOException {
        //hbase配置文件
        HBaseConfiguration configuration = new HBaseConfiguration();
        configuration.set("hbase.zookeeper.quorum", "bd-101");
        configuration.set("hbase.zookeeper.property.clientPort", "2181");
        //获取hbase管理员对象
        HBaseAdmin admin = new HBaseAdmin(configuration);
        //执行
        boolean tableExists = admin.tableExists(tableName);
        //关闭资源
        admin.close();
        return tableExists;
    }

    /**
     * 新API,1.判断表是否存在
     */
    public boolean newTableExists(String tableName) throws IOException {
        //hbase配置文件
        Configuration configuration = HBaseConfiguration.create();
        configuration.set("hbase.zookeeper.quorum", "bd-101");
        configuration.set("hbase.zookeeper.property.clientPort", "2181");
        /*获取连接对象*/
        Connection connection = ConnectionFactory.createConnection(configuration);
        Admin admin = connection.getAdmin();
        boolean tableExists = admin.tableExists(TableName.valueOf(tableName));
        return tableExists;
    }

    /**
     * 2.创建表
     */
    public void createTable(String tableName, String... cfs) throws IOException {
        if (tableExist(tableName)) {
            System.out.println("表已存在！");
            return;
        }
        /*创建表描述器*/
        HTableDescriptor hTableDescriptor = new HTableDescriptor(TableName.valueOf(tableName));
        /*添加列族*/
        for (String cf : cfs) {
            /*创建列描述器*/
            HColumnDescriptor hColumnDescriptor = new HColumnDescriptor(cf);
            hTableDescriptor.addFamily(hColumnDescriptor);
        }
        /*创建表操作*/
        admin.createTable(hTableDescriptor);
        System.out.println("表创建成功");
    }

    /**
     * 3.删除表
     */
    public void deleteTable(String tableName) throws IOException {
        if (!tableExist(tableName)) {
            System.out.println("表不存在");
            return;
        }
        /*使表不可用(下线)*/
        admin.disableTable(TableName.valueOf(tableName));
        /*执行删除操作*/
        admin.deleteTable(TableName.valueOf(tableName));
        System.out.println("表已删除");

    }

    /**
     * 增,改
     */
    public void putData(String tableName, String rowKey, String cf, String cn, String value) throws IOException {
        /*获取表对象*/
        //旧api
        //HTable hTable = new HTable(configuration, TableName.valueOf(tableName));
        //新api
        Table table = connection.getTable(TableName.valueOf(tableName));
        /*创建put对象*/
        Put put = new Put(Bytes.toBytes(rowKey));
        /*添加数据*/
        put.addColumn(Bytes.toBytes(cf), Bytes.toBytes(cn), Bytes.toBytes(value));
        /*执行添加操作*/
        table.put(put);
        table.close();
    }

    /**
     * 删
     */
    public void delete(String tableName, String rowKey, String cf, String cn) throws IOException {
        Table table = connection.getTable(TableName.valueOf(tableName));
        Delete delete = new Delete(Bytes.toBytes(rowKey));
        delete.addColumns(Bytes.toBytes(cf), Bytes.toBytes(cn));
//        delete.addColumn(Bytes.toBytes(cf),Bytes.toBytes(cn));
        table.delete(delete);
        table.close();
    }

    /**
     * 查,全表扫描
     */
    public void scanTable(String tableName) throws IOException {
        Table table = connection.getTable(TableName.valueOf(tableName));
        Scan scan = new Scan();
        ResultScanner scanner = table.getScanner(scan);
        for (Result result : scanner) {
            Cell[] cells = result.rawCells();
            for (Cell cell : cells) {
                String value = Bytes.toString(CellUtil.cloneValue(cell));
                String rowkey = Bytes.toString(CellUtil.cloneRow(cell));
                String cf = Bytes.toString(CellUtil.cloneFamily(cell));
                String cn = Bytes.toString(CellUtil.cloneQualifier(cell));
                System.out.println("rowkey = " + rowkey + ",cf = " + cf + ",cn = " + cn + ",value = " + value);
            }
        }
        table.close();
    }

    /**
     * 获取指定列族:列的数据
     */
    public void getData(String tableName,String rowKey,String cf,String cn) throws IOException {
        Table table = connection.getTable(TableName.valueOf(tableName));
        Get get = new Get(Bytes.toBytes(rowKey));
        get.addColumn(Bytes.toBytes(cf),Bytes.toBytes(cn));
//        get.setMaxVersions();
        Result result = table.get(get);
        Cell[] cells = result.rawCells();
        for (Cell cell : cells) {
            String value = Bytes.toString(CellUtil.cloneValue(cell));
            String rowKey1 = Bytes.toString(CellUtil.cloneRow(cell));
            String cf1 = Bytes.toString(CellUtil.cloneFamily(cell));
            String cn1 = Bytes.toString(CellUtil.cloneQualifier(cell));
            System.out.println("rowkey = " + rowKey1 + ",cf = " + cf1 + ",cn = " + cn1 + ",value = " + value);
        }
    }

    public static void main(String[] args) throws IOException {
        TestHbase testHbase = new TestHbase();
//        testHbase.putData("student","1001", "info", "sex", "female");
//        testHbase.delete("student","1001", "", "");
//        testHbase.scanTable("student");
        testHbase.getData("student","1003", "info", "name");
        close(connection, admin);
    }
}
