package com.haoge;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

/**
 * DDL
 * 1.创建命名空间
 * 2.判断表是否存在
 * 3.创建表
 * 4.删除表
 * <p>
 * DML
 * 1.添加数据（修改）
 * 2.查询数据（get）
 * 3.查询数据（scan）
 * 4.删除数据
 */
public class HbaseAPI {
    private static Configuration conf = null;
    private static Connection connection = null;
    private static Admin admin = null;

    //初始化配置
    static {
        //获取配置文件
        conf = new Configuration();

        //设置连接
        conf.set("hbase.zookeeper.quorum", "hadoop102");
        try {
            connection = ConnectionFactory.createConnection(conf);

            //HBaseAdmin admin = new HBaseAdmin(conf);  //过时
            //admin负责DDL操作
            admin = connection.getAdmin();

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    //关闭资源
    public static void close() throws IOException {
        admin.close();
        connection.close();
    }

    //DDL 1.创建命名空间
    public static void createNameSpace(String ns) throws IOException {
        NamespaceDescriptor nsDescriptor = NamespaceDescriptor.create(ns).build();
        admin.createNamespace(nsDescriptor);
        close();
    }

    //DDL 2.判断表是否存在
    public static void isTableExists(String tableName) throws IOException {

        //创建TableName对象
        TableName tn = TableName.valueOf(tableName);
        System.out.println(admin.tableExists(tn));
        close();
    }

    //DDL 3.创建表
    public static void createTable(String tableName) throws IOException {

        //创建HTableDescriptor对象
        HTableDescriptor htd = new HTableDescriptor(tableName);

        //添加列族
        htd.addFamily(new HColumnDescriptor("info1"));
        htd.addFamily(new HColumnDescriptor("info2"));

        admin.createTable(htd);
        close();
    }

    //DDL 4.删除表
    public static void deleteTable(String tableName) throws IOException {

        //创建TableName对象
        TableName tn = TableName.valueOf(tableName);

        //将表置位disable状态
        admin.disableTable(tn);

        admin.deleteTable(tn);

        close();
    }

    //DML 1.添加数据
    public static void putData(String tableName, String rowKey, String columnFamily, String columnName, String value) throws IOException {

        //table负责DML操作
        TableName tn = TableName.valueOf(tableName);
        Table table = connection.getTable(tn);

        //创建put对象，添加值
        Put put = new Put(Bytes.toBytes(rowKey));
        put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(columnName), Bytes.toBytes(value));

        table.put(put);
        close();
    }

    //DML 2.获取数据（get）
    public static void getData(String tableName, String rowKey) throws IOException {

        //table负责DML操作
        TableName tn = TableName.valueOf(tableName);
        Table table = connection.getTable(tn);

        //通过get对象获取result数据
        Get get = new Get(Bytes.toBytes(rowKey));
        Result result = table.get(get);

        //遍历得到值
        for (Cell cell : result.rawCells()) {
            System.out.println("rowKey:" + Bytes.toString(result.getRow())
                        + "columnFamily:" + Bytes.toString(CellUtil.cloneFamily(cell))
                       + "columnName:" + Bytes.toString(CellUtil.cloneQualifier(cell))
                      + "columnValue:" + Bytes.toString(CellUtil.cloneValue(cell))
                     + "timestamp:" + cell.getTimestamp());
        }
        close();
    }

    //DML 2.获取数据（get） 重载，根据列族与列名获取
    public static void getData(String tableName , String rowKey , String columnFamily , String columnName) throws IOException {

        //table负责DML操作
        TableName tn = TableName.valueOf(tableName);
        Table table = connection.getTable(tn);

        //通过get对象获取result数据
        Get get = new Get(Bytes.toBytes(rowKey));
        get.addColumn(Bytes.toBytes(columnFamily) , Bytes.toBytes(columnName));
        Result result = table.get(get);

        //遍历得到值
        for (Cell cell : result.rawCells()) {
            System.out.println("rowKey:" + Bytes.toString(result.getRow())
                    + "columnFamily:" + Bytes.toString(CellUtil.cloneFamily(cell))
                    + "columnName:" + Bytes.toString(CellUtil.cloneQualifier(cell))
                    + "columnValue:" + Bytes.toString(CellUtil.cloneValue(cell))
                    + "timestamp:" + cell.getTimestamp());
        }
        close();
    }

    //DML 2.查询数据（scan）
    public static void scanData(String tableName) throws IOException {

        //table负责DML操作
        TableName tn = TableName.valueOf(tableName);
        Table table = connection.getTable(tn);

        //获取scan对象，得到resultscanner
        Scan scan = new Scan();
        ResultScanner scanner = table.getScanner(scan);

        //遍历数据
        for (Result result : scanner) {
            Cell[] cells = result.rawCells();
            for (Cell cell : cells) {
                System.out.println("rowKey:" + Bytes.toString(result.getRow())
                        + "columnFamily:" + Bytes.toString(CellUtil.cloneFamily(cell))
                        + "columnName:" + Bytes.toString(CellUtil.cloneQualifier(cell))
                        + "columnValue:" + Bytes.toString(CellUtil.cloneValue(cell))
                        + "timestamp:" + cell.getTimestamp());
            }
        }

        close();
    }
    
    public static void main(String[] args) throws IOException {
        //DDL
        //1.创建命名空间
        //createNameSpace("bigtable");

        //2.判断表是否存在
        //isTableExists("stu");

        //3.创建表
        //createTable("hellohbase");

        //4.删除表
        //deleteTable("stu");

        //DML
        //1.添加数据(修改数据)
        //putData("hellohbase", "1001", "info1", "name", "hahaha");
        //putData("hellohbase", "1001", "info2", "address", "shanghai");

        //2.查询数据（get scan）
        //getData("hellohbase" , "1001");
        //getData("hellohbase" , "1001" , "info1" , "name");
        scanData("hellohbase");
    }
}
