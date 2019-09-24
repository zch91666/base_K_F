package com.weibo.dao;

import com.weibo.constant.Constants;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.RowFilter;
import org.apache.hadoop.hbase.filter.SubstringComparator;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.ArrayList;

/**
 * 数据的增删改查
 * <p>
 * 1.发布微博
 * 2.删除微博
 * 3.关注用户
 * 4.取关用户
 * 5.初始化页面
 * 6.拉取某个人全部微博
 */
public class HBaseDAO {
    private static Configuration conf = null;
    private static Connection connection = null;

    //初始化配置
    static {
        try {
            conf = HBaseConfiguration.create();
            connection = ConnectionFactory.createConnection(conf);
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    /**
     * 发布微博
     * 用户发布微博,收件箱中需要更新粉丝的收件箱
     * 为了得到粉丝数据，需要通过用户关系表获取
     * @param uid 发布微博的用户
     * @param content 发布的微博内容
     * @throws IOException
     */
    public static void publish(String uid, String content) throws IOException {
        //获取内容表对象进行操作
        Table contentTable = connection.getTable(TableName.valueOf(Constants.CONTENT_TABLE));

        //rowkey为uid与时间戳的拼接
        String contentRowKey = uid + "_" + System.currentTimeMillis();

        //创建内容表的put对象
        Put contentPut = new Put(Bytes.toBytes(contentRowKey));
        contentPut.addColumn(Constants.CONTENT_TABLE_CF, Bytes.toBytes("content"), Bytes.toBytes(content));
        contentTable.put(contentPut);

        //操作微博收件表
        //创建用户表对象
        Table relationTable = connection.getTable(TableName.valueOf(Constants.RELATION_TABLE));

        //获取粉丝列族，获取相关粉丝
        Get relationGet = new Get(Bytes.toBytes(uid));
        relationGet.addFamily(Constants.RELATION_TABLE_CF2);
        Result fansResult = relationTable.get(relationGet);

        //创建集合存放收件表put对象
        ArrayList<Put> inboxPuts = new ArrayList<Put>();

        //遍历粉丝
        for (Cell cell : fansResult.rawCells()) {

            //遍历的值为收件表rowkey
            byte[] inboxRowKey = CellUtil.cloneValue(cell);

            //创建收件表的put对象
            Put inboxPut = new Put(inboxRowKey);
            inboxPut.addColumn(Constants.INBOX_TABLE_CF, Bytes.toBytes(uid), Bytes.toBytes(contentRowKey));

            inboxPuts.add(inboxPut);
        }

        if (inboxPuts.size() > 0) {
            //获取收件表对象
            Table inboxTable = connection.getTable(TableName.valueOf(Constants.INBOX_TABLE));
            inboxTable.put(inboxPuts);

            //关闭对象
            inboxTable.close();
        }

        //关闭对象
        relationTable.close();
        contentTable.close();
        connection.close();
    }

    /**
     * 关注用户
     * 关注用户，需要给关注者attends列族添加数据，也需要添加被关注者的rowkey以及所对应的粉丝
     * 更新收件箱表，取出关注对象的微博内容，重新插入更新
     * @param uid 操作用户关系表的用户
     * @param attends 将要关注的人
     * @throws IOException
     */
    public static void addAttends(String uid, String... attends) throws IOException {

        //第一部分：用户关系表
        //判断是否传入关注的人
        if (attends.length <= 0) {
            System.out.println("请输入需要关注的人");
            return;
        }

        //创建关系表对象,put对象
        Table relationTable = connection.getTable(TableName.valueOf(Constants.RELATION_TABLE));
        Put relationPut = new Put(Bytes.toBytes(uid));

        //创建集合批量添加attends
        ArrayList<Put> relationPuts = new ArrayList<Put>();

        //遍历attends，进行添加
        for (String attend : attends) {
            relationPut.addColumn(Constants.RELATION_TABLE_CF1, Bytes.toBytes(attend), Bytes.toBytes(attend));

            //创建被关注者的put对象
            Put attendPut = new Put(Bytes.toBytes(attend));
            attendPut.addColumn(Constants.RELATION_TABLE_CF2, Bytes.toBytes(uid), Bytes.toBytes(uid));

            //将被关注者put对象加进集合
            relationPuts.add(attendPut);
        }

        //将关注者put对象加进集合
        relationPuts.add(relationPut);
        relationTable.put(relationPuts);

        //第二部分:微博收件表
        //获取内容表对象
        Table contentTable = connection.getTable(TableName.valueOf(Constants.CONTENT_TABLE));

        //创建操作者的put对象
        Put inboxPut = new Put(Bytes.toBytes(uid));

        for (String attend : attends) {

            //查看内容表内容
            Scan contentScan = new Scan(Bytes.toBytes(attend), Bytes.toBytes(attend + "|"));
            ResultScanner resultScanner = contentTable.getScanner(contentScan);

            //获取当前时间戳
            long ts = System.currentTimeMillis();

            //遍历将数据插入，因为要取前几个微博，可采用全插入，覆盖方法（如果数据量扩大，可改进内容表rowkey）
            for (Result result : resultScanner) {
                byte[] contentRowKey = result.getRow();
                inboxPut.addColumn(Constants.INBOX_TABLE_CF, Bytes.toBytes(attend), ts++, contentRowKey);
            }
        }

        //内容不为空时，创建收件表对象，put数据
        if (!inboxPut.isEmpty()) {

            //获取收件表对象
            Table inboxTable = connection.getTable(TableName.valueOf(Constants.INBOX_TABLE));
            inboxTable.put(inboxPut);

            //关闭收件表对象
            inboxTable.close();
        }

        //关闭资源
        contentTable.close();
        relationTable.close();
        connection.close();
    }

    /**
     * 取消关注
     * 用户取消关注，先删除当前用户的attends列族中的数据，在dels对象的粉丝列族删去uid
     * 在收件表，取消uid用户接受dels用户微博内容
     * @param uid 操作用户关系表的用户
     * @param dels 将要取消关注的对象
     * @throws IOException
     */
    public static void delAttends(String uid , String... dels) throws IOException {

        //第一部分：用户关系表
        //判断是否传入取消关注的人
        if (dels.length <= 0) {
            System.out.println("请输入需要取消关注的人");
            return;
        }

        //创建delete对象集合
        ArrayList<Delete> relationDels = new ArrayList<Delete>();

        //创建关系表对象,put对象
        Table relationTable = connection.getTable(TableName.valueOf(Constants.RELATION_TABLE));
        Delete relationDelete = new Delete(Bytes.toBytes(uid));

        for (String del : dels) {
            relationDelete.addColumns(Constants.RELATION_TABLE_CF1, Bytes.toBytes(del));

            //创建将被删除者的delete对象
            Delete fansDel = new Delete(Bytes.toBytes(del));
            fansDel.addColumns(Constants.RELATION_TABLE_CF2, Bytes.toBytes(uid));

            //添加进删除集合中
            relationDels.add(fansDel);
        }

        //添加进集合，进行删除
        relationDels.add(relationDelete);
        relationTable.delete(relationDels);

        //第二部分：微博收件箱
        //创建收件箱对象
        Table inboxTable = connection.getTable(TableName.valueOf(Constants.INBOX_TABLE));

        //创建删除对象
        Delete inboxDel = new Delete(Bytes.toBytes(uid));
        for (String del : dels) {
            inboxDel.addColumns(Constants.INBOX_TABLE_CF, Bytes.toBytes(del));
        }

        //执行删除
        inboxTable.delete(inboxDel);

        //关闭资源
        inboxTable.close();
        relationTable.close();
        connection.close();
    }

    /**
     * 获取用户初始化界面
     * @param uid
     * @throws IOException
     */
    public static void initData(String uid) throws IOException {
        //获取收件表对象
        Table inboxTable = connection.getTable(TableName.valueOf(Constants.INBOX_TABLE));

        Get inboxGet = new Get(Bytes.toBytes(uid));
        inboxGet.setMaxVersions();
        Result result = inboxTable.get(inboxGet);

        //创建get对象集合
        ArrayList<Get> contentGets = new ArrayList<Get>();

        //遍历result得到cell，通过cell获取内容表信息
        for (Cell cell : result.rawCells()) {

            //获取内容表get对象
            Get contentGet = new Get(CellUtil.cloneValue(cell));
            contentGets.add(contentGet);
        }

        if( contentGets.size() > 0 ){
            //创建内容表对象
            Table contentTable = connection.getTable(TableName.valueOf(Constants.CONTENT_TABLE));

            Result[] results = contentTable.get(contentGets);

            for (Result contentResult : results) {

                for (Cell cell : contentResult.rawCells()) {
                    System.out.println("ROWKEY:" + Bytes.toString(CellUtil.cloneRow(cell)) +
                            "，Value:" + Bytes.toString(CellUtil.cloneValue(cell)));
                }
            }
            contentTable.close();
        }

        //关闭资源
        inboxTable.close();
        connection.close();
    }

    /**
     * 获取一个用户的所有微博
     * @param uid
     * @throws IOException
     */
    public static void getWeiBo(String uid) throws IOException{
        //创建内容表对象
        Table contentTable = connection.getTable(TableName.valueOf(Constants.CONTENT_TABLE));

        //创建scan对象
        Scan contentScan = new Scan();
        contentScan.setFilter(new RowFilter(CompareFilter.CompareOp.EQUAL, new SubstringComparator(uid + "_")));
        ResultScanner results = contentTable.getScanner(contentScan);

        //遍历数据
        for (Result contentResult : results) {
            for (Cell cell : contentResult.rawCells()) {
                System.out.println("ROWKEY:" + Bytes.toString(CellUtil.cloneRow(cell)) +
                        "，Value:" + Bytes.toString(CellUtil.cloneValue(cell)));
            }
        }

        contentTable.close();
        connection.close();
    }
}
