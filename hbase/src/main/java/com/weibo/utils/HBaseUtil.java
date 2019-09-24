package com.weibo.utils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;

import java.io.IOException;

/**
 * 命名空间以及表的增删改查
 */
public class HBaseUtil {
    public static void main(String[] args) {
        //设置命名空间
        //createNS("weibo");

        //创建表
        createTable("weibo:content", 1, "info");
        createTable("weibo:relation", 1, "attends", "fans");
        createTable("weibo:inbox", 3, "info");
    }
    //创建命名空间
    public static void createNS(String ns) {

        Configuration configuration = HBaseConfiguration.create();
        try {
            //1.获取连接及Admin对象
            Connection connection = ConnectionFactory.createConnection(configuration);
            Admin admin = connection.getAdmin();

            //2.获取命名空间描述器
            NamespaceDescriptor namespaceDescriptor = NamespaceDescriptor.create(ns).build();

            //3.创建命名空间
            admin.createNamespace(namespaceDescriptor);

            //4.关闭资源
            admin.close();
            connection.close();

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    //判断表是否存在
    private static boolean isTableExist(String tableName) {

        //1.获取连接及Admin对象
        Configuration configuration = HBaseConfiguration.create();

        boolean exists = true;

        try {
            //1.获取连接及Admin对象
            Connection connection = ConnectionFactory.createConnection(configuration);
            Admin admin = connection.getAdmin();

            //2.判断是否存在
            exists = admin.tableExists(TableName.valueOf(tableName));

            //3.关闭资源
            admin.close();
            connection.close();

        } catch (IOException e) {
            e.printStackTrace();
        }

        //4.返回结果
        return exists;
    }

    //创建表
    public static void createTable(String tableName, int versions, String... cfs) {

        //1.判断列族是否存在
        if (cfs.length <= 0) {
            System.out.println("请设置列族信息！！");
            return;
        }

        //2.判断表是否存在
        if (isTableExist(tableName)) {
            System.out.println(tableName + "表已存在！！");
            return;
        }

        //配置信息
        Configuration configuration = HBaseConfiguration.create();
        try {
            //获取连接以及Admin对象
            Connection connection = ConnectionFactory.createConnection(configuration);
            Admin admin = connection.getAdmin();

            //3.构建表描述器
            HTableDescriptor hTableDescriptor = new HTableDescriptor(TableName.valueOf(tableName));

            //4.循环添加列族信息
            for (String cf : cfs) {
                HColumnDescriptor hColumnDescriptor = new HColumnDescriptor(cf);
                //设置列族的版本
                hColumnDescriptor.setMaxVersions(versions);
                hTableDescriptor.addFamily(hColumnDescriptor);
            }

            //5.创建表
            admin.createTable(hTableDescriptor);

            //6.关闭资源
            admin.close();
            connection.close();

        } catch (IOException e) {
            e.printStackTrace();
        }


    }

}
