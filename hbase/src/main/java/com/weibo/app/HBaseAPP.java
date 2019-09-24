package com.weibo.app;

import com.weibo.constant.Constants;
import com.weibo.dao.HBaseDAO;
import com.weibo.utils.HBaseUtil;

import java.io.IOException;
import java.util.concurrent.CompletionService;

public class HBaseAPP {

    private static void init() {

        //创建命名空间及三张表
        HBaseUtil.createNS(Constants.NAME_SPACE);

        HBaseUtil.createTable(Constants.CONTENT_TABLE, 1, Constants.CONTENT_TABLE_CF.toString());

        HBaseUtil.createTable(Constants.RELATION_TABLE, 1, Constants.RELATION_TABLE_CF1.toString(), Constants.RELATION_TABLE_CF2.toString());

        HBaseUtil.createTable(Constants.INBOX_TABLE, 2, Constants.INBOX_TABLE_CF.toString());
    }

    public static void main(String[] args) throws IOException, InterruptedException {

        //init();

        //1001发布微博
        //HBaseDAO.publish("1001", "Hello World!!!");

        //1002关注1001和1003
        //HBaseDAO.addAttends("1002", "1001", "1003");

        //打印1002的初始化页面
        //HBaseDAO.initData("1002");
        //System.out.println("**********************");

        //1001再发布两条微博
        //HBaseDAO.publish("1001", "Hello haoge!!!");
        //Thread.sleep(500);
        //HBaseDAO.publish("1001", "今天天气不好!!!");

        //打印1002的初始化页面
        //HBaseDAO.initData("1002");
        //System.out.println("**********************");

        //1003关注1001
        //HBaseDAO.addAttends("1003", "1001");

        //打印1003的初始化页面
        //HBaseDAO.initData("1003");
        //System.out.println("**********************");

        //打印1001微博内容
        //HBaseDAO.getWeiBo("1001");

        //1002取关1001
        //HBaseDAO.delAttends("1002", "1001");

        //打印1002的初始化页面
        //HBaseDAO.initData("1002");

        //System.out.println("*************************");

        //1002再次关注1001
        //HBaseDAO.addAttends("1002", "1001");

        //打印1002的初始化页面
        HBaseDAO.initData("1002");

    }

}
