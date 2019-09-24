package com.weibo.constant;

import org.apache.hadoop.hbase.util.Bytes;

/**
 * 命名空间，表名
 */
public class Constants {

    //命名空间
    public static final String NAME_SPACE = "weibo";

    //微博内容表
    public static final String CONTENT_TABLE = "weibo:content";
    public static final byte[] CONTENT_TABLE_CF = Bytes.toBytes("info");

    //微博用户关系表
    public static final String RELATION_TABLE = "weibo:relation";
    public static final byte[] RELATION_TABLE_CF1 = Bytes.toBytes("attends");
    public static final byte[] RELATION_TABLE_CF2 = Bytes.toBytes("fans");

    //微博收件箱表
    public static final String INBOX_TABLE = "weibo:inbox";
    public static final byte[] INBOX_TABLE_CF = Bytes.toBytes("info");
}

