package com.testsolr;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

/**
 * Created by shangyongqiang on 2016/9/12.
 * 读取配置文件的辅助类
 */
public class ConfigProperties {
    private static Properties props;
    private String HBASE_ZOOKEEPER_QUORUM;
    private String HBASE_ZOOKEEPER_PROPERTY_CLIENT_PORT;
    private String HBASE_MASTER;
    private String HBASE_ROOTDIR;
    private String DFS_NAME_DIR;
    private String DFS_DATA_DIR;
    private String FS_DEFAULT_NAME;
    private String SOLR_SERVER;//solr服务器地址
    private String HBASE_TABLE_NAME;//需要建立solr索引的hbsae表的名称
    private String HBASE_TABLE_FAMILY;//hbase的列祖

    public ConfigProperties(String lo) {
        props = new Properties();
        try {
            InputStream in = this.getClass().getResourceAsStream(lo);
            props.load(in);
            HBASE_ZOOKEEPER_QUORUM = props.getProperty("HBASE_ZOOKEEPER_QUORUM");
            HBASE_ZOOKEEPER_PROPERTY_CLIENT_PORT = props.getProperty("HBASE_ZOOKEEPER_PROPERTY_CLIENT_PORT");
            HBASE_MASTER = props.getProperty("HBASE_MASTER");
            HBASE_ROOTDIR = props.getProperty("HBASE_ROOTDIR");
            DFS_NAME_DIR = props.getProperty("DFS_NAME_DIR");
            DFS_DATA_DIR = props.getProperty("DFS_DATA_DIR");
            FS_DEFAULT_NAME = props.getProperty("FS_DEFAULT_NAME");
            SOLR_SERVER = props.getProperty("SOLR_SERVER");
            HBASE_TABLE_NAME = props.getProperty("HBASE_TABLE_NAME");
            HBASE_TABLE_FAMILY = props.getProperty("HBASE_TABLE_FAMILY");
        } catch (IOException e) {
            throw new RuntimeException("加载配置文件出错");

        } catch (NullPointerException e) {
            throw new RuntimeException("文件不存在");
        }

    }

    public static Properties getProps() {
        return props;
    }

    public static void setProps(Properties props) {
        ConfigProperties.props = props;
    }

    public String getHBASE_ZOOKEEPER_QUORUM() {
        return HBASE_ZOOKEEPER_QUORUM;
    }

    public void setHBASE_ZOOKEEPER_QUORUM(String HBASE_ZOOKEEPER_QUORUM) {
        this.HBASE_ZOOKEEPER_QUORUM = HBASE_ZOOKEEPER_QUORUM;
    }

    public String getHBASE_ZOOKEEPER_PROPERTY_CLIENT_PORT() {
        return HBASE_ZOOKEEPER_PROPERTY_CLIENT_PORT;
    }

    public void setHBASE_ZOOKEEPER_PROPERTY_CLIENT_PORT(String HBASE_ZOOKEEPER_PROPERTY_CLIENT_PORT) {
        this.HBASE_ZOOKEEPER_PROPERTY_CLIENT_PORT = HBASE_ZOOKEEPER_PROPERTY_CLIENT_PORT;
    }

    public String getHBASE_MASTER() {
        return HBASE_MASTER;
    }

    public void setHBASE_MASTER(String HBASE_MASTER) {
        this.HBASE_MASTER = HBASE_MASTER;
    }

    public String getHBASE_ROOTDIR() {
        return HBASE_ROOTDIR;
    }

    public void setHBASE_ROOTDIR(String HBASE_ROOTDIR) {
        this.HBASE_ROOTDIR = HBASE_ROOTDIR;
    }

    public String getDFS_NAME_DIR() {
        return DFS_NAME_DIR;
    }

    public void setDFS_NAME_DIR(String DFS_NAME_DIR) {
        this.DFS_NAME_DIR = DFS_NAME_DIR;
    }

    public String getDFS_DATA_DIR() {
        return DFS_DATA_DIR;
    }

    public void setDFS_DATA_DIR(String DFS_DATA_DIR) {
        this.DFS_DATA_DIR = DFS_DATA_DIR;
    }

    public String getFS_DEFAULT_NAME() {
        return FS_DEFAULT_NAME;
    }

    public void setFS_DEFAULT_NAME(String FS_DEFAULT_NAME) {
        this.FS_DEFAULT_NAME = FS_DEFAULT_NAME;
    }

    public String getSOLR_SERVER() {
        return SOLR_SERVER;
    }

    public void setSOLR_SERVER(String SOLR_SERVER) {
        this.SOLR_SERVER = SOLR_SERVER;
    }

    public String getHBASE_TABLE_NAME() {
        return HBASE_TABLE_NAME;
    }

    public void setHBASE_TABLE_NAME(String HBASE_TABLE_NAME) {
        this.HBASE_TABLE_NAME = HBASE_TABLE_NAME;
    }

    public String getHBASE_TABLE_FAMILY() {
        return HBASE_TABLE_FAMILY;
    }

    public void setHBASE_TABLE_FAMILY(String HBASE_TABLE_FAMILY) {
        this.HBASE_TABLE_FAMILY = HBASE_TABLE_FAMILY;
    }
}