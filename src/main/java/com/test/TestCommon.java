package com.test;

/**
 * Created by shangyongqiang on 2016/3/30.
 */
public class TestCommon {
    public static void main(String [] args){
        System.out.println(System.getProperty("user.dir"));

        String path=TestCommon.class.getResource("/").getPath();
        System.out.println(path);
    }

}
