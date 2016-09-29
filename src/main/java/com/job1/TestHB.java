package com.job1;

/**
 * Created by shangyongqiang on 2016/7/11.
 */
public class TestHB {
    public static void main(String []args){
        String arg1="info:departure,info:arrival,info:date,info:bsid,info:count";
        arg1 = arg1.replaceAll("date", "dat");
        System.out.println(arg1);
    }
}
