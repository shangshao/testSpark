package com.job1;

import java.sql.*;
import java.util.*;

/**
 * Created by shangyongqiang on 2016/7/6.
 */
public class TestHive {
    public static void main(String []args){
        List<Map<Integer,String>> list = new ArrayList();
        List li = new ArrayList();
        li.add(4);
        li.add(5);
        li.add(2);
        li.add(3);
        li.add(4);
        li.add(10);
        li.add(1);
        for (int i = 0; i <10 ; i++) {
            Map<Integer,String> map = new HashMap();
            list.add(map);
        }
        int i=0;
        for (Map a:list){
            a.put(i,"aa");
            i++;
        }
//        System.out.print(list.get(5).get(5));
        Collections.sort(li);
        System.out.println(li.get(li.size()-1));



        String[] lie="info:city,info:bsid,info:count".split("\\,");
        StringBuilder sb = new StringBuilder();
        String starttime="20160601";
        String outputPath="/user/shangyongqiang/hb/tianqi/";
        String tn="shang";
        sb.append("create table if not exists syq.");
        sb.append(tn+" (");
        for (String l:lie){
            sb.append(l.split("\\:")[1]+" string,");
        }
        sb.deleteCharAt(sb.lastIndexOf(","));
        sb.append(")  partitioned by (dt string) row format delimited fields terminated by '\\t' stored as textfile");
        String lo= "load data inpath '"+outputPath+"' into table syq."+tn+" partition (dt="+starttime+")";
        System.out.println(sb.toString());

        Connection conn =null;
        try {
            Class.forName("org.apache.hive.jdbc.HiveDriver");
            conn = DriverManager.getConnection("jdbc:hive2://192.168.3.171:10000/syq", "shangyongqiang", "");
            String sql = sb.toString();
            PreparedStatement ps = conn.prepareStatement(sql);
            PreparedStatement ps1 = conn.prepareStatement(lo);
            ps.execute();
            ps1.execute();
//            Statement stmt = conn.createStatement();
//            stmt.executeQuery(sql);
//            stmt.executeQuery(lo);
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
                if(conn!=null)
                    conn.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }
}




