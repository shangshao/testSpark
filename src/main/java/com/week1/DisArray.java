package com.week1;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by shangyongqiang on 2016/5/25.
 */
public class DisArray {

    public static List getList(String [] a){
        List list = new ArrayList();
        Map<String,Integer> map = new HashMap();
        for (int i = 0; i <a.length ; i++) {
            for (int j = i+1; j < a.length; j++) {
                //排除掉value为2的数据
                if(map.get(a[i])!=null ){
                    if(map.get(a[i])==2  ) break;
                }
                if(map.get(a[j])!=null){
                    if(map.get(a[j])==2)  break;
                }

                //两两字符串进行比较
                if(a[i].length()>=a[j].length()){

                    if(a[i].contains(a[j])){
                        map.put(a[i],1);
                        map.put(a[j],2);
                    }else{
                        map.put(a[i],1);
                        map.put(a[j],1);
                    }
                } else {

                    if(a[j].contains(a[i])){
                        map.put(a[j],1);
                        map.put(a[i],2);
                    }else{
                        map.put(a[i], 1);
                        map.put(a[j], 1);
                    }
               }
            }
        }
        for(String s :map.keySet()){
            if(map.get(s)==1){
//               list.add(s);
                Pattern p = null;
                Matcher m = null;
                String value = "";
                p = Pattern.compile("([\\u4e00-\\u9fa5]+)");
                m = p.matcher(s);
                while (m.find()) {
                    value +=m.group(0);
                }
                if(value!=null){
                    list.add(value);
                }
            }

        }
        return list;
    }
    public static void main(String [] args){
        String a="吃什么,吃什么fs会,吃什么749849会引起,吃什么会fsdfd引起痛风";
        System.out.println(getList(a.split(",")).toString());
    }
}
