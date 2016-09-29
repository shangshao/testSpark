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
public class Test {
    //测试汉字以及个数
    public static void main(String[]args){
        String regEx = "[\\u4e00-\\u9fa5]";
        String str = "解决,哈哈哈哈哈,我,我们一,我们一起去吃饭,我们一起,我们,我们一起去,我们一起去吃";
//        Pattern p = Pattern.compile(regEx);
//        Matcher m = p.matcher(str);
//        while(m.find()){
//            System.out.println(m.group(1));
//        }
//        System.out.println(m.groupCount());
        String [] a = str.split(",");
        Map<String,Integer> map = new HashMap();
        for (int i = 0; i <a.length ; i++) {
            for (int j = i+1; j < a.length; j++) {
                //排除掉value为2的数据
                if(map.get(a[i])!=null &&map.get(a[j])!=null){
                    if(map.get(a[i])==2 ||map.get(a[j])==2 ) break;
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
                System.out.println(s);
            }

        }

    }
}
