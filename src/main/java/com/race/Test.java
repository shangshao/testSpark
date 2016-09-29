package com.race;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by shangyongqiang on 2016/4/20.
 */
public class Test {
    public static void main(String[] args) {
        String[] list = "吃什么,吃什么会,吃什么会引起,吃什么会引起痛风".split(",");
        List<String> lists=getList(list);
        for(int i=0;i<lists.size();i++){
            System.out.println(list[i]);
        }
    }
    static List getList(String [] a){
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
                list.add(s);
            }

        }
        return list;
    }
}
