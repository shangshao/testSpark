package com.week1;







import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import org.json.JSONObject;

import java.util.*;

/**
 * Created by shangyongqiang on 2016/5/25.
 */
public class Test1 {
    //测试汉字以及个数
    public static void main(String[] args) {
        String str = "/chuangya/";
        String a=str.replaceAll("/","");
        System.out.println(a);
        Map map = new TreeMap();
        map.put(1.4,"hh");
        map.put(1.2,"w");
        map.put(1.4,"4");
        map.put(1.5,"rt");
        map.put(1.0,"gfg");
        map.put(1.6,"vx");
        map.put(1.3,"bb");
        map.put(1.1,"k");
        System.out.println(map.toString());
        List list =new ArrayList();
        list.add("1.4:hh");
        list.add("1.2:w");
        list.add("1.4:4");
        list.add("1.5:rt");
        list.add("1.0:gfg");
        list.add("1.6:vx");
        list.add("1.3:bb");
        list.add("1.1:k");
        Collections.sort(list);
        Collections.reverse(list);
        System.out.println(list.toString());
        Gson gson = new Gson();
       String b= gson.toJson(map);
        System.out.println(b);
        b=b.replace("{", "[{");
        b=b.replace("}","}]");
        b=b.replaceAll(",", "},{");
        System.out.println(b);
//        List  o = gson.fromJson(b, new TypeToken<List<String>>() {}.getType());
////        Collections.reverse(o);
////        Collections.sort(o);
//        System.out.println(o.toString());

    }
}