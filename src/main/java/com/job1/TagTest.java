package com.job1;


import com.google.gson.Gson;
import com.hankcs.hanlp.HanLP;
import com.hankcs.hanlp.seg.common.Term;
import net.sf.json.JSONObject;

import java.io.File;
import java.lang.reflect.Type;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by shangyongqiang on 2016/7/14.
 */
public class TagTest {
    public static void main (String []args){
        //分析标签的结构
        //一级标签 二级标签  三级标签

            String str = "123abc这个中文cde123abc也要提取123ab";
            Pattern p = null;
            Matcher m = null;
            String value = "";
            p = Pattern.compile("([\\u4e00-\\u9fa5]+)");
            m = p.matcher(str);
            while (m.find()) {
                value +=m.group(0);
        }
        System.out.println(value);

//        String pa1=this.class.getResource("/hanlp.properties").getPath();
//        String fi= pa1+"hanlp.properties";
//        File file =new File(pa1);
//        System.out.println(file.canRead());
//        System.out.println(pa1);
       String pa2= System.getProperty("user.dir");
//        String pt =Thread.currentThread().getContextClassLoader().getResource("/").getPath();
        System.out.println(pa2);
        Map map = new LinkedHashMap<String,String>();
        map.put("cc","11");
        map.put("bb","11");
        map.put("aa","11");
        map.put("cc","11");
        System.out.println(map.toString());
        map.put("bb", "22");
        Gson gson = new Gson();
        String json1 = gson.toJson(map);
        System.out.println(map.toString());
        String ou= (String) System.getProperties().get("user.dir");
        ou=ou.replaceAll("\\\\","/");
          ou+="/src/main/resources/data/dictionary/custom/CustomDictionary.txt.bin";
        System.out.println(ou);
        String li="8516216,1110107001019004,2,{\"pile_code\":\"1110107001019004\",\"inter_no\":\"1\",\"session_id\":\"000000516450\",\"user_id\":\"0000000000014925\",\"action\":\"1\",\"result\":\"1\",\"soc\":\"84\",\"time\":\"1471312216\",\"ecode\":\"0\"},2,1471312245";
        Map<String, String> ma = new HashMap<String, String>();
        
//        li.replaceAll("[\\{\\}]","\\|");
//        System.out.println(li);     "{"+line.split("\\{|\\}")[1]+"}"
        ma=gson.fromJson("{"+li.split("\\{|\\}")[1]+"}",Map.class);
        System.out.println();
        System.out.println(ma.toString());
        String cc="#######gdfgdfg##4545645645#";
        System.out.println(cc.lastIndexOf("#"));
        System.out.println("1.0L1.0T".replaceAll("L|T",""));
        String a ="cdcdcd";
        System.out.println(HanLP.extractKeyword(a,10));
        List<Term> segment = HanLP.segment(a);
        for (Term term : segment) {
            if (term.nature.startsWith("nsyq")) {
                System.out.println(term);
            }
        }
        System.out.println(HanLP.extractSummary(a, 5));
    }

}
