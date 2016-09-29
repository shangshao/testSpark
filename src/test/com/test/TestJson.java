package com.test;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;

import java.lang.reflect.Array;
import java.util.List;
import java.util.Map;

/**
 * Created by shangyongqiang on 2016/8/31.
 */
public class TestJson {
    public static void main(String[] args) {
        String a ="AF9E1C7E0645CA1F751D5D4CECD3411F|JKLMNOPQRSTU|00040008015| [ { \" skuid\":\"1637897360\",\"price\":\"499.00\"},{\"skuid\":\"11986492\",\"price\":\"60.00\"},{\"skuid\":\"11794224\",\"price\":\"37.60\"},{\"skuid\":\"117942\n" +
                "                 29\",\"price\":\"59.10\"},{\"skuid\":\"11808389\",\"price\":\"44.80\"},{\"skuid\":\"11829772\",\"price\":\"107.00\"},{\"skuid\":\"11895923\",\"price\":\"63.20\"},{\"skuid\":\"11909384\",\"price\":\"71.20\"},{\"skuid\":\"11896488\"\n" +
                "                 ,\"price\": \" 78.40\"} ] |2016083100|SH|2";
        Gson gson  = new Gson();
        List<Map<String, String>> o =(List<Map<String, String>>) gson.fromJson(a.split("\\|")[3], new TypeToken<List<Map<String, String>>>() {
        }.getType());
        for (Map<String, String> stringStringMap : o) {

            System.out.println(   stringStringMap.get("skuid"));
        }

        System.out.println( a.replaceAll("\\{|\\}|\\[|\\]|\\\"", ""));

    }
}
