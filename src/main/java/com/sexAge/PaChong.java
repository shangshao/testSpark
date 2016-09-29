package com.sexAge;

import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by shangyongqiang on 2016/6/23.
 */
public class PaChong {
    /**
     * @param args
     * @throws Exception
     */
    public static void main(String[] args) {

            get("http://www.pc6.com/pc/nanshengapp/2/");
        get("http://www.pc6.com/pc/nanshengapp/");
//        get("http://www.pc6.com/pc/nvshengapp/3/");
//        get("http://www.pc6.com/pc/nvshengapp/4/");

    }

        public static void get(String url){
            Document doc = null;
            try {
                doc = Jsoup.connect(url).get();
            } catch (IOException e) {

            }
            Elements ListDiv = doc.getElementsByAttributeValue("class", "clearfix mainCont");
            for (Element element : ListDiv) {
                Elements a = element.getElementsByTag("li");
                for(Element c : a){
                    Elements b = c.getElementsByTag("strong");
                        String linkHref = b.get(0).text();
                        System.out.println(linkHref);
                }


            }
        }

}
