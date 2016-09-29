package com.week1;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

/**
 * Created by shangyongqiang on 2016/6/1.
 */
public class PaChong {
        static String url="http://www.cnblogs.com/zyw-205520/archive/2012/12/20/2826402.html";
        /**
         * @param args
         * @throws Exception
         */
        public static void main(String[] args)  {

            // TODO Auto-generated method stub
            List<String> list = new ArrayList();
            List<String> timelist = new ArrayList();
            List<String> phonelist = new ArrayList<String>();
            Map<String,String> map = new HashMap();
            String url ="http://www.3533.com";
            Document doc = null;
            try {
                doc = Jsoup.connect("http://www.3533.com/phone/").get();
            } catch (IOException e) {

            }
//            Elements hrefs = doc.select("a[href]");//class=pbox
            Elements ListDiv = doc.getElementsByAttributeValue("class","pbox");
            for (Element element :ListDiv) {
                Elements li = element.getElementsByTag("li");
                for(Element l:li){
                    Elements links = l.getElementsByTag("img");
                    Elements links1 = l.getElementsByTag("a");
                    for (int i = 0; i < links.size(); i++) {
                        String linkHref = links.get(i).attr("alt");
                        String linkText = links1.get(i).attr("href");
                        map.put(linkText.replaceAll("/",""),linkHref);
                        String lin=url+linkText;
                        list.add(lin);
//                        System.out.println(linkHref+","+linkText);
                    }
                }
            //遍历list中各个品牌的手机
                Elements notime = new Elements();
                for(String t:list){
                    Document type = null;
                    try {
                        type = Jsoup.connect(t).get();
                    //分年份
                    if(type.getElementsByAttributeValue("class","year").size()==0){
                         notime = type.getElementsByAttributeValue("class","modelbox");
                        for(Element element1 :notime){
                            Elements links = element1.getElementsByTag("a");
                            for(Element l :links){
                                String linkText =url+l.attr("href");
                                phonelist.add(linkText);
                            }
                        }
                    }else{
                    Elements time = type.getElementsByAttributeValue("class","year");
                    for(Element element1 :time){
                        Elements links = element1.getElementsByTag("a");
                        for(Element l :links){
                            String linkText =t+l.attr("href");
                            timelist.add(linkText);
                        }
                    }
                    }
                    } catch (IOException e) {

                    }

                }
             //遍历各个年份下的手机型号

                for(String t:timelist){
                    Document type = null;
                    try {
                        type = Jsoup.connect(t).get();
                    } catch (IOException e) {

                    }
                    Elements time = type.getElementsByAttributeValue("class","modelbox");
                    for(Element element1 :time){
                        Elements links = element1.getElementsByTag("a");
                        for(Element l :links){
                            String linkText =url+l.attr("href");
                            phonelist.add(linkText);
                        }
                    }

                }

//                for(String t:phonelist){
//                    System.out.println(t);
//                }
                //取出这个型号下的操作系统
                for(String t:phonelist){
                    Document type = null;
                    try {
                        type = Jsoup.connect(t).get();
                        //                    String title = type.getElementsByTag("title").get(0).toString().split(" ");
                        Elements time = type.getElementsByAttributeValue("class","lr");
                        String a ="";
                        for(Element element1 :time){
                            Elements lii = element1.getElementsByTag("li");
                            for (Element l : lii) {
                                Elements links = l.getElementsByAttributeValue("class", "sl");
                                Elements lin = l.getElementsByAttributeValue("class","sr");
                                for (int i = 0; i <links.size() ; i++) {
                                    if(links.get(i).text().contains("操作系统")){
                                        a=lin.get(i).text();
                                    }
                                }
//                            Elements links1 = l.getElementsByTag("a");
//                            for (int i = 0; i < links.size(); i++) {
//                                String linkHref = links.get(i).attr("alt");
//                                String linkText = links1.get(i).attr("href");
//                                String lin=url+linkText;
//                                list.add(lin);
////                        System.out.println(linkHref+","+linkText);
//                            }
                            }

                        }
                        String [] b = t.split("\\/");
                        String pin = b[3];
                        String ty=b[4];
                        String pincn=map.get(b[3]);
                        System.out.println(pin+","+pincn+","+ty+","+a);
                    } catch (IOException e) {

                    }

                    }
                }


//******************************************






//                for (Element link : links) {
//                    String linkHref = link.attr("alt");
////                    String linkText = link.text().trim();
//                    System.out.println(linkHref);
////                    System.out.println(linkText);
//            article();
        }

//    System.out.println(hrefs);
//            System.out.println("------------------");
//            System.out.println(hrefs.select("[href^=http]"));

//            BolgBody();

//            Blog();
        /*
         * Document doc = Jsoup.connect("http://www.oschina.net/")
         * .data("query", "Java") // 请求参数 .userAgent("I ’ m jsoup") // 设置
         * User-Agent .cookie("auth", "token") // 设置 cookie .timeout(3000) //
         * 设置连接超时时间 .post();
         */// 使用 POST 方法访问 URL

        /*
         * // 从文件中加载 HTML 文档 File input = new File("D:/test.html"); Document doc
         * = Jsoup.parse(input,"UTF-8","http://www.oschina.net/");
         */


        /**
         * 获取指定HTML 文档指定的body
         * @throws IOException
         */
        private static void BolgBody() throws IOException {
            // 直接从字符串中输入 HTML 文档
            String html = "<html><head><title> 开源中国社区 </title></head>"
                    + "<body><p> 这里是 jsoup 项目的相关文章 </p></body></html>";
            Document doc = Jsoup.parse(html);
            System.out.println(doc.body());


            // 从 URL 直接加载 HTML 文档
            Document doc2 = Jsoup.connect(url).get();
            String title = doc2.body().toString();
            System.out.println(title);
        }

        /**
         * 获取博客上的文章标题和链接
         */
        public static void article() throws IOException {
            Document doc;
                doc = Jsoup.connect("http://www.3533.com/motorola/moto_g_lte/").get();
                Elements time = doc.getElementsByAttributeValue("class", "lr");
                String a = "无";
                for (Element element1 : time) {
                    Elements lii = doc.getElementsByTag("li");
                    for (Element l : lii) {
                        Elements links = l.getElementsByAttributeValue("class","sl");
                        Elements lin = l.getElementsByAttributeValue("class","sr");
                        for (int i = 0; i <links.size() ; i++) {
                            if(links.get(i).text().contains("操作系统")){
                                System.out.println(lin.get(i).text());
                            }
                        }
//                        for (Element ll : links) {
//
//                        }

                    }
                }

//            System.out.println(a);
            }

        /**
         * 获取指定博客文章的内容
         */
        public static void Blog() {
            Document doc;
            try {
                doc = Jsoup.connect("http://www.cnblogs.com/zyw-205520/archive/2012/12/20/2826402.html").get();
                Elements ListDiv = doc.getElementsByAttributeValue("class","postBody");
                for (Element element :ListDiv) {
                    System.out.println(element.html());
                }
            } catch (IOException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }

        }

    }

//     static class TaoThread extends Thread{
//        private Document doc;
//
//        public TaoThread() {
//        }
//        public TaoThread(Document doc) {
//            this.doc = doc;
//        }
//        @Override
//        public void run() {
//            Elements ids = doc.getElementsByClass("id_td");
//            Elements names = doc.getElementsByClass("zh_td");
//            for(Element id:ids){
//                System.out.println("取出id："+id.text());
//            }
//            for(int i=0;i<ids.size();i++){
//                System.out.println("id:"+ids.get(i).text()+" zh:"+names.get(i).text());
//            }
//        }
//    }
