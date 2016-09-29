package com.job1;



import java.io.IOException;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.wltea.analyzer.lucene.IKAnalyzer;

/**
 * 相关的jar包
 * lucene-core-3.6.2.jar,lucene-memory-3.6.2.jar,
 * lucene-highlighter-3.6.2.jar,lucene-analyzers-3.6.2.jar
 * IKAnalyzer2012.jar
 *
 * 截取一片文章中频繁出现的关键字，并给予分组排序（倒叙），以数组格式返回n个关键字
 *
 * 并该类内部含有一个List2Map方法，可将重复<String>集合转换为Map<String, Integer>格式
 * 并算出该<String>重复次数，放入相应的value中
 */

/**
 * 获取文章关键字
 *  Created by shangyongqiang on 2016/7/26.
 */
public class TestKeyWord1 {
    /** 测试文章 */
    static String keyWord = "东来东往东来东往," +
            "彭羚等你回来,庞晓宇秋风刺伤了我的温柔," +
            "张学友还是觉得你最好,张雨生张惠妹最爱的人伤" +
            "我最深,卓依婷东南西北风,寂悸寂寞女神,刘小慧初恋情人," +
            "邝美云与龙共舞,冷漠司徒兰芳即使孤独也要走下去,亚东月光" +
            "落地的声音,王菲怀念,龙广我再也不相信爱情,降央卓玛走天涯" +
            ",熊天平许茹芸你的眼睛,刘梦风雨中奔跑的孩子,李易峰我爱" +
            "的人伤我最深,陈玉建永远爱你的,小山心痛,纳兰张天赋一生" +
            "痴情只为你,乔嘉别再让我为你受折磨,王璐你给的明天,七朵" +
            "组合不一样的我,蒙面哥,高进伤心的歌,林俊杰阿信黑暗骑" +
            "士,许美静遗憾,刀郎守候在凌晨200的伤心,龙梅子如果没" +
            "遇见你,天佑送给黑粉,蒋雪儿因为你爱上他,薛之谦丑八" +
            "怪,唐古原来你从来没有爱过我,辛莉恩我们都会幸福" +
            "的,王羽泽我们分手了,降央卓玛爱江山更爱美人,祁" +
            "隆叹情缘,许茹芸突然想爱你,王力宏公转自转,赵仰瑞" +
            "好出门不如赖在家,林俊杰金莎被风吹过的夏天,何思渔" +
            "寂寞的歌,0,0,1,0,0,0,0,萧敬腾王妃,周华健永远陪" +
            "伴你,冷漠郭玲迷茫的爱,庄心妍心有所爱,高凌风心上" +
            "人,薛之谦小孩,萧敬腾会痛的石头,郑源包容,李双江草" +
            "原之夜,乌兰图雅梦草原,梁咏琪胆小鬼,高安思思念念全是" +
            "你,张宇爱要让全世界知道,乐小虎天空挂念,云非非冷漠" +
            "看透爱情看透你,齐秦不让我的眼泪陪我过夜,moe刘玥小" +
            "贱未完成的爱恋,苏仨潘成uang里个,张敬轩罗宾,唐古究" +
            "竟是谁的错,干露露男人惭愧不惭愧,叶贝文情人鹤顶红,欢" +
            "子别这样对我,卓依婷金包银闽南语,吉克隽逸失眠的黑夜," +
            "王菲般若波罗蜜多心经,孟文豪爱的伤伤感版,蓝琪儿火火的" +
            "爱,叶丽仪昨夜泪痕,凤凰传奇郎的诱惑,凤凰传奇拜新年吉特" ;

    static String a="一个产品创意初期对用户的需求定义，在产品原型产出后，有时候会推倒重来。\n" +
            "为什么对用户理解遗失？用户是多样的，在设计初期没有对用户进行定义，闭门造车中，设计师很容易忘记用户雏形。因此，在设计初期需要用户画像来帮助设计师理解用户，用户画像在整个产品过程中具有指导作用。\n" +
            "用户画像的核心工作是为用户打标签，打标签的重要目的之一是为了让人能够理解并且方便计算机处理，如，可以做分类统计：喜欢红酒的用户有多少？喜欢红酒的人群中，男、女比例是多少？\n" +
            "也可以做数据挖掘工作：利用关联规则计算，喜欢红酒的人通常喜欢什么运动品牌？利用聚类算法分析，喜欢红酒的人年龄段分布情况？\n" +
            "大数据处理，离不开计算机的运算，标签提供了一种便捷的方式，使得计算机能够程序化处理与人相关的信息，甚至通过算法、模型能够“理解” 人。当计算机具备这样的能力后，无论是搜索引擎、推荐引擎、广告投放等各种应用领域，都将能进一步提升精准度，提高信息获取的效率。";
    /** 获取关键字个数 */
    private final static Integer NUM=20;
    /** 截取关键字在几个单词以上的数量 */
    private final static Integer QUANTITY=1;
    /**
     * 传入String类型的文章，智能提取单词放入list中
     * @param article
     * @param a
     * @return
     * @throws IOException
     */
    private static List<String> extract(String article,Integer a) throws IOException {
        List<String> list =new ArrayList<String>(); //定义一个list来接收将要截取出来单词
        IKAnalyzer analyzer = new IKAnalyzer(); //初始化IKAnalyzer
        analyzer.setUseSmart(true); //将IKAnalyzer设置成智能截取
        TokenStream tokenStream= //调用tokenStream方法(读取文章的字符流)
                analyzer.tokenStream("", new StringReader(article));
        while (tokenStream.incrementToken()) { //循环获得截取出来的单词
            CharTermAttribute charTermAttribute = //转换为char类型
                    tokenStream.getAttribute(CharTermAttribute.class);
            String keWord= charTermAttribute.toString(); //转换为String类型
            if (keWord.length()>a) { //判断截取关键字在几个单词以上的数量(默认为2个单词以上)
                list.add(keWord); //将最终获得的单词放入list集合中
            }
        }
        return list;
    }
    /**
     * 将list中的集合转换成Map中的key，value为数量默认为1
     * @param list
     * @return
     */
    private static Map<String, Integer> list2Map(List<String> list){
        Map<String, Integer> map=new HashMap<String, Integer>();
        for(String key:list){ //循环获得的List集合
            if (list.contains(key)) { //判断这个集合中是否存在该字符串
                map.put(key, map.get(key) == null ? 1 : map.get(key)+1);
            } //将集中获得的字符串放在map的key键上
        } //并计算其value是否有值，如有则+1操作
        return map;
    }
    /**
     * 提取关键字方法
     * @param article
     * @param a
     * @param n
     * @return
     * @throws IOException
     */
    public static String[] getKeyWords(String article,Integer a,Integer n) throws IOException {
        List<String> keyWordsList= extract(article,a); //调用提取单词方法
        Map<String, Integer> map=list2Map(keyWordsList); //list转map并计次数
        //使用Collections的比较方法进行对map中value的排序
        ArrayList<Entry<String, Integer>> list = new ArrayList<Entry<String,Integer>>(map.entrySet());
        Collections.sort(list, new Comparator<Map.Entry<String, Integer>>() {
            public int compare(Map.Entry<String, Integer> o1, Map.Entry<String, Integer> o2) {
                return (o2.getValue() - o1.getValue());
            }
        });
        if (list.size()<n) n=list.size(); //排序后的长度，以免获得到null的字符
        String[] keyWords=new String[n]; //设置将要输出的关键字数组空间
        for(int i=0; i< list.size(); i++) { //循环排序后的数组
            if (i<n) { //判断个数
                keyWords[i]=list.get(i).getKey(); //设置关键字进入数组
            }
        }
        return keyWords;
    }
    /**
     *
     * @param article
     * @return
     * @throws IOException
     */
    public static String[] getKeyWords(String article) throws IOException{
        return getKeyWords(article,QUANTITY,NUM);
    }
    public static void main(String[] args) {
        try {
            String [] keywords = getKeyWords(a);
            for(int i=0; i<keywords.length; i++){
                System.out.println(keywords[i]);
            }
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }
}
