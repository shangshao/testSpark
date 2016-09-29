package com.job1;


import com.hankcs.hanlp.HanLP;
import com.hankcs.hanlp.tokenizer.NLPTokenizer;
import org.wltea.analyzer.core.IKSegmenter;
import org.wltea.analyzer.core.Lexeme;

import java.io.*;
import java.util.List;

/**
 * Created by shangyongqiang on 2016/7/19.
 */
public class TestFenci {
    public static void main(String[] args) {
        String str = "马云和马化腾是两个牛人";
        System.out.println(str.length());
        IKAnalysis(str);
       System.out.println(NLPTokenizer.segment("马云和马化腾是两个牛人"));
//        Segment nShortSegment = new NShortSegment().enableCustomDictionary(false).enablePlaceRecognize(true).enableOrganizationRecognize(true);
//        Segment shortestSegment = new DijkstraSegment().enableCustomDictionary(false).enablePlaceRecognize(true).enableOrganizationRecognize(true);
//        String[] testCase = new String[]{
//                "今天，刘志军案的关键人物,山西女商人丁书苗在市二中院出庭受审。",
//                "刘喜杰石国祥会见吴亚琴先进事迹报告团成员",
//        };
//        for (String sentence : testCase)
//        {
//            System.out.println("N-最短分词：" + nShortSegment.seg(sentence) + "\n最短路分词：" + shortestSegment.seg(sentence));
//        }
        String a="太在意你,央金兰泽遇上你是我的缘,杨蔓爱上别人的人,高安一直爱着你,高进放爱走吧,卓依婷杜十娘,杨梓文祺垂帘听政,龙梅子你的爱情像闪电,波拉贺世哲爱不悔,成泉成全我吧重制版,孙露温柔的慈悲,邱永传十一年,云菲菲白狐电视剧聊斋狐仙主题曲,冷漠张冬玲我在红尘中遇见你,蒙面哥hold不住的爱,门丽亲爱的不要离开我好吗,卓依婷小草,陈诗妮蜕变的蝴蝶,许嵩内线,凤凰传奇荷塘月色铃声版,蔡琴落花流水,易欣别再伤害我,邓紫棋红蔷薇白玫瑰,张惠妹不要骗我,唐古放下你不容易,0,0,0,4,0,0,0,0,0,0,0,0,0,0,0,0,0,王菲匆匆那年,周传雄割舍,游鸿明五月的雪,王菲我愿意,降央卓玛手心里的温柔,龙梅子爱就这样,王麟雅美蝶,袁野那一个,金莎爱的魔法膜法世家年度代言歌曲,郑晓填如果寂寞了,乌兰图雅莫斯科郊外的晚上,钟镇涛让一切随风,韩红梨花又开放,曹越门丽今生无缘来生再聚,程响不要对我说,马健涛大街小巷都听我的歌,高安勿忘你独唱版,卓依婷免失志,李茂山迟来的爱,徐誉滕深深深深,黄琬婷蓝莲花,陈慧娴飘雪,冷漠李策旧情人旧情歌,路童英雄谱快四版,王绎龙上帝是个,许嵩断桥残雪";
        String b="对构造柱的构造要求则, 五四运动, 我国标准, 试述我国建筑, 试述我国建筑, 试述我国建筑, 试述我国建筑, 试述我国建筑, 对构造柱的构造要求有哪些, 试述我国建筑," +
                "构造柱的构造要求邮, 试述未来我国建筑行业的发展趋势, 试述我国建筑, 欢乐谷, 对构造住, 梅西退出国家队, 梅西事件";
//        Segment segment = HanLP.newSegment().enableNameRecognize(true);
//        Segment segment = HanLP.newSegment().enablePlaceRecognize(true);
        List<String> keywordList = HanLP.extractKeyword(b, 10);
            System.out.println(keywordList);


//        System.out.println(HanLP.extractSummary(a,5));

    }

    public static String IKAnalysis(String str) {
        StringBuffer sb = new StringBuffer();
        try {
            byte[] bt = str.getBytes();
            InputStream ip = new ByteArrayInputStream(bt);
            Reader read = new InputStreamReader(ip);
            IKSegmenter iks = new IKSegmenter(read, false);
            Lexeme t;
            while ((t = iks.next()) != null) {
                sb.append(t.getLexemeText() + " , ");
            }
            sb.delete(sb.length() - 1, sb.length());
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        System.out.println(sb.toString());
        return sb.toString();
    }
}
