package com.job1;

import com.hankcs.hanlp.HanLP;
import com.hankcs.hanlp.corpus.tag.Nature;
import com.hankcs.hanlp.seg.common.Term;
import com.hankcs.hanlp.tokenizer.NLPTokenizer;
import org.apache.commons.lang.StringUtils;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Created by shangyongqiang on 2016/7/27.
 */
public class Hanlp {
    public static void main(String[] args) {
//        HanLP.delcache();
        System.out.println("关键词的提取|");
//        AddEuma.addEnum(Nature.class,"car");
//        HanLP.delecach();
//词性的标注
//        HanLP.Config.enableDebug();
        List<Term> list = HanLP.segment("关键词的提取");
        for (Term a : list) {
            System.out.println(a.toString());
        }

        System.out.println(list.toString());
        List<Term> te = NLPTokenizer.segment("中国科学院计算技术研究所的宗成庆教授正在教授自然语言");

        for (Term term : te) {
            Nature nature = term.nature;

        }
        System.out.println(te);
//        Segment seg = new DijkstraSegment().enableCustomDictionary(false).enablePlaceRecognize(true).enableOrganizationRecognize(true);
//        Segment seg1 = new NShortSegment().enableCustomDictionary(false).enablePlaceRecognize(true).enableOrganizationRecognize(true);
//        seg.seg("不知道你在说什么");
//        System.out.println(seg1.seg("不知道你在说什么"));
//用户自定义词典
//动态插入
//        CustomDictionary.add("攻城狮");
//        //强行插入
//        CustomDictionary.insert("白富美","nz 1024");
//        System.out.println(CustomDictionary.add("单身狗","nz 1024 n 1"));
//        System.out.println(CustomDictionary.get("单身狗"));
//        String text = "攻城狮逆袭单身狗,赢取白富美,走上人生巅峰";
//        System.out.println(HanLP.segment(text));
//        Predefine.HANLP_PROPERTIES_PATH=(String)System.getProperties().get("java.class.path")+"/hanlp.properties";
//        System.out.println(Predefine.HANLP_PROPERTIES_PATH);

//        Segment nShortSegment = new NShortSegment().enableCustomDictionary(false).enablePlaceRecognize(true).enableOrganizationRecognize(true);
//        Segment shortestSegment = new ViterbiSegment().enableCustomDictionary(false).enablePlaceRecognize(true).enableOrganizationRecognize(true);
//        Segment segment = new ViterbiSegment();
        System.out.println(HanLP.segment("七莘路华茂路上海交通大学农学院尚永强哦哦走上人生巅峰三泉路保德路彭浦新村"));
        System.out.println(HanLP.segment("我开着东风标致屌丝回家吃饭哈哈哈哈"));
//        HanLP.Config.enableDebug();
        //利用组合方式  先取出自定义词语,然后再取出关键词
        Set<String> set1 =new HashSet<String>();
        for (Term a : HanLP.segment("我开着东风标致屌丝回家吃饭哈哈哈哈")) {
            if (a.nature.startsWith("syq") ) {
               set1.add(a.word);
            }
        }
        set1.addAll(HanLP.extractKeyword("我开着东风标致屌丝回家吃饭哈哈哈哈", 5));
        System.out.println(set1);
        System.out.println("*************************");
        System.out.println(HanLP.extractKeyword("首页,邦购网触屏版",10));
        System.out.println(StringUtils.join(HanLP.extractKeyword("首页,邦购网触屏版", 10), "|")) ;
        for (Term t : HanLP.segment("奶粉&nbsp;雅培&nbsp;雀巢&nbsp;味全&nbsp;多美滋&nbsp;美赞臣等&nbsp;每伴清清宝奶伴侣&nbsp;各种辅食&nbsp;钙铁锌贝亲奶瓶日常用品&nbsp;贝儿欣奶瓶及日常生活必备品&nbsp;各种纸尿裤&nbsp;拉拉裤&nbsp;日本花王&nbsp;好奇&nbsp;菲比&nbsp;帮宝适&nbsp;妈咪宝贝&nbsp;早教机&nbsp;各种益智玩具&nbsp;孕妇防辐射服")) {
            if (t.nature.startsWith("nsyq")) {
                System.out.print(t+",");
            }
        }
        System.out.println(HanLP.extractKeyword("宝山,上海,中国,寶山,上海,中國",5));
        System.out.println(HanLP.extractKeyword("板簧中心距拉线曲阳路玉田路路口上海市虹口区浦东",5));
        System.out.println(HanLP.extractKeyword("奶粉&nbsp;雅培&nbsp;雀巢&nbsp;味全&nbsp;多美滋&nbsp;美赞臣等&nbsp;每伴清清宝奶伴侣&nbsp;各种辅食&nbsp;钙铁锌贝亲奶瓶日常用品&nbsp;贝儿欣奶瓶及日常生活必备品&nbsp;各种纸尿裤&nbsp;拉拉裤&nbsp;日本花王&nbsp;好奇&nbsp;菲比&nbsp;帮宝适&nbsp;妈咪宝贝&nbsp;早教机&nbsp;各种益智玩具&nbsp;孕妇防辐射服", 5));
        System.out.println(HanLP.segment("板簧中心距拉线曲阳路玉田路路口上海市虹口区浦东"));
//        Segment segment = new CRFSegment();
//        segment.enablePartOfSpeechTagging(true);
//        System.out.println(segment.seg("择天记电视|qq阅读网页版|老九门|芭比宝贝的睡衣派对芭比宝贝的睡衣派对小游戏7k7k芭比宝贝的睡衣派|腾讯文学腾讯读书|煎饼侠|老九门小说|老九门小说阅读|朱丽叶宝贝的感恩节朱丽叶宝贝的感恩节小游戏7k7k朱丽叶宝贝的感恩|择天记电视剧|qq阅读网站是什么？"));
//        HanLP.copchche();
    }

}
