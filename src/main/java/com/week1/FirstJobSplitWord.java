package com.week1;

import org.ansj.splitWord.analysis.ToAnalysis;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.IOUtils;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.wltea.analyzer.lucene.IKAnalyzer;

import java.io.*;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by shangyongqiang on 2016/5/23.
 */
public class FirstJobSplitWord {
    public static void main(String[]args) throws IOException {
        //用户的数据一天的数据(用户的唯一标示,还有搜索关键词的集合)
        //对用户的搜索关键词进行分词
        //读取文件进行分词
        //hdfs 上的文件路径是/user/hive/warehouse/getdatas.db/s_keys
        String str = null;
        Configuration conf= new Configuration();
        FileSystem fileSystem = FileSystem.get(conf);
        Analyzer analyzer = new IKAnalyzer(false);
        for(File path:readfile("hdfs://192.168.3.171//user/hive/warehouse/getdatas.db/s_keys")){
            BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream(new File("/user/hive/warehouse/getdatas.db/s_keys"))));
            while((str=br.readLine())!=null){
                String[] a =str.split("\\t");
                StringReader reader = new StringReader(a[1]);
                TokenStream ts = analyzer.tokenStream("", reader);
                CharTermAttribute term=ts.getAttribute(CharTermAttribute.class);
                StringBuilder s = new StringBuilder();
                s.append(a[0]+",");
                while(ts.incrementToken()){
                    s.append(term.toString()+" ");
                }
//            System.out.println(s);


                FileSystem fs = FileSystem.get(URI.create("/shang/macdata/part-r-00000"), conf);
                FSDataInputStream is = fileSystem.open(new Path("hdfs://dmp01:9000/part-r-00000"));
                FSDataOutputStream out = fs.append(new
                        Path("/shang/macdata/part-r-00000"));
                IOUtils.copyBytes(is, out, 4096, true);




                s.append("\n");
//                BufferedWriter out = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(new File(""), true)));
//                out.write(String.valueOf(s));
//                out.close();
//                reader.close();
            }
            analyzer.close();
        }
        BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream(new File("/user/hive/warehouse/getdatas.db/s_keys"))));
        while((str=br.readLine())!=null){
            String[] a =str.split("\\t");
            StringReader reader = new StringReader(a[1]);
            TokenStream ts = analyzer.tokenStream("", reader);
            CharTermAttribute term=ts.getAttribute(CharTermAttribute.class);
            StringBuilder s = new StringBuilder();
            s.append(a[0]+",");
            while(ts.incrementToken()){
                s.append(term.toString()+" ");
            }
//            System.out.println(s);
            s.append("\n");
            BufferedWriter out = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(new File(""), true)));
            out.write(String.valueOf(s));
//            readfile("D:\\SogouC-UTF8\\UTF8\\test\\ClassFile\\C000010");
            out.close();
            reader.close();
        }
        analyzer.close();
//        String word  ="途胜报价及图片2015款,高山汽车售价,汉兰达,hld,高山汽车,HLD";
//        System.out.println(ToAnalysis.parse(word));
    }


//读取目录下的文件列表
    public static List<File> readfile(String filepath) throws FileNotFoundException {
        List<File> list = new ArrayList<File>();
        try {

            File file = new File(filepath);
            if (!file.isDirectory()) {
//                System.out.println("文件");
//                System.out.println("path=" + file.getPath());
//                System.out.println("absolutepath=" + file.getAbsolutePath());
//                System.out.println("name=" + file.getName());

            } else if (file.isDirectory()) {
                String[] filelist = file.list();
                for (int i = 0; i < filelist.length; i++) {
                    File readfile = new File(filepath + "\\" + filelist[i]);
                    if (!readfile.isDirectory()) {
                        list.add(readfile);
//                        System.out.println("absolutepath="
//                                + readfile.getAbsolutePath());
//                        System.out.println("name=" + readfile.getName());

                    } else if (readfile.isDirectory()) {
                        readfile(filepath + "\\" + filelist[i]);
                    }
                }

            }

        } catch (FileNotFoundException e) {
            System.out.println("readfile()   Exception:" + e.getMessage());
        }
        return list;
    }

}
