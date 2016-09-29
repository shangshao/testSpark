package com.test;

import org.apache.commons.io.FileUtils;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.wltea.analyzer.lucene.IKAnalyzer;

import java.io.*;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by shangyongqiang on 2016/4/8.
 */
public class IKtest {
    public static void main(String[] args) throws Exception {
        String path ="D:\\SogouC-UTF8\\UTF8\\test\\ClassFile";
        File fi = new File(path);
        String[] aa= fi.list();
        int a1 =0;
        for(String aaa:aa){
            String path1="\\"+aaa;
            String path2=path+path1;
            System.out.println(path2);
            a1++;
         Analyzer analyzer = new IKAnalyzer(false);
        File file = new File("D:\\SogouC-UTF8\\UTF8\\"+a1+".txt");
        if(!file.exists()){
            file.createNewFile();
        }
        for(File a:readfile(path2)){
            StringReader reader = new StringReader(FileUtils.readFileToString(a));
            TokenStream ts = analyzer.tokenStream("", reader);
            CharTermAttribute term=ts.getAttribute(CharTermAttribute.class);
            StringBuilder s = new StringBuilder();
            s.append(a1+",");
            while(ts.incrementToken()){
                s.append(term.toString()+" ");
            }
//            System.out.println(s);
            s.append("\n");
            BufferedWriter out = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(file, true)));
                out.write(String.valueOf(s));
//            readfile("D:\\SogouC-UTF8\\UTF8\\test\\ClassFile\\C000010");
            out.close();
            reader.close();
        }
        analyzer.close();

        }

//        Analyzer analyzer = new IKAnalyzer(false);
//        File file = new File("D:\\SogouC-UTF8\\UTF8\\shang.txt");
//        if(!file.exists()){
//            file.createNewFile();
//        }
//        for(File a:readfile("D:\\SogouC-UTF8\\UTF8\\test\\ClassFile\\C000010")){
//            StringReader reader = new StringReader(FileUtils.readFileToString(a));
//            TokenStream ts = analyzer.tokenStream("", reader);
//            CharTermAttribute term=ts.getAttribute(CharTermAttribute.class);
//            StringBuilder s = new StringBuilder();
//            s.append("1,");
//            while(ts.incrementToken()){
//                s.append(term.toString()+" ");
//            }
////            System.out.println(s);
//            s.append("\n");
//            BufferedWriter out = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(file, true)));
//                out.write(String.valueOf(s));
////            readfile("D:\\SogouC-UTF8\\UTF8\\test\\ClassFile\\C000010");
//            out.close();
//            reader.close();
//        }
//        analyzer.close();
//        StringReader reader = new StringReader(FileUtils.readFileToString(new File("D:\\SogouC-UTF8\\UTF8\\test\\ClassFile\\C000010\\0.txt")));
//        TokenStream ts = analyzer.tokenStream("", reader);
//        CharTermAttribute term=ts.getAttribute(CharTermAttribute.class);
//        StringBuilder s = new StringBuilder();
//        s.append("类别");
//        while(ts.incrementToken()){
//            s.append(term.toString()+" ");
//        }
//        System.out.print(s);
//        readfile("D:\\SogouC-UTF8\\UTF8\\test\\ClassFile\\C000010");
//        analyzer.close();
//        reader.close();
    }


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
