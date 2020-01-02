package DSPPCode.flink.twitter_json_filter;

import org.apache.flink.api.java.tuple.Tuple2;
import scala.collection.mutable.HashMap;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;

public class FilterImpl extends Filter {

    /**
     * 给路径赋值，调用读取停词文件数据方法
     * 在实现类Impl中需要实现此构造方法
     *
     * @param stopWordPath 文件路径
     */
    FilterImpl(String stopWordPath) throws IOException {
        super(stopWordPath);
    }

    public void readStopWords(String stopWordPath) throws IOException {
        FileReader fr = new FileReader(stopWordPath);
        BufferedReader bf = new BufferedReader(fr);
        String str;
        while ((str = bf.readLine()) != null){
            stopWords.add(str.toLowerCase());
        }
    }

    public boolean filter(Tuple2<String, Integer> wordcount) throws Exception {
        if(wordcount.f1 < 4){
            return false;
        }else {
            for (String stopWord : stopWords) {
                if (stopWord.equals(wordcount.f0)) {
                    return false;
                }
            }
        }
        System.out.println(wordcount.f0);
        System.out.println(wordcount.f1);
        return true;
    }
}
