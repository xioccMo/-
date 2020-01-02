package DSPPCode.flink.twitter_json_filter;
import java.util.*;
import java.util.regex.*;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.util.Collector;

public class ParseJsonAndSplitImpl extends ParseJsonAndSplit {
    public void flatMap(String value, Collector<Tuple2<String, Integer>> collector) throws Exception {
        jsonParser = new ObjectMapper();
        Map<String, String> map = jsonParser.readValue(value, Map.class);
        if(!value.contains("lang\":\"en")){
            return;
        }

        for(String word: map.get("text").split(" ")) {
            word = word.toLowerCase();
            Tuple2<String, Integer> tuple2 = new Tuple2<>();
            tuple2.setFields(word, 1);
            collector.collect(tuple2);
        }
    }
}
