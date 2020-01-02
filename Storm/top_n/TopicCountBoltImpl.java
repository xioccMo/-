package DSPPCode.storm.top_n;
import java.util.*;
import java.util.regex.*;

import org.apache.storm.generated.GlobalStreamId;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.MessageId;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

public class TopicCountBoltImpl extends TopicCountBolt {

    public void processTweet(String tweet){

        Pattern r = Pattern.compile("#(.*?)[\\p{P}]*?([\\s]|$)");
        Matcher m = r.matcher(tweet);
        while(m.find()){
            String topic = m.group(1);
            if(!counter.containsKey(topic)){
                counter.put(topic, 0);
            }
            int count = counter.get(topic);
            counter.put(topic, count+1);
        }
    }

    public void reportTopNToPrinter() {
        List<Map.Entry<String, Integer>> list = new ArrayList<Map.Entry<String, Integer>>(counter.entrySet());
        Collections.sort(list, new Comparator<Map.Entry<String, Integer>>() {
            @Override
            public int compare(Map.Entry<String, Integer> o1, Map.Entry<String, Integer> o2) {
                return o2.getValue() - o1.getValue();
            }
        });
        int num = 0;
        String result = "";
        for (HashMap.Entry<String, Integer> entry : list) {
            if(num >= topN){
                break;
            }
            num += 1;
            result += entry.getKey()+ "," + String.valueOf(entry.getValue()) + "\n";
        }
        collector.emit(new Values(result));
    }
}
