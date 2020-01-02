package DSPPCode.storm.slide_count_window;

import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.*;

public class SlideCountWindowBoltImpl extends SlideCountWindowBolt{

    private Map<String, Integer> id_count = new HashMap<>();
    private Map<String, String> value_append_map = new HashMap<>();


    @Override
    public void execute(Tuple tuple, BasicOutputCollector basicOutputCollector){
        String id = tuple.getString(0);
        if(!id_count.containsKey(tuple.getString(0))){
            id_count.put(id, 0);
            value_append_map.put(id, "");
        }
        int count = id_count.get(id) + 1;
        String string = value_append_map.get(id) + tuple.getString(1);
        if(count % 2 == 1){
            value_append_map.put(id, string);
        }else {
            value_append_map.put(id, tuple.getString(1));
            basicOutputCollector.emit(new Values(outputFormat(String.valueOf(id), string, String.valueOf(count / 2))));
        }
        id_count.put(id, count);
    }
}
