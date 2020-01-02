package DSPPCode.spark.broadcast_join;

import org.python.antlr.op.In;

import java.util.*;
import org.apache.spark.broadcast.Broadcast;

import java.util.Iterator;
import java.util.Map;

public class BroadcastJoinMapperImpl extends BroadcastJoinMapper{

    public Iterator<String> call(String order){


        List<String> list = new ArrayList<String>();
        String[] data = order.split(",");
        Map<Long, String> map = persons.getValue();
        Long key = Long.valueOf(data[2]);
        if(map.containsKey(key)) {
            list.add(map.get(Long.valueOf(data[2])) + "," + data[1]);
        }
        return list.iterator();
    }
}
