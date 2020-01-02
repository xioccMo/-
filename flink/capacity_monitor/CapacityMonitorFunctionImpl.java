package DSPPCode.flink.capacity_monitor;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

public class CapacityMonitorFunctionImpl extends CapacityMonitorFunction {
    @Override
    public void flatMap(Tuple2<String, Integer> tuple, Collector<Tuple2<String, Boolean>> collector) throws Exception {
        if(count.value() == null){
            count.update(0);
        }
        int origin  = count.value();
        count.update(tuple.f1 + origin);
        if(origin <= THRESHOLD && count.value() > THRESHOLD){
            collector.collect(new Tuple2<>(tuple.f0, true));
        }else if(origin > THRESHOLD && count.value() <= THRESHOLD){
            collector.collect(new Tuple2<>(tuple.f0, false));
        }
    }
}
