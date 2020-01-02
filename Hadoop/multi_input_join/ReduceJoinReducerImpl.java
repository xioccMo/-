package DSPPCode.hadoop.multi_input_join;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class ReduceJoinReducerImpl extends ReduceJoinReducer {

    public void reduce(Text key, Iterable<TextPair> values, Context context)
            throws IOException, InterruptedException {
        List<String> Person = new ArrayList<>();
        List<String> Order = new ArrayList<>();
        for (TextPair value : values) {
            if (value.getFlag().toString().equals("Person")) {
                Person.add(value.getData().toString());
            }
            else if(value.getFlag().toString().equals("Order")) {
                Order.add(value.getData().toString());
            }
        }
        for (String person : Person) {
            for (String order : Order) {
                Text text = new Text(person + "\t" + order + "\t");
                context.write(text, NullWritable.get());
            }
        }
    }
}
