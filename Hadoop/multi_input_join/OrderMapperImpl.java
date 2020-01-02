package DSPPCode.hadoop.multi_input_join;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

import java.io.IOException;

public class OrderMapperImpl extends OrderMapper {

    public void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException{

        TextPair textPair = new TextPair();
        String[] data = value.toString().split("\t");

        textPair.setFlag(new Text("Order"));
        textPair.setData(new Text(data[1]));
        context.write(new Text(data[2]), textPair);

    }
}
