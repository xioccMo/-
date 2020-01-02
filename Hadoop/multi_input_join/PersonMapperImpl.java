package DSPPCode.hadoop.multi_input_join;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class PersonMapperImpl extends PersonMapper {

    public void map(LongWritable key, Text value, Mapper.Context context)
            throws IOException, InterruptedException{
        TextPair textPair = new TextPair();
        String[] data = value.toString().split("\t");

        textPair.setFlag(new Text("Person"));
        textPair.setData(new Text(data[1] + "\t" + data[2]));
        context.write(new Text(data[0]), textPair);
    }
}
