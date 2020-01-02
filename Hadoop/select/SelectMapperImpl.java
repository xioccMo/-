package DSPPCode.hadoop.select;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.util.StringTokenizer;
import java.io.IOException;

public class SelectMapperImpl extends SelectMapper{

    public void map(Object key, Text value, Context context)
            throws IOException, InterruptedException {
        if(value.toString().split("\t")[2].equals("shanghai"))
            context.write(value, NullWritable.get());
    }
}
