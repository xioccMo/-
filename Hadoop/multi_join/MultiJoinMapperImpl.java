package DSPPCode.hadoop.multi_join;

import org.apache.hadoop.io.Text;

import java.io.IOException;

public class MultiJoinMapperImpl extends MultiJoinMapper {
    public void map(Object key, Text value, Context context)
            throws IOException, InterruptedException{
        String[] data = value.toString().split("\t");
        if (data[0].equals("companyname") || data[0].equals("addressid")){
            return;
        }
        char[] a1 = data[0].toCharArray();
        if(a1.length == 1 && a1[0] >= '0' && a1[0] <= '9'){
            context.write(new Text(data[0]), new Text(data[1]));
        }else{
            context.write(new Text(data[1]), new Text(data[0] + " "));
        }





    }
}
