package DSPPCode.hadoop.multi_join;

import org.apache.hadoop.io.Text;

import java.io.IOException;
import java.util.List;
import java.util.ArrayList;

public class MultiJoinReducerImpl extends MultiJoinReducer {
    public void reduce(Text key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {
        if (row == 0) {
            context.write(new Text("companyname"), new Text("addressname"));
            row++;
        }
        List<String> Company = new ArrayList<String>();
        List<String> Address = new ArrayList<String>();

        for (Text value : values) {
            String text = value.toString();
            if (text.endsWith(" ")) {
                Company.add(text.substring(0, text.length()-1));
            } else {
                Address.add(text);
            }
        }
        for (String company : Company) {
            for (String address : Address) {
                context.write(new Text(company), new Text(address));
            }
        }
    }
}
