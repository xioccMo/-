package DSPPCode.hadoop.ssp;

import org.apache.hadoop.io.Text;

import java.io.IOException;

public class SimpleShortestPathsMapperImpl extends SimpleShortestPathsMapper {

    public void map(Text key, Text value, Context context)
            throws IOException, InterruptedException {


        int runCounter = context.getConfiguration().getInt("run.counter", 1);
        String[] dataValue = value.toString().split("\t");
        String keyValue = key.toString();
        if (runCounter == 1) {
            if (keyValue.equals("A")) {
                context.write(key, new Text("0\t" + value));
                Node node = new Node();
                node.FormatNode("0\t" + value);
                for (int i = 0; i < node.getNodeNum(); i++) {
                    context.write(new Text(node.getNodeKey(i)), new Text(node.getNodeValue(i)));
                }
            } else {
                context.write(key, new Text("inf\t" + value));
            }
        } else {
            Node node = new Node();
            node.FormatNode(value.toString());
            context.write(key,value);
            if(!node.getDistance().equals("inf")) {
                for (int i = 0; i < node.getNodeNum(); i++) {
                    context.write(new Text(node.getNodeKey(i)),
                            new Text(String.valueOf(Integer.valueOf(node.getNodeValue(i))
                                    + Integer.valueOf(node.getDistance()))));
                }
            }
        }
    }
}
