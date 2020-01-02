package DSPPCode.hadoop.ssp;

import org.apache.hadoop.io.Text;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class SimpleShortestPathsReducerImpl extends SimpleShortestPathsReducer {

    public void reduce(Text nodeKey, Iterable<Text> values, Context context)
            throws IOException, InterruptedException{
        String mindis = null;
        Node node = new Node();
        List<String> Dis = new ArrayList<String>();
        for(Text value: values){
            String[] data = value.toString().split("\t");
            if(data.length != 1) {
                node.FormatNode(value.toString());
                mindis = node.getDistance();
            } else{
                Dis.add(data[0]);
            }
        }

        for(String dis:Dis){
            if(mindis.equals("inf")){
                mindis = dis;
            }else if(Integer.valueOf(dis) < Integer.valueOf(mindis)){
                mindis = dis;
            }
        }
        isChange(node, mindis, context);
        node.setDistance(mindis);
        context.write(nodeKey, new Text(node.toString()));
    }
}
