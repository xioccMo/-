package DSPPCode.giraph.count_vertex;

import org.apache.giraph.graph.Vertex;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;

import java.io.IOException;

public class CountVertexImpl extends CountVertex {
    @Override
    public void compute(Vertex<IntWritable, IntWritable, NullWritable> vertex, Iterable<IntWritable> iterable) throws IOException {
        if (getSuperstep() == 0){
            aggregate("countVertex", new IntWritable(1));
            sendMessage(vertex.getId(), vertex.getId());
        }else if(getSuperstep() == 1){
            vertex.setValue(getAggregatedValue("countVertex"));
        }
        vertex.voteToHalt();
    }
}
