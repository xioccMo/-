package DSPPCode.giraph.page_rank;

import org.apache.giraph.graph.Vertex;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;

import java.io.IOException;

public class PageRankImpl extends PageRank {
    @Override
    public void compute(Vertex<LongWritable, DoubleWritable, FloatWritable> vertex, Iterable<DoubleWritable> iterable) throws IOException {
        double num = (double) vertex.getNumEdges();
        if(getSuperstep() < MAX_SUPERSTEP) {
            if (getSuperstep() == 0) {
                sendMessageToAllEdges(vertex, new DoubleWritable(1 / num));
            } else {
                double total = (1 - D) / getTotalNumVertices();
                for (DoubleWritable msg : iterable) {
                    total += D * (msg.get());
                }
                vertex.setValue(new DoubleWritable(total));
                sendMessageToAllEdges(vertex, new DoubleWritable(total / num));
            }
        }else {
            vertex.voteToHalt();
        }
    }
}
