package DSPPCode.spark.pagerank;

import scala.Tuple2;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class FlatMapToPairImpl extends FlatMapToPair {

    @Override
    public Iterator<Tuple2<String, Double>> call(Tuple2<Iterable<String>, Double> outsideWeight) throws Exception{

        Double weight = outsideWeight._2;
        List<String> Node = new ArrayList<>();
        outsideWeight._1.forEach(Node::add);
        List<Tuple2<String, Double>> list = new ArrayList<Tuple2<String, Double>>();
        for(String node:Node){
            list.add(new Tuple2<String, Double>(node, weight / Node.size()));
        }
        return list.iterator();
    }
}
