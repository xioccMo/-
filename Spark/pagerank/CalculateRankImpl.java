package DSPPCode.spark.pagerank;

import scala.Tuple2;

public class CalculateRankImpl extends CalculateRank {

    @Override
    public Tuple2<String, Double> call(Tuple2<String, Iterable<Double>> weight) throws Exception{
        double TotelWeight = 0;
        String Key = weight._1;
        for(Double UnitWeight: weight._2){
            TotelWeight += UnitWeight;
        }
        return new Tuple2<String, Double>(Key, TotelWeight * FACTOR + 1 - FACTOR);
    }
}