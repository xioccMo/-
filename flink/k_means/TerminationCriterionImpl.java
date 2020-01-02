package DSPPCode.flink.k_means;

import DSPPCode.flink.k_means.util.Centroid;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.operators.FilterOperator;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;

public class TerminationCriterionImpl extends TerminationCriterion {
    @Override
    public FilterOperator<Tuple2<Tuple3<Integer, Double, Double>, Tuple3<Integer, Double, Double>>> getTerminatedDataSet(DataSet<Centroid> newCentroids, DataSet<Centroid> oldCentroids) throws Exception {
        return newCentroids.join(oldCentroids)
                .where(new KeySelector<Centroid, Integer>() {
                    @Override
                    public Integer getKey(Centroid centroid) throws Exception {
                        return centroid.id;
                    }
                }).equalTo(new KeySelector<Centroid, Integer>() {
                    @Override
                    public Integer getKey(Centroid centroid) throws Exception {
                        return centroid.id;
                    }
                }).map(new MapFunction<Tuple2<Centroid, Centroid>, Tuple2<Tuple3<Integer, Double, Double>, Tuple3<Integer, Double, Double>>>() {
                    @Override
                    public Tuple2<Tuple3<Integer, Double, Double>, Tuple3<Integer, Double, Double>> map(Tuple2<Centroid, Centroid> centroidCentroidTuple2) throws Exception {
                        return new Tuple2<>(new Tuple3<Integer, Double, Double>(centroidCentroidTuple2.f0.id, centroidCentroidTuple2.f0.x, centroidCentroidTuple2.f0.y),
                                new Tuple3<Integer, Double, Double>(centroidCentroidTuple2.f1.id, centroidCentroidTuple2.f1.x, centroidCentroidTuple2.f1.y));
                    }
                }).filter(new FilterFunction<Tuple2<Tuple3<Integer, Double, Double>, Tuple3<Integer, Double, Double>>>() {
                    @Override
                    public boolean filter(Tuple2<Tuple3<Integer, Double, Double>, Tuple3<Integer, Double, Double>> tuple3Tuple3Tuple2) throws Exception {
                        return !(Math.sqrt(Math.pow(tuple3Tuple3Tuple2.f0.f1 - tuple3Tuple3Tuple2.f1.f1, 2) + Math.pow(tuple3Tuple3Tuple2.f0.f2 - tuple3Tuple3Tuple2.f1.f2, 2)) < EPSILON);
                    }
                });
    }
}
