package DSPPCode.spark.shuffle_join;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import scala.Serializable;
import scala.Tuple2;

public class ShuffleJoinImpl extends ShuffleJoin implements Serializable {




    @Override
    public JavaRDD<String> join(JavaPairRDD<Long, String> personRdd, JavaPairRDD<Long, String> orderRdd){
        JavaPairRDD<Long, Tuple2<String, String>> personorder = personRdd.join(orderRdd);
        JavaRDD<Tuple2<String, String>> result = personorder.values();
        JavaRDD<String> finalresult = result.map(
                new Function<Tuple2<String, String>, String>() {
                    @Override
                    public String call(Tuple2<String, String> tuple2) throws Exception {
                        return tuple2._1 + "," + tuple2._2;
                    }
                }
        );
        return finalresult;
    }
}
