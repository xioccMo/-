package DSPPCode.flink.k_means;

import DSPPCode.flink.k_means.util.*;
import org.apache.flink.api.java.DataSet;

public class IterationStepImpl extends IterationStep {
    @Override
    public DataSet<Centroid> runStep(DataSet<Point> points, DataSet<Centroid> centroid) throws Exception {
        centroid = points.map(new SelectNearestCenter()).withBroadcastSet(centroid,"centroids")
                //计算每个点到最近聚类中心的计数
                .map(new CountAppender())
                .groupBy(0).reduce(new CentroidAccumulator())
                //计算新的聚类中心
                .map(new CentroidAverager());
        return centroid;
    }
}
