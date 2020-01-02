package DSPPCode.spark.k_means;

import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;

import java.lang.reflect.Array;
import java.util.*;

public class KMeansImpl extends KMeans {


    /**
     * TODO 聚类点集合centers中找出距离目标点centers最近的一个聚类点, 返回该聚类点在集合中的索引位置.
     * 输入: 目标点point与聚类点集合centers.
     * 输出: 所求聚类点在集合中的索引位置, 类型为int, 位置从0开始计算.
     */
    @Override
    public int closestPoint(Vector point, List<Vector> centers){
        int num = 0;
        double mindis = squaredDistance(point, centers.get(0));
        for(int i = 1; i < centers.size(); i++){
            double dis = squaredDistance(point, centers.get(i));
            if (dis < mindis){
                mindis = dis;
                num = i;
            }
        }
        return num;
    }

    /**
     * TODO 计算点与点的距离使用平方距离, 譬如(a,b)与(c,d)的距离为(a-c)^2+(b-d)^2, 即squaredDistance.
     * 输入: 两个点, 类型为Vector.
     * 输出: 两点的距离.
     */
    @Override
    public double squaredDistance(Vector point, Vector index){
        double[] a = point.toArray();
        double[] b = index.toArray();

        return Math.pow(a[0] - b[0], 2) + Math.pow(a[1] - b[1], 2);
    }
}
