package DSPPCode.hadoop.k_means;

public class KMeansImpl extends KMeans {

    @Override
    public void kMeans(String inputPath, String oldCenterPath, String newCenterPath) throws Exception {
        System.out.println(newCenterPath);

        do {
            runOneStep(inputPath, oldCenterPath, newCenterPath);
        }while(!compareAndUpdateCenters(oldCenterPath, newCenterPath));
    }
}
