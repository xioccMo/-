package DSPPCode.spark.pi;

public class PiSimulatorImpl extends PiSimulator {

    @Override
    public Integer call(Object unused){
        if (Math.pow(Math.random() * 2 - 1, 2) + Math.pow(Math.random() * 2 - 1, 2) > 1) {
            return 0;
        }
        return 1;
    }
}
