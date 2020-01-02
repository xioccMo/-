package DSPPCode.storm.window_join;

import DSPPCode.storm.window_join.util.FileProcess;
import org.apache.storm.tuple.Tuple;

import java.io.BufferedWriter;

public class PrinterBoltImpl extends PrinterBolt {

    public PrinterBoltImpl(String outputFile) {
        super(outputFile);
    }

    public String parseTuple(Tuple tuple)
    {
        return "[" + String.valueOf(tuple.getInteger(0)) + ", " + tuple.getString(2) + ", " + String.valueOf(tuple.getInteger(1)) + "]\n";
    }

    public void saveResult(String outputFile, String result){
        BufferedWriter bufferedWriter = FileProcess.getWriter(outputFile);
        FileProcess.write(result, bufferedWriter);
    }
}
