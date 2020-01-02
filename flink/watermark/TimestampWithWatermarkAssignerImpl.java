package DSPPCode.flink.watermark;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.watermark.Watermark;

import javax.annotation.Nullable;

public class TimestampWithWatermarkAssignerImpl extends TimestampWithWatermarkAssigner {

    private long watermark = 0;
    private long time = 0;
    @Nullable
    @Override
    public Watermark getCurrentWatermark() {
        return new Watermark(time - MAX_OUT_OF_ORDER);
    }

    @Override
    public long extractTimestamp(Tuple2<Long, Integer> tuple, long unused) {
        time = Math.max(tuple.f0, time);
        return tuple.f0;
    }
}
