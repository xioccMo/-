package DSPPCode.storm.window_join;

import org.apache.storm.bolt.JoinBolt;
import org.apache.storm.topology.base.BaseWindowedBolt.Duration;

import java.util.concurrent.TimeUnit;


public class StormJoinBoltImpl extends StormJoinBolt {
    public void  setJoinBolt(){

    }
    public JoinBolt getJoinBolt(){
        return new JoinBolt("ageSpout", "id")
                .join("genderSpout", "id", "ageSpout")
                .select("ageSpout:id, ageSpout:age, genderSpout:gender")
                .withTumblingWindow( new Duration(20000,  TimeUnit.MICROSECONDS) );
    }
}
