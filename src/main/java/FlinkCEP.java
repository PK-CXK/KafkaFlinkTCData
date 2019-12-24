import javafx.beans.binding.ObjectExpression;
import javafx.event.Event;
import jdk.nashorn.internal.objects.annotations.Where;
import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.functions.PatternProcessFunction;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.shaded.netty4.io.netty.util.internal.SocketUtils;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.graph.StreamGraph;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

import javax.annotation.Nullable;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * Created by 乐 on 2019/12/11.
 */
//两秒内三次登录失败
public class FlinkCEP {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env=StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        SingleOutputStreamOperator<UserLog> UL = env
                .readTextFile("D:\\Intellij idea\\MyProject\\KafkaFlinkTCData\\UserLogIn.csv")
                .map(x -> {
                    String[] split = x.split(",");
                    return new UserLog(Long.parseLong(split[0]), split[1], split[2], Long.parseLong(split[3]));
                })
                .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<UserLog>(Time.seconds(1)) {
                    @Override
                    public long extractTimestamp(UserLog element) {
                        return element.timeStamp*1000;
                    }
                });
        Pattern pa=Pattern.<UserLog>begin("step1").where(new SimpleCondition<UserLog>() {
            @Override
            public boolean filter(UserLog value) throws Exception {
                return value.type.equals("fail");
            }
        }).followedBy("step2").where(new SimpleCondition<UserLog>() {
            @Override
            public boolean filter(UserLog value) throws Exception {
                return value.type.equals("fail");
            }
        }).times(2).within(Time.seconds(3));
        PatternStream<UserLog> ps=CEP.pattern(UL.keyBy(new KeySelector<UserLog, Long>() {
            @Override
            public Long getKey(UserLog value) throws Exception {
                return value.id ;
            }
        }),pa);
        ps.process(new PatternProcessFunction<UserLog, String>() {
            @Override
            public void processMatch(Map<String, List<UserLog>> match, Context ctx, Collector<String> out) throws Exception {
                List<UserLog> step1 = match.get("step2");
                Iterator<UserLog> iterator = step1.iterator();
                while (iterator.hasNext()){
                    UserLog next = iterator.next();
                    out.collect(next.id+"有风险 时间戳："+ next.timeStamp);
                }
            }
        }).print().setParallelism(1);
        env.execute();
    }
    static class UserLog{
        Long id;
        String ip;
        String type;
        Long timeStamp;
        UserLog(Long i,String p,String t,Long ts){
            this.id=i;
            this.ip=p;
            this.type=t;
            this.timeStamp=ts;
        }
    }
}
