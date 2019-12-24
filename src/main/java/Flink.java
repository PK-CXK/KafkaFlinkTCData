import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;
import org.apache.kafka.common.serialization.StringDeserializer;
import sun.nio.cs.FastCharsetProvider;

import javax.annotation.Nullable;
import javax.annotation.Resource;
import java.io.IOException;
import java.net.URISyntaxException;
import java.sql.Timestamp;
import java.util.*;

/**
 * Created by 乐 on 2019/12/4.
 */
public class Flink {
    public static void main(String[] args) throws Exception {
        //kafka配置文件
        Properties props=new Properties();
        props.put("bootstrap.servers", "192.168.64.135:9092,192.168.64.137:9092");
        props.put("zookeeper.connect", "192.168.64.135:2181,192.168.64.137:2181");
        props.put("group.id", "kf");//消费者组，只要group.id相同，就属于同一个消费者组
        props.put("enable.auto.commit", "true");//自动提交offset
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        //生成consumer
        FlinkKafkaConsumer<String> fkconsumer=new FlinkKafkaConsumer("topic",new SimpleStringSchema(),props);
        //生成环境导入kafka
        final StreamExecutionEnvironment env=StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        //DataStreamSource<String> ds=env.addSource(fkconsumer);
        DataStreamSource<String> ds = env.readTextFile("D:\\Intellij idea\\MyProject\\KafkaFlinkTCData\\UserBehavior.csv");
        //转为UserBehavior对象并设置时间戳和水位线
        boolean flag=true;
        SingleOutputStreamOperator<UserBehavior> ubds = ds
                .map(line -> {
            String[] split = line.split(",");
            return new UserBehavior(Long.parseLong(split[0]), Long.parseLong(split[1]), Long.parseLong(split[2]), split[3], Long.parseLong(split[4]));
        })
                .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<UserBehavior>(Time.seconds(3)) {
            @Override
            //要乘1000
            public long extractTimestamp(UserBehavior element) {
                return element.timeStamp*1000;
            }
        });
        //每五分钟统计1小时的topN, userId;itemId;itemCate;type;timeStamp;
        ubds
                .filter(x -> x.type.equals("pv"))
                .keyBy("itemId")
                .timeWindow(Time.hours(1), Time.minutes(5))
                .aggregate(new MyAggFunction(), new MyWinFunction())
                .keyBy("timeEnd")
                .process(new TopNItem(5)).print().setParallelism(1);
        env.execute();
    }
    static class MyAggFunction implements AggregateFunction<UserBehavior,Long,Long>{
        @Override
        public Long createAccumulator() {
            return 0L;
        }

        @Override
        public Long add(UserBehavior value, Long accumulator) {
            return accumulator+1;
        }

        @Override
        public Long getResult(Long accumulator) {
            return accumulator;
        }

        @Override
        public Long merge(Long a, Long b) {
            return a+b;
        }
    }
    public static class CountOut{
        //全为public才能当key
        public long itemId;
        public long count;
        public long timeEnd;
        public CountOut(){}
        public CountOut(long i, long c,long t){
            this.itemId=i;
            this.count=c;
            this.timeEnd=t;
        }
        @Override
        public String toString(){
            return "编号："+itemId+" 次数："+count;
        }

        public long getTimeEnd() {
            return timeEnd;
        }
    }
    //第三项必须是tuple
    static class MyWinFunction implements WindowFunction<Long,CountOut,Tuple,TimeWindow>{

        /**
         * Evaluates the window and outputs none or several elements.
         *
         * @param tuple  The key for which this window is evaluated.
         * @param window The window that is being evaluated.
         * @param input  The elements in the window being evaluated.
         * @param out    A collector for emitting elements.
         * @throws Exception The function may throw exceptions to fail the program and trigger recovery.
         */
        @Override
        public void apply(Tuple tuple, TimeWindow window, Iterable<Long> input, Collector<CountOut> out) throws Exception {
            Long itemId =tuple.getField(0);
            Long count=input.iterator().next();
            Long timeEnd=window.getEnd();
            //System.out.println("apply方法：  时间戳："+timeEnd);
            out.collect(new CountOut(itemId,count,timeEnd));
        }
    }
    static class TopNItem extends KeyedProcessFunction<Tuple,CountOut,String>{
        private int N;
        //保存TopN
        private ListState<CountOut> countState;
        @Override
        public void processElement(CountOut value, Context ctx, Collector<String> out) throws Exception {
            Iterator<CountOut> iterator = countState.get().iterator();
            List<CountOut> li=new ArrayList<>();
            while (iterator.hasNext()){
                li.add(iterator.next());
            }
            if(li.size()<N)
                countState.add(value);
            else{
                //找到里面最小的
                int minIndex=0;
                for(int i=1;i<N;i++){
                    if(li.get(i).count<li.get(minIndex).count){
                        minIndex=i;
                    }
                }
                //替换
                if(li.get(minIndex).count<value.count){
                    li.remove(minIndex);
                    li.add(value);
                }
                countState.update(li);
            }
            ctx.timerService().registerEventTimeTimer(value.getTimeEnd()+1);
        }
        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            countState=getRuntimeContext().getListState(new ListStateDescriptor<CountOut>("countState",CountOut.class));
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
            Iterator<CountOut> iterator = countState.get().iterator();
            List<CountOut> li=new ArrayList<>();
            while (iterator.hasNext()){
                li.add(iterator.next());
            }
            //清理
            countState.clear();
            //排序
            li.sort(new Comparator<CountOut>() {
                //负的是前面小
                @Override
                public int compare(CountOut o1, CountOut o2) {
                    if(o1.count<o2.count)
                        return 1;
                    else if(o1.count>o2.count)
                        return -1;
                    else
                        return 0;
                }
            });
            //输出
            StringBuffer res=new StringBuffer();
            res.append("======"+new Timestamp(timestamp-1)+"======"+"\n");
            for(int i=0;i<li.size();i++){
                res.append("商品编号："+li.get(i).itemId+"\n"+"点击量："+li.get(i).count+"\n");
            }
            out.collect(res.toString());
        }
        TopNItem(int n){
            this.N=n;
        }
    }

}
