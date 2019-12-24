import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.AggregateOperator;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import scala.annotation.ClassfileAnnotation;

/**
 * Created by 乐 on 2019/12/14.
 */
public class WordCount {
    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env=ExecutionEnvironment.getExecutionEnvironment();
        DataSource<String> dataSource = env.readTextFile("D:\\Intellij idea\\MyProject\\KafkaFlinkTCData\\Word.csv");
        AggregateOperator<Tuple2<String, Long>> sum = dataSource.map(new MyMap())
                .groupBy(0)
                .sum(1);
        sum.print();

    }
    static class MyMap implements MapFunction<String, Tuple2<String, Long>>{

        @Override
        public Tuple2<String, Long> map(String value) throws Exception {
            return new Tuple2<>(value,1L);
        }
    }
}
