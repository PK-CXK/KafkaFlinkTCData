import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.mapreduce.CellCreator;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by 乐 on 2019/12/19.
 */
public class HbaseWriter extends RichSinkFunction {

    @Override
    public void open(org.apache.flink.configuration.Configuration parameters) throws Exception {
        super.open(parameters);

    }

    @Override
    public void invoke(Object value, Context context) throws Exception {
    }

    @Override
    public void close() throws Exception {
        super.close();
    }
}
