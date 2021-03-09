package flinkWatermarkDemo;

import org.apache.flink.util.Collector;

public class MyProcessFunction extends org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction {
    @Override
    public void process(Object o, Context context, Iterable iterable, Collector collector) throws Exception {

    }
}
