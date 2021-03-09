package flinkWatermarkDemo;

import flinkWatermarkDemo.MyKeySelector;
import flinkWatermarkDemo.MyProcessFunction;
import flinkWatermarkDemo.MyTrigger;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.time.Duration;
import java.util.Properties;

public class WindowTest {
    public static void main(String[] args) throws Exception {
        // 构建流处理环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //获取kafka数据流
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "localhost:9092");
        properties.setProperty("group.id", "test");
//        FlinkKafkaConsumer<String> topic = new FlinkKafkaConsumer<>("topic", new SimpleStringSchema(), properties);
        DataStream<String> stream = env
                .addSource(new FlinkKafkaConsumer<>("topic", new SimpleStringSchema(), properties));
//                .assignTimestampsAndWatermarks(WatermarkStrategy.forBoundedOutOfOrderness(Duration.ofSeconds(2000))));//在数据源添加水印策略
//    通过事件时间处理乱序数据,需要下面方法设置水印策略，其中包括水印生成器和时间戳分配器，时间戳分配器可以自己实现，也可以不实现，kafka数据源数据会自带时间戳
        stream.assignTimestampsAndWatermarks(WatermarkStrategy.forBoundedOutOfOrderness(Duration.ofSeconds(2)));//在数据流设置水印策略
        SingleOutputStreamOperator process = stream.keyBy(new MyKeySelector())
                //KeyedStream->windowedStream 用window（），DataStream->AllWindowedStream 用windowAll（）
//                如果按照处理时间需要用TumblingProcessingTimeWindows()方法
                .window(TumblingEventTimeWindows.of(Time.seconds(5)))
//               设置了允许迟到时间，该窗口会在水印时间>=窗口结束时间+允许迟到时间时再次触发窗口计算
                .allowedLateness(Time.seconds(3))
      /*          如果用户不自定义Trigger的实现类，Flink将会调用默认的trigger，例如对于时间属性为 EventTime 的窗口，Flink 默认会使用EventTimeTrigger类；
                时间属性为 ProcessingTime 的窗口，Flink 默认使用 ProcessingTimeTrigger类，
                如果用户指定了要使用的 trigger，默认的 trigger 将会被覆盖，不会起作用*/
                .trigger(new MyTrigger())
                .process(new MyProcessFunction());
        /*stream.windowAll(TumblingEventTimeWindows.of(Time.seconds(5)))
                .allowedLateness(Time.seconds(3))
                .trigger(new MyTrigger())
                .process(new ProcessAllWindowFunction() {
                    @Override
                    public void process(Context context, Iterable iterable, Collector collector) throws Exception {

                    }
                });*/
        process.print();
        env.execute();

    }
}
