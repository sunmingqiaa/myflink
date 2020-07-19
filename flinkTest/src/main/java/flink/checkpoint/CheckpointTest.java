package flink.checkpoint;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer011;
import java.util.Properties;

public class CheckpointTest {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //开启检查点
        env.enableCheckpointing(5000);
        //持久化检查点
        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        //设置重启策略
//        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(
//                3, 10000));
        //设置数据源为kafka
        String bootstrap = "node01:9092";
        String zk = "node01:2181";
        String groupId = "group1";
        String topic = "flink1";
        String sinkTopic = "flink2";

        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", bootstrap);
        properties.setProperty("zookeeper.connect", zk);
        properties.setProperty("group.id", groupId);
        FlinkKafkaConsumer011<String> consumer = new FlinkKafkaConsumer011<>(topic, new SimpleStringSchema(), properties);
        consumer.setStartFromGroupOffsets();
        DataStreamSource<String> stream = env.addSource(consumer);
        //处理数据,当处理数据量是10的倍数,抛出异常,然后重启策略触发,job会尝试三次重启,如果继续异常,job失败
        SingleOutputStreamOperator<String> sum = stream.map(
                new RichMapFunction<String, Tuple2<String, Integer>>() {
//                    int num = 0;

                    @Override
                    public Tuple2<String, Integer> map(String s) {
//                        num++;
//                        if (num % 10 == 0) {
//                            System.out.println("出现错误,即将重启");
//                            throw new RuntimeException("出现错误，程序重启！");
//                        } else {
                            return new Tuple2(s, 1);
//                        }
                    }
                }).keyBy(0)
                .sum(1)
                .map(
                        new RichMapFunction<Tuple2<String, Integer>, String>() {
                            @Override
                            public String map(Tuple2<String, Integer> value) {
                                return (value.toString());
                            }
                        }
                );
        //sink到kafka
//        sum.addSink(new FlinkKafkaProducer011<String>(bootstrap, sinkTopic, new SimpleStringSchema()));
        sum.print();
        try {
            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}

