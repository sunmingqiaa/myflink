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
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //开启检查点
        env.enableCheckpointing(5000);
        //持久化检查点
        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        //设置重启策略
//        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(
//                3, 10000));
        //设置数据源为kafka
        String bootstrap = "192.168.100.100:9092";
        String zk = "192.168.100.100:2181";
        String groupId = "group1";
        String topic = "flink1";
        String sinkTopic = "flink2";

        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", bootstrap);
        properties.setProperty("zookeeper.connect", zk);
        properties.setProperty("group.id", groupId);
        FlinkKafkaConsumer011<String> consumer = new FlinkKafkaConsumer011<>(topic, new SimpleStringSchema(), properties);
        //consumer.setStartFromEarliest();
        DataStreamSource<String> stream = env.addSource(consumer);
        SingleOutputStreamOperator<String> data = stream.map(
                new RichMapFunction<String, String>() {
                    @Override
                    public String map(String s) {
                            return s;
                        }

                });
        //sink到kafka
//        sum.addSink(new FlinkKafkaProducer011<String>(bootstrap, sinkTopic, new SimpleStringSchema()));
        data.print();
        env.execute();


    }
}

