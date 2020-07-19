import java.util
import java.util.Properties
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011
import org.apache.flink.streaming.connectors.kafka.internals.KafkaTopicPartition
import org.apache.flink.api.scala._

/**
  * 使用kafka作为flink的数据源进行读取数据
  */
object KafkaSource {
  def main(args: Array[String]): Unit = {

    // 1. env
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    //2：构建kafka的请求参数
    val topic = "flink1"
    //创建请求的参数集合
    val props = new Properties()
    props.setProperty("bootstrap.servers","node01:9092")

    //3. 创建消费者实例
    val consumer: FlinkKafkaConsumer011[String] = new FlinkKafkaConsumer011[String](topic,new SimpleStringSchema(),props)

    //4.构建kafka数据源
    val kafkaDataStream: DataStream[String] = env.addSource(consumer)

    //5.打印数据
    kafkaDataStream.print()

    //6. 启动任务
    env.execute()
  }
}