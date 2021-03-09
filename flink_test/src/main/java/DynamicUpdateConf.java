import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.BroadcastConnectedStream;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.flink.util.Collector;

import java.util.concurrent.TimeUnit;



public class DynamicUpdateConf {

    public static void main(String[] args) throws Exception {

        // 构建流处理环境
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        environment.setParallelism(1);

        final MapStateDescriptor<String, String> CONFIG_KEYWORDS = new MapStateDescriptor<>(
                "config-keywords",
                BasicTypeInfo.STRING_TYPE_INFO,
                BasicTypeInfo.STRING_TYPE_INFO);

        // 模拟处理命令数据源，自定义广播流
        BroadcastStream<String> broadcastStream = environment.addSource(new RichSourceFunction<String>() {

            private volatile boolean isRunning = true;
            //测试数据集
            private String[] dataSet = new String[] {
                    "notHandle",
            };

            /**
             * 数据源：模拟每30秒随机更新一次拦截的关键字
             * @param ctx
             * @throws Exception
             */
            @Override
            public void run(SourceContext<String> ctx) throws Exception {
                int size = dataSet.length;
                while (isRunning) {
                    TimeUnit.SECONDS.sleep(30);
                    int seed = (int) (Math.random() * size);
                    //随机选择关键字发送
                    ctx.collect(dataSet[seed]);
                    System.out.println("读取到上游发送的处理命令:" + dataSet[seed]);
                }
            }

            @Override
            public void cancel() {
                isRunning = false;
            }
        }).broadcast(CONFIG_KEYWORDS);

        // 自定义数据流
        DataStream<String> dataStream = environment.addSource(new RichSourceFunction<String>() {

            private volatile boolean isRunning = true;

            //测试数据集
            private final String[] dataSet = new String[] {
                    "处理业务逻辑",
            };

            /**
             * 模拟每3秒随机产生1条消息
             * @param ctx
             * @throws Exception
             */
            @Override
            public void run(SourceContext<String> ctx) throws Exception {
                int size = dataSet.length;
                while (isRunning) {
                    TimeUnit.SECONDS.sleep(3);
                    int seed = (int) (Math.random() * size);
                    ctx.collect(dataSet[seed]);
                }
            }

            @Override
            public void cancel() {
                isRunning = false;
            }

        });

        BroadcastConnectedStream<String, String> connect = dataStream.connect(broadcastStream);
        // 数据流和广播流连接处理并将拦截结果打印
        dataStream.connect(broadcastStream).process(new BroadcastProcessFunction<String, String, String>() {

            //拦截的关键字
            private String keywords ;

            @Override
            public void open(Configuration parameters) throws Exception {
                super.open(parameters);
                keywords = "handle";
                System.out.println("初始化模拟连接数据库读取处理命令：handle");
            }

            @Override
            public void processElement(String value, ReadOnlyContext ctx, Collector<String> out) throws Exception {
                if (keywords.equals("handle")) {
                    out.collect(value);
                }else {
                    System.out.println("收到unhandle命令，跳过处理逻辑");
                }
            }

            @Override
            public void processBroadcastElement(String value, Context ctx, Collector<String> out) throws Exception {
                keywords = value;
                System.out.println("处理命令更新成功：" + value);
            }
        }).print();

        environment.execute();
    }

}
