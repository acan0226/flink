package net.acan.net.acan.flink.chapter07.state;

import net.acan.net.acan.flink.bean.WaterSensor;
import net.acan.net.acan.flink.util.MyUtil;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.contrib.streaming.state.EmbeddedRocksDBStateBackend;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.producer.ProducerRecord;

import javax.annotation.Nullable;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Properties;

public class State10_Kafka_Flink_Kafka {
    public static void main(String[] args) {

        System.setProperty("HADOOP_USER_NAME", "atguigu");

        Configuration conf = new Configuration();
        conf.setInteger("rest.port", 20000);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        env.setParallelism(1);

        env.enableCheckpointing(2000);//开启checkpoint
        //设置状态后端
        env.setStateBackend(new HashMapStateBackend());
        env.getCheckpointConfig().setCheckpointStorage("hdfs://hadoop162:8020/ck10");
        //设置Checkpoint的一致性
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);

        // 设置同时并发的checkpoint的数量
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
        // 两个checkpoint之间的最小时间间隔
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(500);
        //checkpoint 超时时间
        env.getCheckpointConfig().setCheckpointTimeout(10*1000);
        // 程序被取消之后, checkpoint数据仍然保留
        env.getCheckpointConfig().setExternalizedCheckpointCleanup(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);


        Properties sourceProps = new Properties();
        sourceProps.put("bootstrap.servers","hadoop162:9092,hadoop163:9092,hadoop164:9092");
        sourceProps.put("group.id","State10_Kafka_Flink_Kafka");
        sourceProps.put("auto.reset.offset","latest");
        sourceProps.put("isolation.level", "read_committed");//读取提交的数据


        Properties  sinkProps = new Properties();
        sinkProps.put("bootstrap.servers", "hadoop162:9092,hadoop163:9092,hadoop164:9092");
        sinkProps.put("transaction.timeout.ms", 15 * 60 * 1000);
        env
                .addSource(new FlinkKafkaConsumer<String>("s1", new SimpleStringSchema(),sourceProps))
                .flatMap(new FlatMapFunction<String, Tuple2<String,Long>>() {
                    @Override
                    public void flatMap(String value, Collector<Tuple2<String, Long>> out) throws Exception {
                        for (String word : value.split(" ")) {
                            out.collect(Tuple2.of(word,1L));
                        }
                    }
                })
                .keyBy(t -> t.f0)
                .sum(1)
                .addSink(new FlinkKafkaProducer<Tuple2<String, Long>>("acan",
                        new KafkaSerializationSchema<Tuple2<String, Long>>() {
                            @Override
                            public ProducerRecord<byte[], byte[]> serialize(
                                    Tuple2<String, Long> element,
                                    @Nullable Long timestamp) {
                                String msg = element.f0 + "_" + element.f1;
                                return new ProducerRecord<>("s2",msg.getBytes(StandardCharsets.UTF_8));
                            }
                        },
                        sinkProps
                , FlinkKafkaProducer.Semantic.EXACTLY_ONCE));

        try {
            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
