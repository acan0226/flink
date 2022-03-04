package net.acan.net.acan.flink.chapter05.sink;

import net.acan.net.acan.flink.bean.WaterSensor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.apache.kafka.clients.producer.ProducerRecord;

import javax.annotation.Nullable;
import java.nio.charset.StandardCharsets;
import java.util.Properties;

public class sink01_kafka {
    public static void main(String[] args) {
        Configuration conf = new Configuration();
        conf.setInteger("rest.port", 20000);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        env.setParallelism(1);

        DataStreamSource<WaterSensor> stream = env.fromElements(
                new WaterSensor("sensor_1", 1L, 10),
                new WaterSensor("sensor_1", 3L, 30),
                new WaterSensor("sensor_1", 4L, 30),
                new WaterSensor("sensor_1", 2L, 20),
                new WaterSensor("sensor_1", 5L, 40),
                new WaterSensor("sensor_2", 4L, 100),
                new WaterSensor("sensor_2", 5L, 200)
        );
        Properties props = new Properties();
        stream.addSink(new FlinkKafkaProducer<WaterSensor>(
                "aa",
                new KafkaSerializationSchema<WaterSensor>() {
                    @Override
                    public ProducerRecord<byte[], byte[]> serialize(WaterSensor waterSensor,
                                                                    @Nullable Long aLong) {
                        return new ProducerRecord<>(waterSensor.getId(), waterSensor.toString().getBytes(StandardCharsets.UTF_8));
                    }
                },
                props,
                FlinkKafkaProducer.Semantic.AT_LEAST_ONCE

        ));

        try {
            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
