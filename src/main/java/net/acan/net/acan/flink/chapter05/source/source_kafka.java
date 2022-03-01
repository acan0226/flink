package net.acan.net.acan.flink.chapter05.source;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.util.serialization.JSONKeyValueDeserializationSchema;

import java.util.Properties;

public class source_kafka {
    public static void main(String[] args) {
        Configuration conf = new Configuration();
        conf.setInteger("rest.port", 20000);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        env.setParallelism(1);

        Properties props = new Properties();
        props.put("bootstrap.servers","hadoop162:9092,hadoop163:9092,hadoop164:9092");
        props.put("group.id","source_kafka2");
        props.put("auto.reset.offset","latest");

        //DataStreamSource<String> stream = env.addSource(new FlinkKafkaConsumer<>("a1", new SimpleStringSchema(), props));
        //stream.print();

        DataStreamSource<ObjectNode> stream = env.addSource(new FlinkKafkaConsumer<>("a1", new JSONKeyValueDeserializationSchema(true), props));
        stream
        .map(node -> node.get("value").get("ts"))
                .print();

        try {
            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
