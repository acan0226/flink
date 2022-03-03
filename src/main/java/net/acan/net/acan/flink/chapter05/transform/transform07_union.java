package net.acan.net.acan.flink.chapter05.transform;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.ConnectedStreams;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoMapFunction;

public class transform07_union {
    public static void main(String[] args) {
        Configuration conf = new Configuration();
        conf.setInteger("rest.port", 20000);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        env.setParallelism(2);

        DataStreamSource<Integer> s1 = env.fromElements(10,20,30);
        DataStreamSource<Integer> s2 = env.fromElements(1, 2, 3);

        DataStream<Integer> union = s1.union(s2);
        union.map(new MapFunction<Integer, String>() {
            @Override
            public String map(Integer integer) throws Exception {
                return integer + "<";
            }
        }).print();


        try {
            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
/*
connect:
1. 只能连接两个流
2. 这两个流的类型可以不一样

Union:
1. 可以多个流union在一起
2. 类型必须一样
 */