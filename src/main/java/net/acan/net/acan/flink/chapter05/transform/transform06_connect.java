package net.acan.net.acan.flink.chapter05.transform;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.ConnectedStreams;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoMapFunction;

public class transform06_connect {
    public static void main(String[] args) {
        Configuration conf = new Configuration();
        conf.setInteger("rest.port", 20000);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        env.setParallelism(2);

        DataStreamSource<String> s1 = env.fromElements("a", "b", "c");
        DataStreamSource<Integer> s2 = env.fromElements(1, 2, 3);

        ConnectedStreams<String, Integer> connect = s1.connect(s2);
        connect.map(new CoMapFunction<String, Integer, String>() {

            @Override
            public String map1(String s) throws Exception {
                return s + ">";
            }

            @Override
            public String map2(Integer integer) throws Exception {
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
