package net.acan.net.acan.flink.chapter05.transform;

import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class transform_keyby {
    public static void main(String[] args) {
        Configuration conf = new Configuration();
        conf.setInteger("rest.port", 20000);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        env.setParallelism(2);

        DataStreamSource<Integer> stream = env.fromElements(1, 2, 3, 4, 5, 6);

        stream.keyBy(new KeySelector<Integer, String>() {
            @Override
            public String getKey(Integer value) throws Exception {
                return value % 2 == 0 ? "jishu":"oushu";
            }
        }).print();

        try {
            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
