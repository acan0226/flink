package net.acan.net.acan.flink.chapter05.transform;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class transform_map_rich {
    public static void main(String[] args) {
        Configuration conf = new Configuration();
        conf.setInteger("rest.port", 20000);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        env.setParallelism(1);

        DataStreamSource<Integer> stream = env.fromElements(1, 2, 3, 4, 5, 6);
//        stream.map(new MapFunction<Integer, Integer>() {
//            @Override
//            public Integer map(Integer value) throws Exception {
//                return value * value;
//            }
//        }).print();

        stream.map(new RichMapFunction<Integer, Integer>() {
            @Override
            public void open(Configuration parameters) throws Exception {
                // 应用初始化成功之后会自动调用这个方法
                // 每个并行度执行一次
                // 这个方法内一般实现一些初始化的操作, 获取获取已经连接信息: jdbc连接
                System.out.println("Flink01_Map_Rich.open");
            }

            @Override
            public void close() throws Exception {
                // 应用关闭的时候执行, 执行次数和open一致
                // 用来释放资源
                System.out.println("Flink01_Map_Rich.close");
            }

            @Override
            public Integer map(Integer v) throws Exception {
                return v * v;
            }
        }).print();

        try {
            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
