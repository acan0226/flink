package net.acan.net.acan.flink.chapter02;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class wc_bound {
    public static void main(String[] args) {
        //1/创建一个流的执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        //2/通过env得到一个数据流
        DataStreamSource<String> dataStreamSource = env.socketTextStream("hadoop162", 9999);

        //3/各种转换

    }
}
