package net.acan.net.acan.flink.chapter11;

import net.acan.net.acan.flink.bean.WaterSensor;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.time.Duration;

import static org.apache.flink.table.api.Expressions.$;

public class Flink12_Window_TVF_3 {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 1. 先创建一个流
        DataStream<WaterSensor> stream = env
                .fromElements(
                        new WaterSensor("sensor_1", 1000L, 10),
                        new WaterSensor("sensor_1", 2000L, 20),
                        new WaterSensor("sensor_2", 3000L, 30),
                        new WaterSensor("sensor_1", 4001L, 40),
                        new WaterSensor("sensor_1", 5000L, 50),
                        new WaterSensor("sensor_2", 6000L, 60)
                )
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy
                                .<WaterSensor>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                                .withTimestampAssigner((a,b)->a.getTs())
                );

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        Table table = tableEnv.fromDataStream(stream,$("id"),$("ts").rowtime(),$("vc"));
        tableEnv.createTemporaryView("sensor",table);

//        select id,start,end,from t group id ,window
//        滚动窗口
//        descriptor(timecol) 指定时间列

        tableEnv.sqlQuery(
                "select " +
                        "id, " +
                        "window_start, " +
                        "window_end, " +
                        "sum(vc) vc_sum " +
                        "from table( tumble(table sensor, descriptor(ts),interval '5' second )) " +
                        "group by window_start, window_end, grouping sets( (id) , () )")
                .execute()
                .print();




    }
}
