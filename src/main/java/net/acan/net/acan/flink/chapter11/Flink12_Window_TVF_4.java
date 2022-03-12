package net.acan.net.acan.flink.chapter11;

import net.acan.net.acan.flink.bean.Person;
import net.acan.net.acan.flink.bean.WaterSensor;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.time.Duration;

import static org.apache.flink.table.api.Expressions.$;

public class Flink12_Window_TVF_4 {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 1. 先创建一个流
        DataStream<Person> stream = env
                .fromElements(
                        new Person("a1", "b1", "c1", "d1", 1000L, 10),
                        new Person("a2", "b2", "c2", "d2", 2000L, 20),
                        new Person("a3", "b3", "c3", "d3", 3000L, 30),
                        new Person("a4", "b4", "c4", "d4", 4000L, 40),
                        new Person("a5", "b5", "c5", "d5", 5000L, 50),
                        new Person("a6", "b6", "c6", "d6", 6000L, 60),
                        new Person("a7", "b7", "c7", "d7", 7000L, 70),
                        new Person("a8", "b8", "c8", "d8", 8000L, 80)
                )
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy
                                .<Person>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                                .withTimestampAssigner((a,b)->a.getTs())
                );

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        Table table = tableEnv.fromDataStream(stream,$("a"), $("b"), $("c"), $("d"), $("ts").rowtime(), $("score"));
        tableEnv.createTemporaryView("sensor",table);

//        select id,start,end,from t group id ,window
//        滚动窗口
//        descriptor(timecol) 指定时间列
//
//        tableEnv.sqlQuery(
//                "select " +
//                        "id, " +
//                        "window_start, " +
//                        "window_end, " +
//                        "sum(vc) vc_sum " +
//                        "from table( tumble(table sensor, descriptor(ts),interval '5' second )) " +
//                        "group by window_start, window_end, grouping sets( (id) , () )")
//                .execute()
//                .print();

        tableEnv.sqlQuery(
                        "select " +
                                "a,b,c,d,window_start,window_end, " +
                                "sum(score) vc_sum " +
                                "from table( tumble(table sensor, descriptor(ts),interval '5' second )) " +
                                "group by window_start, window_end, cube(a,b,c,d)")
                .execute()
                .print();



    }
}
