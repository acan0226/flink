package net.acan.net.acan.flink.chapter11;

import net.acan.net.acan.flink.bean.WaterSensor;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.time.Duration;

import static org.apache.flink.table.api.Expressions.$;

public class Flink09_Time_Processing {
    public static void main(String[] args) {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        environment.setParallelism(1);

        SingleOutputStreamOperator<WaterSensor> stream = environment.fromElements(
                        new WaterSensor("sensor_1", 1000L, 10),
                        new WaterSensor("sensor_1", 2000L, 20),
                        new WaterSensor("sensor_2", 3000L, 30),
                        new WaterSensor("sensor_1", 4000L, 40),
                        new WaterSensor("sensor_1", 5000L, 50),
                        new WaterSensor("sensor_2", 6000L, 60)
                )
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy
                                .<WaterSensor>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                                .withTimestampAssigner((element, recordTimestamp) -> element.getTs()));

        //创建表环境
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(environment);

        //Table table = tableEnv.fromDataStream(stream, $("id"), $("ts"), $("pt").proctime(), $("vc"));
        Table table = tableEnv.fromDataStream(stream, $("id"), $("ts"), $("pt").rowtime(), $("vc"));

        table.execute().print();
    }
}
