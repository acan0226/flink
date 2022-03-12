package net.acan.net.acan.flink.chapter11;

import net.acan.net.acan.flink.bean.WaterSensor;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Over;
import org.apache.flink.table.api.OverWindow;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.time.Duration;

import static org.apache.flink.table.api.Expressions.$;
import static org.apache.flink.table.api.Expressions.lit;

public class Flink13_Window_Over_2 {
    public static void main(String[] args) {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        environment.setParallelism(1);

        // 1. 先创建一个流
        DataStream<WaterSensor> stream = environment
                .fromElements(
                        new WaterSensor("sensor_1", 1000L, 10),
                        new WaterSensor("sensor_1", 2000L, 20),
                        new WaterSensor("sensor_1", 2000L, 40),
                        new WaterSensor("sensor_1", 5000L, 50),
                        new WaterSensor("sensor_2", 6000L, 30),
                        new WaterSensor("sensor_2", 7000L, 60)
                )
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy
                                .<WaterSensor>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                                .withTimestampAssigner((ws, ts) -> ws.getTs())

                );

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(environment);

        Table table = tableEnv.fromDataStream(stream, $("id"), $("ts").rowtime(), $("vc"));
        tableEnv.createTemporaryView("sensor", table);

        // rank(xx) over(partition by id order by count rows between unbounded preceding and current row)

        tableEnv.sqlQuery("select " +
                "id,ts,vc," +
//                "sum(vc) over(partition by id order by ts rows between unbounded preceding and current row) vc_sum " +
//                "sum(vc) over(partition by id order by ts rows between 1 preceding and current row) vc_sum " +
//                "sum(vc) over(partition by id order by ts range between unbounded preceding and current row) vc_sum " +
                "sum(vc) over(partition by id order by ts range between interval '2' second preceding and current row) vc_sum " +
                "from sensor")
                .execute()
                .print();

    }
}
