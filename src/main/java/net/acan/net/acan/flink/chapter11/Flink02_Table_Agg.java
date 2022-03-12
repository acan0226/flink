package net.acan.net.acan.flink.chapter11;

import net.acan.net.acan.flink.bean.WaterSensor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import static org.apache.flink.table.api.Expressions.$;

public class Flink02_Table_Agg {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //1.先创建一个流
        DataStreamSource<WaterSensor> stream = env.fromElements(
                new WaterSensor("sensor_1", 1000L, 10),
                new WaterSensor("sensor_1", 2000L, 20),
                new WaterSensor("sensor_2", 3000L, 30),
                new WaterSensor("sensor_1", 4000L, 40),
                new WaterSensor("sensor_1", 5000L, 50),
                new WaterSensor("sensor_2", 6000L, 60)
        );

        //2.需要相关的表环境
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        //3.把表转成一个表(动态表)
        Table table = tEnv.fromDataStream(stream);

        //4.把结果表转成流，输出
        Table result = table
                .groupBy($("id"))
                .aggregate($("vc").sum().as("vc_sum"))
                .select($("id"),  $("vc_sum"));
                // 打印表原数据
                result.printSchema();
                //当有删除或更新时，使用撤回流
//        DataStream<Tuple2<Boolean, Row>> resultStream = tEnv.toRetractStream(result, Row.class);
//        resultStream
//                .filter(t->t.f0)
//                .map(t->t.f1)
//                .print();
//        env.execute();
        result.execute().print(); // idea端调试sql代码使用


    }
}
