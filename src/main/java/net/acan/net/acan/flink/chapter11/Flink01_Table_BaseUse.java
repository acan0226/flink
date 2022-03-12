package net.acan.net.acan.flink.chapter11;

import net.acan.net.acan.flink.bean.WaterSensor;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import static org.apache.flink.table.api.Expressions.$;
import static org.apache.flink.table.api.Expressions.CURRENT_ROW;

public class Flink01_Table_BaseUse {
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
                .where($("id").isEqual("sensor_1"))
                .select($("id"), $("ts"), $("vc"));
        result.printSchema();
                // 打印表原数据
        DataStream<Row> resultStream = tEnv.toAppendStream(result, Row.class);
        resultStream.print();
        env.execute();
    }
}
