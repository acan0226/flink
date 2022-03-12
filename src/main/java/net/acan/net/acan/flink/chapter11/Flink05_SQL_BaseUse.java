package net.acan.net.acan.flink.chapter11;

import net.acan.net.acan.flink.bean.WaterSensor;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class Flink05_SQL_BaseUse {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<WaterSensor> stream = env.fromElements(
                new WaterSensor("sensor_1", 1000L, 10),
                new WaterSensor("sensor_1", 2000L, 20),
                new WaterSensor("sensor_2", 3000L, 30),
                new WaterSensor("sensor_1", 4000L, 40),
                new WaterSensor("sensor_1", 5000L, 50),
                new WaterSensor("sensor_2", 6000L, 60)
        );

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        Table table = tableEnv.fromDataStream(stream);

        // 纯sql的方式操作表
        // 提供了两个执行sql的方法:
        //tEnv.executeSql(""); // 执行ddl语句 和 增删改
        // tEnv.sqlQuery(""); // 只执行查询语句

        // 1.查询未注册的表

       // tableEnv.sqlQuery("select * from "+ table +" where id = 'sensor_1'").execute().print();

        //2.查询已注册的表
        tableEnv.createTemporaryView("sensor", table);
        //tableEnv.sqlQuery("select * from sensor where id='sensor_1'").execute().print();

        tableEnv
                .sqlQuery("select " +
                        "id," +
                        "sum(vc) vc_sum " +
                        "from sensor " +
                        "group by id"
                ).execute()
                .print();
    }
}
