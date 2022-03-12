package net.acan.net.acan.flink.chapter11;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class Flink10_Time_Event {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        tableEnv.executeSql(
                "create table sensor(" +
                        "   id string," +
                        "   ts bigint, " +
                        "   vc int," +
                        "et as to_timestamp_ltz(ts,3)," +
                        "watermark for et as et - interval '3' second "+
                        ")with(" +
                        "   'connector' = 'filesystem', " +
                        "   'path' = 'input/sensor.txt', " +
                        "   'format' = 'csv' " +
                        ")");

        Table table = tableEnv.sqlQuery("select * from sensor");
        table.printSchema();
        table.execute().print();

    }
}
