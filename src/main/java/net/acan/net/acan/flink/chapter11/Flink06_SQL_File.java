package net.acan.net.acan.flink.chapter11;

import net.acan.net.acan.flink.bean.WaterSensor;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class Flink06_SQL_File {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // 纯sql的方式操作表
        // 提供了两个执行sql的方法:
        //tEnv.executeSql(""); // 执行ddl语句 和 增删改

        tableEnv
                .executeSql("create table sensor(" +
                        "id string," +
                        "ts bigint," +
                        "vc int" +
                        ")with(" +
                        "'connector' = 'filesystem', " +
                        "'path' = 'input/sensor.txt', " +
                        "'format' = 'csv'" +
                        ")");

        tableEnv
                .executeSql("create table abc(" +
                        "id string," +
                        "ts bigint," +
                        "vc int" +
                        ")with(" +
                        "'connector' = 'filesystem', " +
                        "'path' = 'input/a.txt', " +
                        "'format' = 'csv'" +
                        ")");

        tableEnv.sqlQuery("select * from sensor where id = 'sensor_1'").executeInsert("abc");
    }
}
