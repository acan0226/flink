package net.acan.net.acan.flink.chapter11;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class Flink08_SQL_Print {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // 纯sql的方式操作表
        // 提供了两个执行sql的方法:
        //tEnv.executeSql(""); // 执行ddl语句 和 增删改

        tableEnv
                .executeSql("create table sensor(" +
                        "id string, " +
                        "ts bigint, " +
                        "vc int" +
                        ")with(" +
                        "'connector' = 'kafka', " +
                        "'topic' = 's1'," +
                        "  'properties.bootstrap.servers' = 'hadoop162:9092',\n" +
                        "  'properties.group.id' = 'atguigu',\n" +
                        "  'scan.startup.mode' = 'latest-offset',\n" +
                        "  'format' = 'json'" +
                        ")");

        tableEnv
                .executeSql("create table abc(" +
                        "id string, " +
                        "ts bigint, " +
                        "vc int" +
                        ")with(" +
                        "'connector' = 'print' " +
                        ")");

        tableEnv
                .executeSql("create table def(" +
                        "id string," +
                        "vc int" +
                        ")with(" +
                        "'connector' = 'print' " +
                        ")");

        tableEnv.sqlQuery("select * from sensor").executeInsert("abc");
        tableEnv.sqlQuery("select id, sum(vc) vc from sensor group by id").executeInsert("def");
    }
}
