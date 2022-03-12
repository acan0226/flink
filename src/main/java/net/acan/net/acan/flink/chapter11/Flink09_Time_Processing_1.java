package net.acan.net.acan.flink.chapter11;

import net.acan.net.acan.flink.bean.WaterSensor;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import static org.apache.flink.table.api.Expressions.$;

public class Flink09_Time_Processing_1 {
    public static void main(String[] args) {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        environment.setParallelism(1);

        //创建表环境
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(environment);

       tableEnv.executeSql(
               "create table sensor(" +
                       "id string," +
                       "ts bigint," +
                       "vc int," +
                       "pt as proctime() " +
                       ")with(" +
                       "   'connector' = 'filesystem', " +
                       "   'path' = 'input/sensor.txt', " +
                       "   'format' = 'csv' " +
                       ")");

       tableEnv.sqlQuery("select * from sensor").execute().print();


    }
}
