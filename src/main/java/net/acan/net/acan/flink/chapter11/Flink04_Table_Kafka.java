package net.acan.net.acan.flink.chapter11;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.Csv;
import org.apache.flink.table.descriptors.Kafka;
import org.apache.flink.table.descriptors.Schema;

public class Flink04_Table_Kafka {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        //2.需要相关的表环境
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        Schema schema = new Schema()
                .field("id", DataTypes.STRING())
                .field("ts", DataTypes.BIGINT())
                .field("vc", DataTypes.INT());

        tEnv.connect(new Kafka()
                .version("universal")
                .property("bootstrap.servers", "hadoop162:9092")
                .property("group.id", "atguigu")
                .topic("s1")
                .startFromLatest()
        )
                .withFormat(new Csv())
                .withSchema(schema)
                .createTemporaryTable("sensor");



    }

}
