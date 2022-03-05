package net.acan.net.acan.flink.chapter05.sink;

import net.acan.net.acan.flink.bean.WaterSensor;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.connector.jdbc.JdbcStatementBuilder;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.util.Collector;

import java.sql.PreparedStatement;
import java.sql.SQLException;

public class sink02_Jdbc {
    public static void main(String[] args) {
        Configuration conf = new Configuration();
        conf.setInteger("rest.port", 20000);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        env.setParallelism(1);

        SingleOutputStreamOperator<WaterSensor> stream = env
                .socketTextStream("hadoop162", 9999)
                .map(line -> {
                    String[] data = line.split(",");
                    return new WaterSensor(data[0], Long.valueOf(data[1]), Integer.valueOf(data[2]));
                });
        stream
                .keyBy( WaterSensor::getId)
                .sum("vc")
                .addSink(JdbcSink.sink(
                        "replace into sensor(id, ts, vc)values(?,?,?)",
                        new JdbcStatementBuilder<WaterSensor>() {
                            @Override
                            public void accept(PreparedStatement ps,
                                               WaterSensor waterSensor) throws SQLException {
                                ps.setString(1,waterSensor.getId());
                                ps.setLong(2,waterSensor.getTs());
                                ps.setInt(3,waterSensor.getVc());
                            }
                        },
                        new JdbcExecutionOptions.Builder()
                                .withBatchIntervalMs(2000)
                                .withBatchSize(16*1024)
                                .withMaxRetries(3)
                                .build(),
                        new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                                .withDriverName("com.mysql.jdbc.Driver")
                                .withUrl("jdbc:mysql://hadoop162:3306/test?characterEncoding=utf8&useSSL=false")
                                .withUsername("root")
                                .withPassword("aaaaaa")
                                .build()
                ));

        try {
            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
