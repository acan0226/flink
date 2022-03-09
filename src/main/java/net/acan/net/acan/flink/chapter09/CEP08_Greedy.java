package net.acan.net.acan.flink.chapter09;

import net.acan.net.acan.flink.bean.WaterSensor;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.time.Duration;
import java.util.List;
import java.util.Map;

public class CEP08_Greedy {
    public static void main(String[] args) {
        Configuration conf = new Configuration();
        conf.setInteger("rest.port", 20000);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        env.setParallelism(1);
        // 1. 先有数据流
        // 返回事件时间
        SingleOutputStreamOperator<WaterSensor> stream = env
                .readTextFile("input/sensor.txt")
                .map(line -> {
                    String[] data = line.split(",");
                    return new WaterSensor(data[0], Long.valueOf(data[1]), Integer.valueOf(data[2]));
                })
                .assignTimestampsAndWatermarks(
                WatermarkStrategy.
                        <WaterSensor>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                        .withTimestampAssigner((SerializableTimestampAssigner<WaterSensor>) (element, recordTimestamp) -> element.getTs())
        );
        //2.指定规则（定义模式）
        Pattern<WaterSensor, WaterSensor> pattern = Pattern.
                <WaterSensor>begin("s1")
                .where(new SimpleCondition<WaterSensor>() {
                    @Override
                    public boolean filter(WaterSensor value) throws Exception {
                        return "sensor_1".equals(value.getId());
                    }
                })
                .times(2, 3)//.greedy()  // 贪婪性: 如果一条数据既满足第一个又满足, 优先满足第一个
                .next("s2") //严格相连 经常使用

                .where(new SimpleCondition<WaterSensor>() {
                    @Override
                    public boolean filter(WaterSensor value) throws Exception {
                        return value.getVc() == 30;
                    }
                });

        //3.把规则作用在流上，得到模式流
        PatternStream<WaterSensor> ps = CEP.pattern(stream, pattern);

        //4.从模式流中取出匹配的数据
        ps.select(new PatternSelectFunction<WaterSensor, String>() {
            @Override
            public String select(Map<String, List<WaterSensor>> map) throws Exception {
                return map.toString();
            }
        }).print();


        try {
            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}
