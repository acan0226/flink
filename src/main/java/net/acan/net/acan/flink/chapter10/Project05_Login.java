package net.acan.net.acan.flink.chapter10;

import net.acan.net.acan.flink.bean.LoginEvent;
import net.acan.net.acan.flink.util.MyUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.util.List;
import java.util.Map;


public class Project05_Login {
    public static void main(String[] args) {
        Configuration conf = new Configuration();
        conf.setInteger("rest.port", 20000);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        env.setParallelism(1);

        KeyedStream<LoginEvent, Long> stream = env.readTextFile("input/LoginLog.csv")
                .map(line -> {
                    String[] data = line.split(",");
                    return new LoginEvent(
                            Long.valueOf(data[0]),
                            data[1],
                            data[2],
                            Long.valueOf(data[3]) * 1000
                    );
                })
                //创建水印
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy
                                .<LoginEvent>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                                //指定事件时间
                                .withTimestampAssigner((SerializableTimestampAssigner<LoginEvent>) (element, recordTimestamp) -> element.getEventTime())
                )
                .keyBy(a -> a.getUserId());
        //定义模式：2s内，连续两次登陆
        Pattern<LoginEvent, LoginEvent> pattern = Pattern
                .<LoginEvent>begin("fail")
                .where(new SimpleCondition<LoginEvent>() {
                    @Override
                    public boolean filter(LoginEvent value) throws Exception {
                        return "fail".equals(value.getEventType());
                    }
                })
                .times(2).consecutive()
                .within(Time.seconds(2));
        //把规则作用在流上，得到模式流
        PatternStream<LoginEvent> ps = CEP.pattern(stream, pattern);
        //从模式流中取出匹配数据
        ps.select(new PatternSelectFunction<LoginEvent, String>() {
            @Override
            public String select(Map<String, List<LoginEvent>> map) throws Exception {
                Long userId = map.get("fail").get(0).getUserId();
                return userId+"在恶意登陆";
            }
        })
                .print();


        try {
            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
