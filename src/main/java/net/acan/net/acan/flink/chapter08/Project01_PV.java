package net.acan.net.acan.flink.chapter08;

import net.acan.net.acan.flink.bean.UserBehavior;
import net.acan.net.acan.flink.bean.WaterSensor;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.util.Date;

public class Project01_PV {
    public static void main(String[] args) {
        Configuration conf = new Configuration();
        conf.setInteger("rest.port", 20000);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        env.setParallelism(1);

        env
                .readTextFile("input/UserBehavior.csv")
                        .map(new MapFunction<String, UserBehavior>() {
                            @Override
                            public UserBehavior map(String value) throws Exception {
                                String[] data = value.split(",");
                                return new UserBehavior(
                                        Long.valueOf(data[0]),
                                        Long.valueOf(data[1]),
                                        Integer.valueOf(data[2]),
                                        data[3],
                                        Long.valueOf(data[4])*1000
                                );
                            }
                        })
                .filter(a -> "pv".equals(a.getBehavior()))
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy
                                .<UserBehavior>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                                .withTimestampAssigner(
                                        ( (element, recordTimestamp) -> element.getTimestamp()
                                )
                ))
                .keyBy(UserBehavior::getBehavior)
                .window(TumblingEventTimeWindows.of(Time.hours(1)))
                .aggregate(new AggregateFunction<UserBehavior, Long, Long>() {
                               @Override
                               public Long createAccumulator() {
                                   return 0L;
                               }

                               @Override
                               public Long add(UserBehavior value, Long accumulator) {
                                   return accumulator+1;
                               }

                               @Override
                               public Long getResult(Long accumulator) {
                                   return accumulator;
                               }

                               @Override
                               public Long merge(Long a, Long b) {
                                   return null;
                               }
                           },
                        new ProcessWindowFunction<Long, String, String, TimeWindow>() {
                            @Override
                            public void process(String s,
                                                Context context,
                                                Iterable<Long> elements,
                                                Collector<String> out) throws Exception {
                                Long result = elements.iterator().next();
                                Date start = new Date(context.window().getStart());
                                Date end = new Date(context.window().getEnd());
                                out.collect(start+" "+end+" "+result);
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
