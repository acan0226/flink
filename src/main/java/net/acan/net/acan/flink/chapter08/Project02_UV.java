package net.acan.net.acan.flink.chapter08;

import net.acan.net.acan.flink.bean.UserBehavior;
import net.acan.net.acan.flink.util.MyUtil;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.text.SimpleDateFormat;
import java.time.Duration;
import java.util.Date;
import java.util.List;

public class Project02_UV {
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
                .process(new ProcessWindowFunction<UserBehavior, String, String, TimeWindow>() {

                    private MapState<Long, Object> userIdState;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        userIdState = getRuntimeContext().getMapState(new MapStateDescriptor<Long, Object>(
                                "userIdState",
                                Long.class,
                                Object.class
                        ));
                    }


                    @Override
                    public void process(String s,
                                        Context context,
                                        Iterable<UserBehavior> elements,
                                        Collector<String> out) throws Exception {

                        // 状态只和key有关, 不同的窗口共用相同的状态. 每个窗口用之前要清空
                        userIdState.clear();

                        for (UserBehavior element : elements) {
                            userIdState.put(element.getUserId(), new Object());
                        }
                        List<Long> list = MyUtil.toList(userIdState.keys());
                        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                        String start = dateFormat.format(context.window().getStart());
                        String end = dateFormat.format(context.window().getEnd());
                        out.collect(start+"_"+end+" "+list.size());
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
