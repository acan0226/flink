package net.acan.net.acan.flink.chapter08;

import net.acan.net.acan.flink.bean.AdsClickLog;
import net.acan.net.acan.flink.bean.LoginEvent;
import net.acan.net.acan.flink.util.MyUtil;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.state.ReducingState;
import org.apache.flink.api.common.state.ReducingStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.util.List;

public class Project05_Login {
    public static void main(String[] args) {
        Configuration conf = new Configuration();
        conf.setInteger("rest.port", 20000);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        env.setParallelism(1);

env.readTextFile("input/LoginLog.csv")
        .map(line->{
            String[] data = line.split(",");
            return new LoginEvent(
                    Long.valueOf(data[0]),
                    data[1],
                    data[2],
                    Long.valueOf(data[3]) * 1000
            );
        })
        .keyBy(a->a.getUserId())
        .countWindow(2,1)
        .process(new ProcessWindowFunction<LoginEvent, String, Long, GlobalWindow>() {
            @Override
            public void process(Long userId,
                                Context context,
                                Iterable<LoginEvent> elements,
                                Collector<String> out) throws Exception {

                List<LoginEvent> list = MyUtil.toList(elements);
                if (list.size() == 2) {


                    LoginEvent event1 = list.get(0);
                    LoginEvent event2 = list.get(1);

                    String eventType1 = event1.getEventType();
                    String eventType2 = event2.getEventType();

                    Long eventTime1 = event1.getEventTime();
                    Long eventTime2 = event2.getEventTime();

                    if ("fail".equals(eventType1) && "fail".equals(eventType2) && Math.abs(eventTime2 - eventTime1) < 2000) {
                        out.collect("用户" + userId + "在恶意登陆");
                    }
                }
            }
        }
        )
        .print();



    try {
            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
