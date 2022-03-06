package net.acan.net.acan.flink.chapter07.timer;

import net.acan.net.acan.flink.bean.WaterSensor;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.time.Duration;

public class Timer02_EventTimer {
    public static void main(String[] args) {
        Configuration conf = new Configuration();
        conf.setInteger("rest.port", 20000);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        env.setParallelism(1);

        SingleOutputStreamOperator<WaterSensor> stream = env
                .socketTextStream("hadoop162", 9999)
                .map( value -> {
                    String[] data = value.split(",");
                    return new WaterSensor(
                            data[0],
                            Long.valueOf(data[1]),
                            Integer.valueOf(data[2])
                    );
                })
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.
                                <WaterSensor>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                                //指定时间戳
                                .withTimestampAssigner( (element, recordTimestamp) -> element.getTs())
                );



            stream
                    .keyBy(WaterSensor::getId)
                    .process(new KeyedProcessFunction<String, WaterSensor, String>() {
                        @Override
                        public void processElement(WaterSensor value,
                                                  Context ctx,
                                                   Collector<String> out) throws Exception {
                           if (value.getVc()>20){
                              // long ts = ctx.timerService().currentWatermark() + 5000;
                               long ts = value.getTs() + 5000;// 5s后
                               System.out.println("定时器: " + ts);
                               ctx.timerService().registerEventTimeTimer(ts);
                           }

                        }
                        // 当定时器触发的时候, 执行这个方法
                        // 参数1: 正在触发的定时器的时间
                        @Override
                        public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
                            System.out.println("触发: " + timestamp);
                            out.collect(ctx.getCurrentKey()+"水位超过20！！");
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
