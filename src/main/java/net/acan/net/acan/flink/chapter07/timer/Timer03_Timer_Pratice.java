package net.acan.net.acan.flink.chapter07.timer;

import net.acan.net.acan.flink.bean.WaterSensor;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.time.Duration;

public class Timer03_Timer_Pratice {
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
                        boolean isFirst = true;
                        Long ts;
                        int lastVc;
                        @Override
                        public void processElement(WaterSensor value,
                                                  Context ctx,
                                                   Collector<String> out) throws Exception {
                        if(isFirst){

                            isFirst = false;
                                    //注册定时器
                            ts = value.getTs()+5000;
                            ctx.timerService().registerEventTimeTimer(ts);
                            lastVc = value.getVc();
                        }else{
                            // 不是第一条, 判断这次的水位相比上次是否上次,
                            // 如果上升则什么都不坐, 如果没有上升, 则删除定时器
                            if(value.getVc()< lastVc){
                                ctx.timerService().deleteEventTimeTimer(ts);
                                // 重新注册新的定时器
                                ts = value.getTs()+5000;
                                ctx.timerService().registerEventTimeTimer(ts);
                                lastVc = value.getVc();
                            }else{
                                System.out.println("水位上升不操作");
                            }
                            }

                        }
                        // 当定时器触发的时候, 执行这个方法
                        // 参数1: 正在触发的定时器的时间
                        @Override
                        public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {

                            out.collect(ctx.getCurrentKey()+"水位连续上升");
                            isFirst = true;
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
