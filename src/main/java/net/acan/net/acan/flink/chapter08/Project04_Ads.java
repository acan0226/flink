package net.acan.net.acan.flink.chapter08;

import net.acan.net.acan.flink.bean.AdsClickLog;
import net.acan.net.acan.flink.bean.WaterSensor;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.state.ReducingState;
import org.apache.flink.api.common.state.ReducingStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.time.Duration;

public class Project04_Ads {
    public static void main(String[] args) {
        Configuration conf = new Configuration();
        conf.setInteger("rest.port", 20000);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        env.setParallelism(1);

env.readTextFile("input/AdClickLog.csv")
        .map(line->{
            String[] data = line.split(",");
            return new AdsClickLog(
                    Long.valueOf(data[0]),
                    Long.valueOf(data[1]),
                    data[2],
                    data[3],
                    Long.valueOf(data[4]) * 1000
            );
        })
        .assignTimestampsAndWatermarks(
                WatermarkStrategy
                        .<AdsClickLog>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                        .withTimestampAssigner( (element, recordTimestamp) -> element.getTimestamp())
        )
        //每个用户对每个广告的点击
        .keyBy(key->key.getUserId()+"_"+ key.getAdsId())
        .process(new KeyedProcessFunction<String, AdsClickLog, String>() {

            private ValueState<Boolean> blState;
            private ReducingState<Long> countState;

            @Override
            public void open(Configuration parameters) throws Exception {
                countState = getRuntimeContext().getReducingState(new ReducingStateDescriptor<Long>("countState",
                        (ReduceFunction<Long>) Long::sum, Long.class));
                blState = getRuntimeContext().getState(new ValueStateDescriptor<Boolean>("blState", Boolean.class));
            }

            @Override
            public void processElement(AdsClickLog value,
                                       Context ctx,
                                       Collector<String> out) throws Exception {
                countState.add(1L);
                Long count = countState.get();
                String msg = value.getUserId()+"点击"+value.getAdsId()+"广告量"+count;
                if (count>99) {
                    if (blState.value() == null) {
                        out.collect(msg+="超过99,,加入黑名单");
                        blState.update(true);
                    }

                }else{
                out.collect(msg);
            }}
        })
        .print();



    try {
            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
