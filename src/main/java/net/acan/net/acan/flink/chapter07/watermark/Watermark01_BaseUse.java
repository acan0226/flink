package net.acan.net.acan.flink.chapter07.watermark;

import net.acan.net.acan.flink.bean.WaterSensor;
import net.acan.net.acan.flink.util.MyUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.util.List;

public class Watermark01_BaseUse {
    public static void main(String[] args) {
        Configuration conf = new Configuration();
        conf.setInteger("rest.port", 20000);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        env.setParallelism(1);

        env.socketTextStream("hadoop162",9999)
                .map(new MapFunction<String, WaterSensor>() {

                    @Override
                    public WaterSensor map(String value) throws Exception {
                        String[] data = value.split(",");
                        return new WaterSensor(
                                data[0],
                                Long.valueOf(data[1]),
                                Integer.valueOf(data[2])
                        );
                    }
                })
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.
                                <WaterSensor>forBoundedOutOfOrderness(Duration.ofSeconds(3)) // 乱序不程度
                                .withTimestampAssigner(
                                        new SerializableTimestampAssigner<WaterSensor>() {

                                            @Override
                                            public long extractTimestamp(WaterSensor element, long recordTimestamp) {
                                                //返回事件时间
                                                return element.getTs();
                                            }
                                        })
                                .withIdleness(Duration.ofSeconds(10)) // 防止数据倾斜带来的水印不更新(10秒并行度2不更新就强制更新)
                )

                .keyBy(WaterSensor::getId)
                .window(TumblingEventTimeWindows.of(Time.seconds(5)))
                .process(new ProcessWindowFunction<WaterSensor, String, String, TimeWindow>() {
                    @Override
                    public void process(String s,
                                        Context context,
                                        Iterable<WaterSensor> elements,
                                        Collector<String> out) throws Exception {
                        WaterSensor result = elements.iterator().next();
                            List<WaterSensor> list = MyUtil.<WaterSensor>toList(elements);
                            out.collect(s+" "+context.window()+list);
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
/*
new BoundedOutOfOrdernessWatermarks<>(maxOutOfOrderness);

this.maxTimestamp = Long.MIN_VALUE + outOfOrdernessMillis + 1;

每来一个元素执行一次
public void onEvent
    maxTimestamp = Math.max(maxTimestamp, eventTimestamp);
    计算最大时间戳

周期性的执行  默认 200ms
public void onPeriodicEmit
    output.emitWatermark(new Watermark(maxTimestamp - outOfOrdernessMillis - 1));



---
多并行度下水印不更新:
1. 把并行度改成1
2. 解决source的数据倾斜
3. 强制更新水印
    添加一个设置
 */
