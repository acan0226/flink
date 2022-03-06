package net.acan.net.acan.flink.chapter07.watermark;

import net.acan.net.acan.flink.bean.WaterSensor;
import net.acan.net.acan.flink.util.MyUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.time.Duration;
import java.util.List;

public class Watermark03_Output {
    public static void main(String[] args) {
        Configuration conf = new Configuration();
        conf.setInteger("rest.port", 20000);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        env.setParallelism(1);

        SingleOutputStreamOperator<String> main = env
                .socketTextStream("hadoop162", 9999)
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
                .sideOutputLateData(new OutputTag<WaterSensor>("late"){})
                .process(new ProcessWindowFunction<WaterSensor, String, String, TimeWindow>() {
                    @Override
                    public void process(String s,
                                        Context context,
                                        Iterable<WaterSensor> elements,
                                        Collector<String> out) throws Exception {
                        WaterSensor result = elements.iterator().next();
                        List<WaterSensor> list = MyUtil.<WaterSensor>toList(elements);
                        out.collect(s + " " + context.window() + list);
                    }
                });

        main.print();
        main.getSideOutput(new OutputTag<WaterSensor>("late"){}).print();
        //因为这个OutputTag这个类有泛型，所以改成匿名内部类，加上{}，才能加入到运行池中，
        // 要是没加{}，后面是侦测不到这个泛型的

        try {
            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }


}
   /*
    flink 解决乱序数据:
        1. 事件时间+水印时间

        2. 允许迟到
        当水印到了窗口的关闭时间, 先对窗口内的数据进行计算, 但是窗口不关闭,
        如果在允许时间内来的数据, 还可以进入这个窗口, 参与计算. 当超过这个允许时间后再斟酌的关闭窗口

        .allowedLateness(Time.seconds(2))

        3. 侧输出流
        当窗口真正关闭之后, 如果仍然有迟到数据, 则把迟到放入到侧输出流中.
        侧输出流的第一个作用:装载真正迟到的数据(所在窗口关闭)
        */