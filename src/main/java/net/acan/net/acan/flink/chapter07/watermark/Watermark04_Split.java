package net.acan.net.acan.flink.chapter07.watermark;

import net.acan.net.acan.flink.bean.WaterSensor;
import net.acan.net.acan.flink.util.MyUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.time.Duration;
import java.util.List;

public class Watermark04_Split {
    public static void main(String[] args) {
        Configuration conf = new Configuration();
        conf.setInteger("rest.port", 20000);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        env.setParallelism(1);

        SingleOutputStreamOperator<WaterSensor> main = env
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
                .process(new ProcessFunction<WaterSensor, WaterSensor>() {
                             // acan 放入主流   bcan 放一个侧输出流  其他放入一个侧输出流

                             @Override
                             public void processElement(WaterSensor value,
                                                        Context ctx,
                                                        Collector<WaterSensor> out) throws Exception {
                                if ("acan".equals((value.getId()))){
                                    out.collect(value);
                                }else if("bcan".equals(value.getId())){
                                    ctx.output(new OutputTag<WaterSensor>("bcan"){},value);
                                }else{
                                    ctx.output(new OutputTag<WaterSensor>("other"){}, value);
                                }
                             }




    });

        main.print("acan");
        main.getSideOutput(new OutputTag<WaterSensor>("bcan"){}).print("bcan");
        main.getSideOutput(new OutputTag<WaterSensor>("other"){}).print("other");
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

            侧输出流的第二个作用:
            分流

        侧输出流的第一个作用:装载真正迟到的数据(所在窗口关闭)
        */