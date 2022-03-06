package net.acan.net.acan.flink.chapter07.window;

import net.acan.net.acan.flink.bean.WaterSensor;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

public class Window05_Aggregate {
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
                .keyBy(WaterSensor::getId)
                .window(TumblingProcessingTimeWindows.of(Time.seconds(8)))
                .aggregate(new AggregateFunction<WaterSensor, Avg, Double>() {
                    // 初始化一个累加器
                    // 第一个数据来到时候触发
                    @Override
                    public Avg createAccumulator() {
                        return new Avg();
                    }

                    // 对进来的元素进行累加
                    // 每来一条数据触发一次
                    @Override
                    public Avg add(WaterSensor value, Avg accumulator) {
                        accumulator.vcSum += value.getVc();
                        accumulator.count++;
                        return accumulator;
                    }

                    // 返回最终的结果
                    // 窗口关闭的时候触发一次
                    @Override
                    public Double getResult(Avg accumulator) {
                        return accumulator.vcSum * 1.0 / accumulator.count;
                    }

                    // 合并累加器
                    // 一般不用实现: 如果窗口是session窗口, 才需要实现
                    // 其他窗口不会触发
                    @Override
                    public Avg merge(Avg a, Avg b) {
                        return null;
                    }
                }, new ProcessWindowFunction<Double, String, String, TimeWindow>() {
                    @Override
                    public void process(String s,
                                        Context context,
                                        Iterable<Double> elements,
                                        Collector<String> out) throws Exception {
                        Double result = elements.iterator().next();
                        out.collect(s+"平均值"+result+" "+context.window());
                    }
                })
                .print();


        try {
            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static class Avg {
        public Integer vcSum = 0 ;
        public Long count = 0L ;
    }
}
/*
窗口处理函数:

增量
    sum max min maxBy minBy

    reduce

    aggregate

全量
    process

 */