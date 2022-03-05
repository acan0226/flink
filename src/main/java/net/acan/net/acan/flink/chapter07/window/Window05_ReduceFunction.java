package net.acan.net.acan.flink.chapter07.window;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

public class Window05_ReduceFunction {
    public static void main(String[] args) {
        Configuration conf = new Configuration();
        conf.setInteger("rest.port", 20000);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        env.setParallelism(1);

        env.socketTextStream("hadoop162",9999)
                .flatMap(new FlatMapFunction<String, Tuple2<String,Long>>() {
                    @Override
                    public void flatMap(String s,
                                        Collector<Tuple2<String, Long>> collector) throws Exception {
                        for (String s1 : s.split(" ")) {
                            collector.collect(Tuple2.of(s1,1L));
                        }
                    }
                })
                .keyBy(x->x.f0)
                .window(TumblingProcessingTimeWindows.of(Time.seconds(5)))
                .reduce(new ReduceFunction<Tuple2<String, Long>>() {
                    @Override
                    public Tuple2<String, Long> reduce(Tuple2<String, Long> t1,
                                                       Tuple2<String, Long> t2) throws Exception {
                        System.out.println("xxxxxx");
                        return Tuple2.of(t1.f0,t1.f1+ t2.f1);
                    }
                }, new ProcessWindowFunction<Tuple2<String, Long>, String, String, TimeWindow>() {
                    @Override
                    public void process(String key,
                                        Context ctx,
                                        Iterable<Tuple2<String, Long>> elements,// 这个集合有且仅有一个元素, 就是前面聚合函数计算的最终结果
                                        Collector<String> out) throws Exception {
                        long start = ctx.window().getStart();
                        long end = ctx.window().getEnd();

                        Tuple2<String, Long> result = elements.iterator().next();
                        out.collect(key + "   " + start + "   " + end + "   " + result);
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
窗口处理函数:

增量
    sum max min maxBy minBy

    reduce

    aggregate

全量
    process

 */