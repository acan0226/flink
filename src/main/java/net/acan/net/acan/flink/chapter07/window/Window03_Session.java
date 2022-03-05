package net.acan.net.acan.flink.chapter07.window;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.ProcessingTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.assigners.SessionWindowTimeGapExtractor;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.Date;
import java.util.Random;

public class Window03_Session {
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
                .window(ProcessingTimeSessionWindows.withGap(Time.seconds(3)))
                .process(new ProcessWindowFunction<Tuple2<String, Long>, String, String, TimeWindow>() {
                    @Override
                    public void process(String s,
                                       Context context,
                                        Iterable<Tuple2<String, Long>> elements,    //存储了窗口内的所有元素
                                        Collector<String> out) throws Exception {
                        Date start = new Date(context.window().getStart());
                        Date end = new Date(context.window().getEnd());
                            int count = 0 ;
                        for (Tuple2<String, Long> element : elements) {
                            count++;
                        }
                        out.collect("key = " + s + ", 窗口开始: " + start + ", 窗口结束: " + end + ", 个数: " + count);
                    }
                })
                //.sum(1)
                .print();


        try {
            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
