package net.acan.net.acan.flink.chapter06;

import net.acan.net.acan.flink.bean.UserBehavior;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

public class Flink01_PV_1 {
    public static void main(String[] args) {
        Configuration conf = new Configuration();
        conf.setInteger("rest.port", 20000);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        env.setParallelism(1);

        env.readTextFile("input/UserBehavior.csv")
                .map(line -> {
                    String[] data = line.split(",");
                    return new UserBehavior(
                           Long.valueOf(data[0]) ,
                            Long.valueOf(data[1]),
                            Integer.valueOf(data[2]),
                            data[3],
                            Long.valueOf(data[4])
                    );
                })
                .keyBy(new KeySelector<UserBehavior, String>() {
                    @Override
                    public String getKey(UserBehavior userBehavior) throws Exception {
                        return userBehavior.getBehavior();
                    }
                })
                .process(new KeyedProcessFunction<String, UserBehavior, Long>() {
                   long count = 0;
                    @Override
                    public void processElement(UserBehavior value,
                                               Context ctx,
                                               Collector<Long> out) throws Exception {
                        if ("pv".equals(ctx.getCurrentKey())) {
                            count++;
                        }
                        out.collect(count);
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
