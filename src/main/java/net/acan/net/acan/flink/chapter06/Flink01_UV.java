package net.acan.net.acan.flink.chapter06;

import lombok.var;
import net.acan.net.acan.flink.bean.UserBehavior;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.util.HashSet;


public class Flink01_UV {
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
                    HashSet<Long> userIds = new HashSet<>();

                    @Override
                    public void processElement(UserBehavior value,
                                               Context ctx,
                                               Collector<Long> out) throws Exception {
                        /*if ("pv".equals(ctx.getCurrentKey())) {
                            int pre = userIds.size();
                            userIds.add(ub.getUserId());
                            int post = userIds.size();

                            // 当uv增长的时候才需要向外输出
                            if (post > pre) {

                                out.collect((long) userIds.size());
                            }
                        }*/
                        if ("pv".equals(ctx.getCurrentKey())) {
                            // 返回值是true表示这次是新增, 否则就是一个旧元素
                            if(userIds.add(value.getUserId())) {
                                out.collect((long) userIds.size());

                            }
                        }

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
