package net.acan.net.acan.flink.chapter06;

import net.acan.net.acan.flink.bean.MarketingUserBehavior;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.Random;

public class Flink03_APP {
    public static void main(String[] args) {
        Configuration conf = new Configuration();
        conf.setInteger("rest.port", 20000);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        env.setParallelism(1);

        env.addSource(new AppSource())
                .map(new MapFunction<MarketingUserBehavior, Tuple2<String,Long>>() {
                    @Override
                    public Tuple2<String, Long> map(MarketingUserBehavior mub) throws Exception {
                        return Tuple2.of(mub.getBehavior()+"_"+mub.getChannel(),1L);
                    }
                })
                .keyBy(k->k.f0)
                .sum(1)
                .print();

        try {
            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static class AppSource implements SourceFunction<MarketingUserBehavior> {
        @Override
        public void run(SourceContext<MarketingUserBehavior> ctx) throws Exception {
                     /*
            public class MarketingUserBehavior {
                private Long userId;
                private String behavior;
                private String channel;
                private Long timestamp;
}
             */
            Random random = new Random();
            String[] behaviors = {"download", "install", "update", "uninstall"};
            String[] channels = {"huawei", "小米", "oppo", "vivo", "apple"};

            while(true){
                Long userId = Long.valueOf(random.nextInt(2260)+ 1);
                String behavior= behaviors[random.nextInt(behaviors.length)];
                String channel = channels[random.nextInt(channels.length)];
                Long timestamp = System.currentTimeMillis();
                ctx.collect(new MarketingUserBehavior(userId,behavior,channel,timestamp));
                Thread.sleep(100);
            }
        }

        @Override
        public void cancel() {

        }
    }
}
