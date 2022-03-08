package net.acan.net.acan.flink.chapter08;

import net.acan.net.acan.flink.bean.HotItem;
import net.acan.net.acan.flink.bean.UserBehavior;
import net.acan.net.acan.flink.util.MyUtil;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.text.SimpleDateFormat;
import java.time.Duration;
import java.util.Comparator;
import java.util.List;

public class Project03_TopN {
    public static void main(String[] args) {
        Configuration conf = new Configuration();
        conf.setInteger("rest.port", 20000);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        env.setParallelism(1);

        env
                .readTextFile("input/UserBehavior.csv")
                        .map(new MapFunction<String, UserBehavior>() {
                            @Override
                            public UserBehavior map(String value) throws Exception {
                                String[] data = value.split(",");
                                return new UserBehavior(
                                        Long.valueOf(data[0]),
                                        Long.valueOf(data[1]),
                                        Integer.valueOf(data[2]),
                                        data[3],
                                        Long.valueOf(data[4])*1000
                                );
                            }
                        })
                .filter(a -> "pv".equals(a.getBehavior()))
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy
                                .<UserBehavior>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                                .withTimestampAssigner(
                                        ( (element, recordTimestamp) -> element.getTimestamp()
                                )
                ))
                .keyBy(UserBehavior::getItemId)
                .window(SlidingEventTimeWindows.of(Time.hours(2), Time.hours(1)))
                .aggregate(new AggregateFunction<UserBehavior, Long, Long>() {
                    @Override
                    public Long createAccumulator() {
                        return 0L;
                    }

                    @Override
                    public Long add(UserBehavior value, Long accumulator) {
                        return accumulator + 1;
                    }

                    @Override
                    public Long getResult(Long accumulator) {
                        return accumulator;
                    }

                    @Override
                    public Long merge(Long a, Long b) {
                        return null;
                    }
                }, new ProcessWindowFunction<Long, HotItem, Long, TimeWindow>() {
                    @Override
                    public void process(Long itemId,
                                        Context context,
                                        Iterable<Long> elements,
                                        Collector<HotItem> out) throws Exception {
                        Long count = elements.iterator().next();
                        long wEnd = context.window().getEnd();
                        out.collect(new HotItem(itemId,wEnd,count));
                    }
                })
                .keyBy(HotItem::getWEnd)
                .process(new KeyedProcessFunction<Long, HotItem, String>() {

                    private ListState<HotItem> hotItemState;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        hotItemState = getRuntimeContext().getListState(new ListStateDescriptor<HotItem>("hotItemState", HotItem.class));
                    }

                    @Override
                    public void processElement(HotItem value,
                                               Context ctx,
                                               Collector<String> out) throws Exception {
                        // 结束是1这个窗口的第一条数据进来的时候, 注册定时器
                        // 如果list状态中没有元素即是第一个,
                        if (!hotItemState.get().iterator().hasNext()) {
                         //註冊定時器
                            ctx.timerService().registerEventTimeTimer(value.getWEnd()+1000);
                        }
                        hotItemState.add(value);
                    }

                    @Override
                    public void onTimer(long timestamp,
                                        OnTimerContext ctx,
                                        Collector<String> out) throws Exception {
                        // 当定时器触发的时候, 证明同一个窗口内的点击量数据已经来齐了, 这个时候可以排序取topN
                        List<HotItem> hotItems = MyUtil.toList(hotItemState.get());
                        //排序
                        hotItems.sort((o1, o2) -> o2.getCount().compareTo(o1.getCount()));

                        String msg = "-------------------\n";
                        for (int i = 0,len=Math.min(3, hotItems.size()); i<len ; i++){
                            msg += hotItems.get(i)+"\n";

                        }
                        out.collect(msg);
                    }
                }).print();








        try {
            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
