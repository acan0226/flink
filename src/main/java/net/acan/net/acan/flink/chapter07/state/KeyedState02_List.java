package net.acan.net.acan.flink.chapter07.state;

import net.acan.net.acan.flink.bean.WaterSensor;
import net.acan.net.acan.flink.util.MyUtil;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.util.Comparator;
import java.util.List;

public class KeyedState02_List {
    public static void main(String[] args) {
        Configuration conf = new Configuration();
        conf.setInteger("rest.port", 20000);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        env.setParallelism(1);
        
        env
                .socketTextStream("hadoop162", 9999)
                .map((MapFunction<String, WaterSensor>) value -> {
                    String[] data = value.split(",");
                    return new WaterSensor(
                            data[0],
                            Long.valueOf(data[1]),
                            Integer.valueOf(data[2])
                    );
                })
                .keyBy(WaterSensor::getId)
                .process(new KeyedProcessFunction<String, WaterSensor, String>() {

                    private ListState<Integer> top3Vc;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        top3Vc = getRuntimeContext().getListState(new ListStateDescriptor<Integer>("top3Vc", Types.INT));
                    }

                    @Override
                    public void processElement(WaterSensor value,
                                               Context ctx,
                                               Collector<String> out) throws Exception {
                    top3Vc.add(value.getVc());
                        List<Integer> list = MyUtil.toList(top3Vc.get());
                        //list.sort((o1, o2) -> o2.compareTo(o1));
                        list.sort(Comparator.reverseOrder());  // 对list中的要元素进行排序

                        if (list.size()>3) {
                            list.remove(list.size()-1);
                        }
                        out.collect("top3"+list);
                        //更新top3狀態
                        top3Vc.update(list);
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
