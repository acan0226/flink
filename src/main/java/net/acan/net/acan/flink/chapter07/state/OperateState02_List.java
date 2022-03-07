package net.acan.net.acan.flink.chapter07.state;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class OperateState02_List {
    public static void main(String[] args) {
               /*
        从socket读数据, 存入到一个ArrayList
        如何把ArrayList中的数据存入到列表状态, 并在程序恢复能够从状态把数据再恢复到ArrayList
         */
        Configuration conf = new Configuration();
        conf.setInteger("rest.port", 20000);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        env.setParallelism(2);

        env.enableCheckpointing(3000);
        env.socketTextStream("hadoop162", 9999)
                .map(new MyMapFunction())
                .print();




        try {
            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static class MyMapFunction implements MapFunction<String,Long>, CheckpointedFunction {
        private Long count = 0L;
        private ListState<Long> listState ;

        @Override
        public Long map(String value) throws Exception {
            count++;
            return count;

        }
        // 初始化状态
        // 从状态中恢复数据. 当程序启动的时候执行.
        // 执行的次数和并行度一致
        @Override
        public void initializeState(FunctionInitializationContext context) throws Exception {
            System.out.println("MyMapFunction.initializeState");
            listState = context.
                    getOperatorStateStore().
                    getListState(new ListStateDescriptor<Long>("state", Long.class));
            for (Long l : listState.get()) {
                count += l;
            }

        }
        // 对状态做快照, 周期性的执行, 把状态进行保存
        // 执行的次数和并行度一致
        @Override
        public void snapshotState(FunctionSnapshotContext context) throws Exception {
            System.out.println("MyMapFunction.snapshotState");
            listState.clear();
            listState.add(count);
        }


    }
}
