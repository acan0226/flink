package net.acan.net.acan.flink.chapter11.function;

import net.acan.net.acan.flink.bean.WaterSensor;
import net.acan.net.acan.flink.bean.WordLen;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.functions.AggregateFunction;
import org.apache.flink.table.functions.TableFunction;

import static org.apache.flink.table.api.Expressions.$;
import static org.apache.flink.table.api.Expressions.call;

public class Function03_Agg {
    //table Functions 制表函数  一进多出，对应hive中的UDTF-
    public static void main(String[] args) {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        environment.setParallelism(1);
        DataStreamSource<WaterSensor> stream = environment.fromElements(
                new WaterSensor("sensor_1", 1000L, 10),
                new WaterSensor("sensor_1", 2000L, 20),
                new WaterSensor("sensor_2", 3000L, 30),
                new WaterSensor("sensor_1", 4000L, 40),
                new WaterSensor("sensor_1", 5000L, 50),
                new WaterSensor("sensor_2", 6000L, 60)
        );


        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(environment);

        // 3. 把流转成一个表(动态表)
        Table table = tableEnv.fromDataStream(stream);
        table.printSchema();
        tableEnv.createTemporaryView("sensor",table);

        // 使用自定义函数
        // 1. 在table api中使用
        // 1.1 内联的方式使用
        table
                .groupBy($("id"))
                .select($("id"),call(MyAgg.class,$("vc")).as("avg_vc"))
                .execute()
                .print();


        // 1.2 先注册后使用
        //先注册
        tableEnv.createTemporaryFunction("my_avg", MyAgg.class);


        // 2. 在sql中使用
        // 先注册
        tableEnv.createTemporaryFunction("my_avg", MyAgg.class);

        tableEnv.sqlQuery(
                "select " +
                        "id , my_avg(vc) avg1 " +
                        "from sensor group by id"
        )
                .execute()
                .print();

    }


    public static class MyAgg extends AggregateFunction<Double,Avg> {
        // 返回最终聚合的结果
        @Override
        public Double getValue(Avg acc) {
            return acc.avg();
        }
        // 初始化一个累加器
        @Override
        public Avg createAccumulator() {
            return new Avg();
        }
        // 返回值必须是void 方法名必须是:accumulate 第一个参数: 必须是累加器 后面的参数传递到这里的值
        public void accumulate (Avg avg,Integer vc){
            avg.sum += vc;
            avg.count++;
        }
    }

    public static class Avg {
       public Integer sum = 0;
       public Long count = 0L;

       public Double avg(){
           return sum *1.0/count;
       }
    }
}

