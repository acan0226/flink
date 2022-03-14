package net.acan.net.acan.flink.chapter11.function;

import net.acan.net.acan.flink.bean.WaterSensor;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.functions.ScalarFunction;

import java.util.Locale;

import static org.apache.flink.table.api.Expressions.$;
import static org.apache.flink.table.api.Expressions.call;

public class Function01_Scalar {
    //Scalar Functions 标量函数
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
        tableEnv.createTemporaryView("sensor",table);

        // 使用自定义函数  将小写转为大写
        // 1. 在table api中使用
        // 1.1 内联的方式使用    call(MyToUpperCase.class, $("id")) 调用自定义函数 : 参数类名  参数2: 传给函数的参数   upper($('id'))

//        table.select($("id"),call(MyToUpperCase.class,$("id").as("my_upper")))
//                .execute()
//                .print();

        // 1.2 先注册后使用
//        tableEnv.createTemporaryFunction("my_upper", MyToUpperCase.class);
//        table.select($("id"),call(MyToUpperCase.class,$("id")))
//                .execute()
//                .print();


        // 2. 在sql中使用
        // 先注册
        tableEnv.createTemporaryFunction("my_upper", MyToUpperCase.class);
        tableEnv.sqlQuery("select id , my_upper(id) id1 from sensor")
                .execute()
                .print();
    }

    public static class MyToUpperCase extends ScalarFunction {
        //方法名必须是eval
        //返回值类型自己定
        public String eval(String s){
            return s == null ? null : s.toUpperCase();//把小写转为大写
        }

    }
}
