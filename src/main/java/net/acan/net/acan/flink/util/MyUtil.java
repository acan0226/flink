package net.acan.net.acan.flink.util;

import net.acan.net.acan.flink.bean.WaterSensor;

import java.util.ArrayList;
import java.util.List;

public class MyUtil {
    public static<T> List<T> toList(Iterable<T> it) {
        List<T> list = new ArrayList<>();
        for (T t : it) {
           list.add(t);
        }
        return list;
    }
}
