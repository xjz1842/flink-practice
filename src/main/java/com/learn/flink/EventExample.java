package com.learn.flink;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

public class EventExample {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

//        env.addSource()
//                .keyBy(e -> e.key)
//                .flatMap(new Deduplicator())
//                .print();

         env.execute();
    }
}
