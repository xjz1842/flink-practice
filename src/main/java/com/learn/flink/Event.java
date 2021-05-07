package com.learn.flink;

public class Event {
    public final String key;
    public final long timestamp;

    public Event(String key, long timestamp){
        this.key = key;
        this.timestamp = timestamp;
    }

}
