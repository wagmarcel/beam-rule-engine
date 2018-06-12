package org.oisp.collection;

public class Message {
    Object msg;
    long timestamp;

    public Message(Object msg_in, long timestamp_in){msg = msg_in; timestamp = timestamp_in;}
    public Object msg(){return msg;}
}
