package org.oisp.conf;

import java.util.HashMap;
import java.util.List;

public class Config {
    public static String KAFKA_RULE_UPDATE_TOPIC_PROPERTY = "Kafka_Rule_Update_Topic_Property";
    public static String KAFKA_BOOTSTRAP_SERVERS_PROPERTY = "Kafka_Bootstrap_Servers";
    public static String KAFKA_ZOOKEEPER_SERVERS_PROPERTY = "Kafka_ZooKeeper_Servers";

    private HashMap<String, Object> conf;
    Config(){
        conf = new HashMap<String, Object>();
    }

    public Object get(String key) {
        return conf.get(key);
    }

    public Config put(String key, Object value){
        conf.put(key,value);
        return this;
    }
    public HashMap<String, Object> getHash(){
        return conf;
    }

    public Config put(HashMap<String, Object> conf_in){
        conf = conf_in;
        return this;
    }
    public String[] getStringArray(String key) {
        String[] result;
        try {
            result = ((List<String>) conf.get(key)).toArray(new String[0]);
        } catch (NullPointerException exception) {
            result = null;
        }
        return result;
    }
}
