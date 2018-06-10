package org.oisp;


import com.google.common.collect.ImmutableMap;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.GenerateSequence;
import org.apache.beam.sdk.io.kafka.KafkaRecord;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.io.kafka.KafkaIO;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.joda.time.Duration;
import org.oisp.conf.RuleEngineOptions;
import org.oisp.conf.ConfigFactory;
import org.oisp.conf.Config;
import org.oisp.transformation.KafkaSourceProcessor;
import org.oisp.transformation.KafkaSourceRulesUpdateProcessor;

/**
 * Rule-engine-test
 */


class CombineKVFn extends DoFn<KafkaRecord<String, String>, KV<String, String>> {
    @ProcessElement
    public void processElement(ProcessContext c) {
        KafkaRecord<String, String> record = c.element();
        KV<String, String> out_kv = KV.<String, String>of("key", record.getKV().getKey() + " " + record.getKV().getValue());
        c.output(out_kv);
    }
}


class CombineKVFromByteArrayFn extends DoFn<KafkaRecord<String, byte[]>, KV<String, String>> {
    @ProcessElement
    public void processElement(ProcessContext c) {
        KafkaRecord<String, byte[]> record = c.element();
        KV<String, String> out_kv = KV.<String, String>of("key", record.getKV().getKey() + " " + new String(record.getKV().getValue()));
        c.output(out_kv);
    }
}

class StringToKVFn extends DoFn<Long, KV<String, String>> {
    @DoFn.ProcessElement
    public void processElement(ProcessContext c) {
        Long seqnum = c.element();
        KV<String, String> out_kv = KV.<String, String>of("", "beam-rule-engine");
        c.output(out_kv);
    }
}


public abstract class RuleEngineBuild {
    public static void main(String[] args) {
        System.out.println("Hello World!");
        PipelineOptions options = PipelineOptionsFactory
                .fromArgs(args)
                .withValidation()
                .as(RuleEngineOptions.class);

        Pipeline p = Pipeline.create(options);
        Pipeline heartbeat = Pipeline.create();
        Pipeline rulesUpdate = Pipeline.create(options);

        Config conf = new ConfigFactory().getConfigFromArgs(((RuleEngineOptions) options).getJSONConfig());

        // Test Pipeline
        p.apply(KafkaIO.<String, String>read()
                .withBootstrapServers("localhost:9092")
                .withTopic("topic1")
                .withKeyDeserializer(StringDeserializer.class)
                .withValueDeserializer(StringDeserializer.class)
                .updateConsumerProperties(ImmutableMap.of("group.id", "rule-engine"))
                .withLogAppendTime()
                .withReadCommitted()
                .commitOffsetsInFinalize()
                .withReadCommitted())
                .apply(ParDo.of(new CombineKVFn()))
                .apply(Window.into(FixedWindows.of(Duration.standardMinutes(2))))
                //.apply(Flatten.<String>pCollections());
                .apply(KafkaIO.<String, String>write()
                        .withBootstrapServers("localhost:9092")
                        .withTopic("topic2")
                        .withKeySerializer(StringSerializer.class)
                        .withValueSerializer(StringSerializer.class)
                );

        //Heartbeat pipeline => creates heartbeat every second
        //TODO: make value and time interval configurable
        heartbeat.apply(GenerateSequence.from(0).withRate(1, Duration.standardSeconds(1)))
                .apply(ParDo.of(new StringToKVFn()))
                .apply(KafkaIO.<String, String>write()
                        .withBootstrapServers("localhost:9092")
                        .withTopic("heartbeat")
                        .withKeySerializer(StringSerializer.class)
                        .withValueSerializer(StringSerializer.class));


        // First experiment with RulesUpdate pipeline from gearpump
        KafkaSourceProcessor rulesKafka = new KafkaSourceRulesUpdateProcessor(conf);
        rulesUpdate.apply(rulesKafka.getTransform())
                .apply(ParDo.of(new CombineKVFromByteArrayFn()))
                .apply(KafkaIO.<String, String>write()
                                .withBootstrapServers("localhost:9092")
                                .withTopic("topic2")
                                .withKeySerializer(StringSerializer.class)
                                .withValueSerializer(StringSerializer.class));

        //heartbeat.run();
        rulesUpdate.run();
        p.run().waitUntilFinish();
    }
}
