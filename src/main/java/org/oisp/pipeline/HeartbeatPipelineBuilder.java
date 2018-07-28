package org.oisp.pipeline;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.GenerateSequence;
import org.apache.beam.sdk.io.kafka.KafkaIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.kafka.common.serialization.StringSerializer;
import org.joda.time.Duration;
import org.oisp.conf.Config;

public class HeartbeatPipelineBuilder {
    static class StringToKVFn extends DoFn<Long, KV<String, String>> {
        @DoFn.ProcessElement
        public void processElement(ProcessContext c) {
            Long seqnum = c.element();
            KV<String, String> out_kv = KV.<String, String>of("", "beam-rule-engine");
            c.output(out_kv);
        }
    }

    static public Pipeline build(PipelineOptions options, Config conf) {
        Pipeline p = Pipeline.create(options);
        p.apply(GenerateSequence.from(0).withRate(1, Duration.standardSeconds(1)))
                .apply(ParDo.of(new StringToKVFn()))
                .apply(KafkaIO.<String, String>write()
                        .withBootstrapServers("localhost:9092")
                        .withTopic("heartbeat")
                        .withKeySerializer(StringSerializer.class)
                        .withValueSerializer(StringSerializer.class));

        return p;
    }

}
