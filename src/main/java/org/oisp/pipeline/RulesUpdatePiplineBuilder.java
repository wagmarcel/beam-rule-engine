package org.oisp.pipeline;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.kafka.KafkaRecord;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.oisp.conf.Config;
import org.oisp.transformation.DownloadRulesTask;
import org.oisp.transformation.KafkaSourceProcessor;
import org.oisp.transformation.KafkaSourceRulesUpdateProcessor;
import org.oisp.transformation.PersistRulesTask;

public class RulesUpdatePiplineBuilder {
    static class CombineKVFromByteArrayFn extends DoFn<KafkaRecord<String, byte[]>, KV<String, String>> {
        @ProcessElement
        public void processElement(ProcessContext c) {
            KafkaRecord<String, byte[]> record = c.element();
            KV<String, String> out_kv = KV.<String, String>of("key", record.getKV().getKey() + " " + new String(record.getKV().getValue()));
            c.output(out_kv);
        }
    }
    static public Pipeline build(PipelineOptions options, Config conf) {
        Pipeline p = Pipeline.create(options);
        KafkaSourceProcessor rulesKafka = new KafkaSourceRulesUpdateProcessor(conf);
        DownloadRulesTask downloadRulesTask = new DownloadRulesTask(conf);
        PersistRulesTask persistRulesTask = new PersistRulesTask(conf);
        p.apply(rulesKafka.getTransform())
                .apply(ParDo.of(new CombineKVFromByteArrayFn()))
                .apply(ParDo.of(downloadRulesTask))
                .apply(ParDo.of(persistRulesTask));

        return p;
    }
}
