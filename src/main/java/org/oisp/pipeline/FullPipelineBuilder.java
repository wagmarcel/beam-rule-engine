package org.oisp.pipeline;


import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import org.apache.beam.sdk.io.GenerateSequence;
import org.apache.beam.sdk.io.kafka.KafkaIO;
import org.apache.beam.sdk.io.kafka.KafkaRecord;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.Flatten;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;
import org.apache.kafka.common.serialization.StringSerializer;

import org.joda.time.Duration;
import org.oisp.collection.*;
import org.oisp.conf.Config;
import org.apache.beam.sdk.Pipeline;
import org.oisp.transformation.*;
import org.oisp.collection.Observation;

import java.util.List;

public class FullPipelineBuilder {


    static class KafkaToObservationFn extends DoFn<KafkaRecord<String, byte[]>, List<Observation>> {
        @ProcessElement
        public void processElement(ProcessContext c) {
            KafkaRecord<String, byte[]> record = c.element();
            Gson g = new Gson();
            List<Observation> obserations = g.fromJson(new String(record.getKV().getValue()), new TypeToken<List<Observation>>() {
            }.getType());
            c.output(obserations);
        }
    }

    static class StringToKVFn extends DoFn<Long, KV<String, String>> {
        @DoFn.ProcessElement
        public void processElement(ProcessContext c) {
            Long seqnum = c.element();
            KV<String, String> out_kv = KV.<String, String>of("", "rules-engine");
            c.output(out_kv);
        }
    }

    static class CombineKVFromByteArrayFn extends DoFn<KafkaRecord<String, byte[]>, KV<String, String>> {
        @ProcessElement
        public void processElement(ProcessContext c) {
            KafkaRecord<String, byte[]> record = c.element();
            KV<String, String> out_kv = KV.<String, String>of("key", record.getKV().getKey() + " " + new String(record.getKV().getValue()));
            c.output(out_kv);
        }
    }

    static public Pipeline build(PipelineOptions options, Config conf){
        Pipeline p = Pipeline.create(options);

        //Observation Pipeline
        KafkaSourceObservationsProcessor observationsKafka = new KafkaSourceObservationsProcessor(conf);
        PCollection<List<RulesWithObservation>> rwo = p.apply(observationsKafka.getTransform())
                .apply(ParDo.of(new KafkaToObservationFn()))
                .apply(ParDo.of(new GetComponentRulesTask(conf)));
        PCollection<KV<String,RuleWithRuleConditions>> basicRulePipeline =
                rwo
                        .apply(ParDo.of(new CheckBasicRule()));

                //.setCoder(KvCoder.of(StringUtf8Coder.of(), SerializableCoder.of(Rule.class)));
        PCollection<KV<String, RuleWithRuleConditions>> timeBasedRulePipeline =
                rwo
                        .apply(ParDo.of(new CheckTimeBasedRule()))
                        .apply(ParDo.of(new PersistTimeBasedRuleState()));
        PCollectionList<KV<String, RuleWithRuleConditions>> ruleColl = PCollectionList.of(basicRulePipeline).and(timeBasedRulePipeline);
        ruleColl
                .apply(Flatten.<KV<String, RuleWithRuleConditions>>pCollections())
                .apply(ParDo.of(new PersistBasicRuleState()))
                .apply(ParDo.of(new CheckRuleFulfillment()));

        rwo.apply(ParDo.of(new PersistObservationTask(conf)))
                .apply(ParDo.of(new CheckObservationInRulesTask(conf)))
                .apply(ParDo.of(new PersistComponentAlertsTask(conf)))
                .apply(ParDo.of(new CheckRulesTask(conf)))
                .apply(ParDo.of(new SendAlertsTask(conf)));

        //Heartbeat
        String serverUri = conf.get(Config.KAFKA_URI_PROPERTY).toString();
        System.out.println("serverUri:" + serverUri);
        //Pipeline p = Pipeline.create(options);
        p.apply(GenerateSequence.from(0).withRate(1, Duration.standardSeconds(1)))
                .apply(ParDo.of(new StringToKVFn()))
                .apply(KafkaIO.<String, String>write()
                        .withBootstrapServers(serverUri)
                        .withTopic("heartbeat")
                        .withKeySerializer(StringSerializer.class)
                        .withValueSerializer(StringSerializer.class));

        //Rules-Update
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
