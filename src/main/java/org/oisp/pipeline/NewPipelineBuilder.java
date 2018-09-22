package org.oisp.pipeline;


import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import org.apache.beam.sdk.io.GenerateSequence;
import org.apache.beam.sdk.io.kafka.KafkaIO;
import org.apache.beam.sdk.io.kafka.KafkaRecord;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.transforms.windowing.AfterProcessingTime;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.transforms.windowing.Trigger;
import org.apache.commons.digester.Rules;
import org.apache.kafka.common.serialization.StringSerializer;
import org.joda.time.Duration;
import org.oisp.collection.Observation;
import org.oisp.conf.Config;
import org.apache.beam.sdk.Pipeline;
import org.oisp.transformation.*;
import org.oisp.collection.Rule;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.HashMap;
import java.util.stream.Collectors;
//import java.util.concurrent.TimeUnit;

public class NewPipelineBuilder {


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

    public static class CombineRules implements SerializableFunction<Iterable<Map<String, List<Rule>>>, Map<String, List<Rule>>> {
        @Override
        public Map<String, List<Rule>> apply(Iterable<Map<String, List<Rule>>> input) {
            Map<String, List<Rule>> output = new HashMap<String, List<Rule>>();
            while (input.iterator().hasNext()) {
                Map<String, List<Rule>> elem = input.iterator().next();
                output.putAll(elem);
            }
            return output;
        }
    }

    static public Pipeline build(PipelineOptions options, Config conf){
        Pipeline p = Pipeline.create(options);


        //Rules-Update
        KafkaSourceProcessor rulesKafka = new KafkaSourceRulesUpdateProcessor(conf);
        RuleSideInputTransform downloadRulesTask = new RuleSideInputTransform(conf);
        final PCollectionView<Map<String, List<Rule>>> rules =
        p.apply(rulesKafka.getTransform())
                .apply(Window.<KafkaRecord<String, byte[]>>into(FixedWindows.of(Duration.standardSeconds(1)))
                        .triggering(AfterProcessingTime.pastFirstElementInPane()).discardingFiredPanes())
                .apply(ParDo.of(new CombineKVFromByteArrayFn()))
                .apply(ParDo.of(downloadRulesTask))
                .apply(Combine.globally( new CombineRules()).asSingletonView());




//Observation Pipeline
        KafkaSourceObservationsProcessor observationsKafka = new KafkaSourceObservationsProcessor(conf);
        p.apply(observationsKafka.getTransform())
                .apply(ParDo.of(new FullPipelineBuilder.KafkaToObservationFn()))
                .apply(ParDo.of(new GetComponentWithRulesTransformation(conf, rules)).withSideInputs(rules));
        return p;
    }
}
