package org.oisp.pipeline;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import org.apache.beam.sdk.io.kafka.KafkaRecord;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.oisp.collection.Observation;
import org.oisp.conf.Config;
import org.apache.beam.sdk.Pipeline;
import org.oisp.transformation.*;
import org.oisp.collection.Observation;

import java.util.List;


public class ObservationPipelineBuilder {

    static class KafkaToObservationFn extends DoFn<KafkaRecord<String, byte[]>, List<Observation>>{
        @ProcessElement
        public void processElement(ProcessContext c) {
            KafkaRecord<String, byte[]> record = c.element();
            Gson g = new Gson();
            List<Observation> obserations = g.fromJson(new String(record.getKV().getValue()), new TypeToken<List<Observation>>(){}.getType());
            c.output(obserations);
        }
    }

    static public Pipeline build(PipelineOptions options, Config conf){
        Pipeline p = Pipeline.create(options);
        KafkaSourceObservationsProcessor observationsKafka = new KafkaSourceObservationsProcessor(conf);
        p.apply(observationsKafka.getTransform())
                .apply(ParDo.of(new KafkaToObservationFn()))
        .apply(ParDo.of(new GetComponentRulesTask(conf)))
                .apply(ParDo.of(new PersistObservationTask(conf)))
                .apply(ParDo.of(new CheckObservationInRulesTask(conf)))
                .apply(ParDo.of(new PersistComponentAlertsTask(conf)))
                .apply(ParDo.of(new CheckRulesTask(conf)))
                .apply(ParDo.of(new SendAlertsTask(conf)));
                                                            
        return p;
    }
}
