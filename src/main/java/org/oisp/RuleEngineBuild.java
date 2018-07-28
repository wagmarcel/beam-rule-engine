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
import org.apache.beam.runners.spark.SparkRunner;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.joda.time.Duration;
import org.oisp.conf.CmdlineOptions;
import org.oisp.conf.Config;
import org.oisp.conf.ExternalConfig;
import org.oisp.pipeline.ObservationPipelineBuilder;
import org.oisp.pipeline.HeartbeatPipelineBuilder;
import org.oisp.pipeline.RulesUpdatePiplineBuilder;
import org.oisp.transformation.*;
import org.oisp.collection.Rule;
import org.oisp.coder.RuleCoder;

import java.util.Map;
import java.util.List;
import java.io.File;
import org.apache.log4j.BasicConfigurator;

/**
 * RuleEngineBuild - creates different pipelines for Rule-engine Example
 */



public abstract class RuleEngineBuild {
    public static void main(String[] args) {

        PipelineOptions options = PipelineOptionsFactory
                .fromArgs(args)
                .withValidation()
                .as(CmdlineOptions.class);

        options.setRunner(SparkRunner.class);

        BasicConfigurator.configure();
        Pipeline heartbeat;
        Pipeline rulesUpdate;
        Pipeline observationPipeline;


        ExternalConfig ext_conf = ExternalConfig.getConfigFromString(((CmdlineOptions) options).getJSONConfig());
        String pipelineName = ((CmdlineOptions) options).getPipelineName();
        Config conf = ext_conf.getConfig();


        switch(pipelineName) {
            case "heartbeat":
                //TODO: make value and time interval configurable
                heartbeat = HeartbeatPipelineBuilder.build(options, conf);
                heartbeat.run().waitUntilFinish();
                break;
            case "rules-update":
                rulesUpdate = RulesUpdatePiplineBuilder.build(options, conf);
                rulesUpdate.run().waitUntilFinish();
                break;
            case "observations":
                observationPipeline = ObservationPipelineBuilder.build(options, conf);
                observationPipeline.run().waitUntilFinish();
                break;
            default:
                System.err.println("Error: No PipelineName specified!");
        }

    }
}
