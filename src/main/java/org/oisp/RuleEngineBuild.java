package org.oisp;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.oisp.conf.CmdlineOptions;
import org.oisp.conf.Config;
import org.oisp.conf.ExternalConfig;
import org.oisp.pipeline.FullPipelineBuilder;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;


/**
 * RuleEngineBuild - creates different pipelines for Rule-engine Example
 */



public abstract class RuleEngineBuild {
    public static void main(String[] args) {

        PipelineOptions options = PipelineOptionsFactory
                .fromArgs(args)
                .withValidation()
                .as(CmdlineOptions.class);

        PipelineOptionsFactory.register(CmdlineOptions.class);
        //options.setRunner(FlinkRunner.class);
        //BasicConfigurator.configure();
        Pipeline heartbeat;
        Pipeline rulesUpdate;
        Pipeline observationPipeline;
        Pipeline fullPipeline;

        //read json config from file - needed because stupid mvn cannot read JSON from comdline. Unbelievable, but true.
        String confFromFile = "";
        try {
            confFromFile = new String(Files.readAllBytes(Paths.get(((CmdlineOptions) options).getJSONConfig())));
        }
        catch (IOException e) {
            System.out.println("Could not find config data: " + e);
            System.exit(1);
        }
        System.out.println("JSON config retrieved: " + confFromFile);
        ExternalConfig ext_conf = ExternalConfig.getConfigFromString(confFromFile);
        String pipelineName = ((CmdlineOptions) options).getPipelineName();
        Config conf;


        switch(pipelineName) {
            case "full":
                conf = ext_conf.getConfig();
                fullPipeline = FullPipelineBuilder.build(options, conf);
                fullPipeline.run().waitUntilFinish();
                break;
            default:
                System.out.println("Error: No PipelineName specified!");
                PipelineOptionsFactory.printHelp(System.out);
        }

    }
}
