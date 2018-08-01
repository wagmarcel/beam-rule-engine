package org.oisp.conf;

import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;

public interface CmdlineOptions extends PipelineOptions {
    @Description("JSON config for RuleEngine")
    @Default.String("")
    String getJSONConfig();
    void setJSONConfig(String value);

    @Description("Pipeline name:(full|heartbeat|rules-update|observations)")
    @Default.String("none")
    String getPipelineName();
    void setPipelineName(String value);
}
