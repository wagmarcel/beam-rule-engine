package org.oisp.pipeline;

import org.apache.beam.sdk.state.StateSpec;
import org.apache.beam.sdk.state.StateSpecs;
import org.apache.beam.sdk.state.ValueState;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;

import java.util.HashMap;
import java.util.Map;

public class CountKafkaMessages extends DoFn<KV<String, Long>, Map<String, Long>> {
    @DoFn.StateId("counter")
    private final StateSpec<ValueState<Long>> stateSpec = StateSpecs.value();

    @ProcessElement
    public void processElement(ProcessContext c,
        @DoFn.StateId("counter") ValueState<Long> state) {

        Long counter = state.read();
        if (counter == null) {
            counter = Long.valueOf(0);
        }
        counter++;
        state.write(counter);
        Map<String, Long> map = new HashMap<String, Long>();
        map.put("ver", counter);
        c.output(map);
    }

}
