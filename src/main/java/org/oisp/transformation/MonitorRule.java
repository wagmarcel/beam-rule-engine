package org.oisp.transformation;

import org.apache.beam.sdk.transforms.Combine;
import org.oisp.collection.Rule;
import org.oisp.collection.RuleFulfilmentState;

public class MonitorRule extends Combine.CombineFn<RuleFulfilmentState, RuleFulfilmentState, String> {

    @Override
    public RuleFulfilmentState createAccumulator() {
        return new RuleFulfilmentState();
    }

    @Override
    public RuleFulfilmentState addInput(RuleFulfilmentState accum, RuleFulfilmentState input) {
        return accum;
    }

    @Override
    public RuleFulfilmentState mergeAccumulators(Iterable<RuleFulfilmentState> accums) {
        RuleFulfilmentState merged = createAccumulator();

        return merged;
    }

    @Override
    public String extractOutput(RuleFulfilmentState accum) {
        return "hello";
  }
}
