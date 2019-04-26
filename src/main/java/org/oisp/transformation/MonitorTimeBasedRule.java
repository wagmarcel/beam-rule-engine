package org.oisp.transformation;

import org.apache.beam.sdk.transforms.Combine;
import org.oisp.collection.Rule;
import org.oisp.collection.RuleWithRuleConditions;

import java.util.HashMap;
import java.util.SortedMap;

public class MonitorTimeBasedRule extends Combine.CombineFn<RuleWithRuleConditions, MonitorTimeBasedRule.Accum, RuleWithRuleConditions> {


    class Accum {
        HashMap<String, SortedMap<Long, Boolean>> rulesWithTimestamps;
        SortedMap<Long, Boolean> obsTimestampsWithFulfillment;
        Rule rule;
    }
    @Override
    public Accum createAccumulator() {
        return new Accum();
    }

    @Override
    public Accum addInput(Accum accum, RuleWithRuleConditions input) {
        // When accum does not have a rule ID, take over whole rule

        return accum;
    }

    @Override
    public Accum mergeAccumulators(Iterable<Accum> accums) {
        Accum merged = null;

        return merged;
    }

    @Override
    public RuleWithRuleConditions extractOutput(Accum accum) {
        return null;
    }
}
