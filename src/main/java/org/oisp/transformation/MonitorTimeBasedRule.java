package org.oisp.transformation;

import org.apache.beam.sdk.transforms.Combine;
import org.oisp.collection.Observation;
import org.oisp.collection.Rule;
import org.oisp.collection.RuleAndRuleCondition;
import org.oisp.collection.RuleCondition;
import org.oisp.rules.ConditionOperators;

import java.util.HashMap;
import java.util.List;
import java.util.SortedMap;
import java.util.TreeMap;

import static org.oisp.collection.Rule.FulfillmentState.FALSE;
import static org.oisp.collection.Rule.FulfillmentState.TRUE;

public class MonitorTimeBasedRule extends Combine.CombineFn<RuleAndRuleCondition, MonitorTimeBasedRule.Accum, RuleAndRuleCondition> {


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
    public Accum addInput(Accum accum, RuleAndRuleCondition input) {
        // When accum does not have a rule ID, take over whole rule

        return accum;
    }

    @Override
    public Accum mergeAccumulators(Iterable<Accum> accums) {
        Accum merged = null;

        return merged;
    }

    @Override
    public RuleAndRuleCondition extractOutput(Accum accum) {
        return null;
    }
}
