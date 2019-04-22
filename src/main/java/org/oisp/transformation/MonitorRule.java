package org.oisp.transformation;

import org.apache.beam.sdk.transforms.Combine;
import org.oisp.collection.Rule;
import org.oisp.collection.RuleCondition;
import org.oisp.rules.ConditionOperators;

public class MonitorRule extends Combine.CombineFn<Rule, Rule, Rule> {
    @Override
    public Rule createAccumulator() {
        return new Rule();
    }

    @Override
    public Rule addInput(Rule accumulator, Rule input) {
        //merge the true states together
        if (accumulator.getId() == null) {
            accumulator = input;
        } else {
            for (int i = 0; i < accumulator.getConditions().size(); i++) {
                if (input.getConditions().get(i).getFulfilled()) {
                    accumulator.getConditions().get(i).setFulfilled(true);
                }
            }
        }
        return accumulator;
    }

    @Override
    public Rule mergeAccumulators(Iterable<Rule> accumulators) {
        // ?? can you write this ??
        Rule merged = createAccumulator();
        for (Rule accum : accumulators) {
            if (accum == null || accum.getId() == null) {
                continue;
            }
            if (merged.getId() == null) {
                merged = accum;
            } else {
                for (int i = 0; i < merged.getConditions().size(); i++) {
                    if (accum.getConditions().get(i).getFulfilled()) {
                        merged.getConditions().get(i).setFulfilled(true);
                    }
                }
            }
        }
        return merged;
    }

    @Override
    public Rule extractOutput(Rule accumulator) {
        if (accumulator != null) {
            if (accumulator.getConditionOperator() == ConditionOperators.AND) {
                Boolean result = true;
                for (RuleCondition rc: accumulator.getConditions()) {
                    result &= rc.getFulfilled();
                }
                if (result) {
                    return accumulator;
                }
            } else {
                Boolean result = false;
                for (RuleCondition rc: accumulator.getConditions()) {
                    result |= rc.getFulfilled();
                }
                if (result) {
                    return accumulator;
                }

            }
        }
        return null;
    }
}
