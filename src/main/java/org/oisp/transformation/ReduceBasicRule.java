package org.oisp.transformation;

import org.apache.beam.sdk.transforms.Combine;
import org.oisp.collection.Rule;
import org.oisp.collection.RuleAndRuleCondition;
import org.oisp.collection.RuleCondition;
import org.oisp.rules.ConditionOperators;
import org.oisp.utils.LogHelper;
import org.slf4j.Logger;

public class ReduceBasicRule extends Combine.CombineFn<RuleAndRuleCondition, Rule, Rule> {
    @Override
    public Rule createAccumulator() {
        return new Rule();
    }

    @Override
    public Rule addInput(Rule accumulator, RuleAndRuleCondition input) {
        if (input == null) {
            return accumulator;
        }
        if (accumulator.getId() == null) {
            accumulator = new Rule(input.getRule());
        }
        accumulator.getConditions().set(input.getIndex(), input.getRc());
        return accumulator;
    }

    @Override
    public Rule mergeAccumulators(Iterable<Rule> accumulators) {
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

        return accumulator;
    }
}
