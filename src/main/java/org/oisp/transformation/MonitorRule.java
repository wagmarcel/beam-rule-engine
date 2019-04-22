package org.oisp.transformation;

import org.apache.beam.sdk.transforms.Combine;
import org.oisp.collection.Rule;
import org.oisp.collection.RuleAndRuleCondition;
import org.oisp.collection.RuleCondition;
import org.oisp.rules.ConditionOperators;
import org.oisp.transformation.acumulators.MonitorRuleAccum;

import java.util.*;

import static org.oisp.collection.Rule.FulfillmentState.FALSE;
import static org.oisp.collection.Rule.FulfillmentState.TRUE;
import static org.oisp.collection.Rule.FulfillmentState.UNDECIDED;

public class MonitorRule extends Combine.CombineFn<RuleAndRuleCondition, MonitorRuleAccum, Rule> {


    @Override
    public MonitorRuleAccum createAccumulator() {
        MonitorRuleAccum accum = new MonitorRuleAccum();
        return accum;
    }

    @Override
    public MonitorRuleAccum addInput(MonitorRuleAccum  accum, RuleAndRuleCondition input) {
        // When accum does not have a rule ID, take over whole rule
        if (input == null || input.getRule() == null || input.getRc() == null) {
            return accum;
        }
        if (accum.getRule() == null) {
            accum.setRule(input.getRule());
        }
        else { // merge fulfillment state
            accum.getRuleconditions().put(input.getIndex(), input.getRc());
        }
        return accum;
    }

    @Override
    public MonitorRuleAccum mergeAccumulators(Iterable<MonitorRuleAccum> accums) {
        MonitorRuleAccum merged = createAccumulator();
        for (MonitorRuleAccum accum : accums) {
            if (accum == null || accum.getRule() == null) { //why would that happen?
                continue;
            }
            if (merged.getRule() == null) {
                merged = accum;
            }
            assert(accum.getRule().getId() == merged.getRule().getId());
            for(Map.Entry<Integer, RuleCondition> entry : accum.getRuleconditions().entrySet()) {
                Integer index = entry.getKey();
                RuleCondition rc = entry.getValue();
                if (merged.getRuleconditions().get(index) != null) {
                    if (merged.getRuleconditions().get(index).getFulfilled() == false) {
                        if (rc.getFulfilled() == true) {
                            merged.getRuleconditions().put(index, rc);
                        }
                    }
                } else {
                    merged.getRuleconditions().put(index, rc);
                }
            }
        }
        return merged;
    }

    @Override
    public Rule extractOutput(MonitorRuleAccum accum) {
        if (accum.getRule() != null) {
            if (accum.getRule().getConditionOperator() == ConditionOperators.AND) {
                if (accum.getRule().getConditions().size() == accum.getRuleconditions().size()) {
                    // AND operator is only checked when all ruleconditions are available
                    Boolean result = true;
                    for (RuleCondition rc : accum.getRuleconditions().values()) {
                        result &= rc.getFulfilled();
                    }
                    if (result) {
                        Rule mutableRule = new Rule(accum.getRule());
                        mutableRule.setConditions(new ArrayList<>(accum.getRuleconditions().values()));
                        return mutableRule;
                    }
                }
            } else {
                for (RuleCondition rc : accum.getRuleconditions().values()) {
                    if (rc.getFulfilled()) {
                        Rule mutableRule = new Rule(accum.getRule());
                        mutableRule.setConditions(new ArrayList<>(Arrays.asList(rc)));
                        return mutableRule;
                    }
                }
            }
        }
        return null;
    }
}
