package org.oisp.transformation;

import org.apache.beam.sdk.transforms.Combine;
import org.oisp.collection.Rule;
import org.oisp.collection.RuleAndRuleCondition;
import org.oisp.collection.RuleCondition;
import org.oisp.rules.ConditionOperators;

import java.util.*;

import static org.oisp.collection.Rule.FulfillmentState.FALSE;
import static org.oisp.collection.Rule.FulfillmentState.TRUE;
import static org.oisp.collection.Rule.FulfillmentState.UNDECIDED;

public class MonitorRule extends Combine.CombineFn<RuleAndRuleCondition, MonitorRule.Accum, Rule> {

    class Accum {
        Rule rule;
        Map<Integer, RuleCondition> ruleconditions;
        Rule.FulfillmentState fulfillmentState;
    }
    @Override
    public Accum createAccumulator() {
        Accum accum = new Accum();
        accum.fulfillmentState = UNDECIDED;
        accum.rule = null;
        accum.ruleconditions = new HashMap<Integer, RuleCondition>();
        return accum;
    }

    @Override
    public Accum addInput(Accum  accum, RuleAndRuleCondition input) {
        // When accum does not have a rule ID, take over whole rule
        if (input == null || input.getRule() == null || input.getRc() == null) {
            return accum;
        }
        if (accum.rule == null) {
            accum.rule = input.getRule();
        }
        else { // merge fulfillment state
            accum.ruleconditions.put(input.getIndex(), input.getRc());
        }
        return accum;
    }

    @Override
    public Accum mergeAccumulators(Iterable<Accum> accums) {
        Accum merged = createAccumulator();
        for (Accum accum : accums) {
            if (accum == null || accum.rule == null) { //why would that happen?
                continue;
            }
            if (merged.rule == null) {
                merged = accum;
            }
            assert(accum.rule.getId() == merged.rule.getId());
            for(Map.Entry<Integer, RuleCondition> entry : accum.ruleconditions.entrySet()) {
                Integer index = entry.getKey();
                RuleCondition rc = entry.getValue();
                if (merged.ruleconditions.get(index) != null) {
                    if (merged.ruleconditions.get(index).getFulfilled() == false) {
                        if (rc.getFulfilled() == true) {
                            merged.ruleconditions.put(index, rc);
                        }
                    }
                } else {
                    merged.ruleconditions.put(index, rc);
                }
            }
        }
        return merged;
    }

    @Override
    public Rule extractOutput(Accum accum) {
        if (accum.rule != null) {
            if (accum.rule.getConditionOperator() == ConditionOperators.AND) {
                if (accum.rule.getConditions().size() == accum.ruleconditions.size()) {
                    // AND operator is only checked when all ruleconditions are available
                    Boolean result = true;
                    for (RuleCondition rc : accum.ruleconditions.values()) {
                        result &= rc.getFulfilled();
                    }
                    if (result) {
                        Rule mutableRule = new Rule(accum.rule);
                        mutableRule.setConditions(new ArrayList<>(accum.ruleconditions.values()));
                        return mutableRule;
                    }
                }
            } else {
                for (RuleCondition rc : accum.ruleconditions.values()) {
                    if (rc.getFulfilled()) {
                        Rule mutableRule = new Rule(accum.rule);
                        mutableRule.setConditions(new ArrayList<>(Arrays.asList(rc)));
                        return mutableRule;
                    }
                }
            }
        }
        return null;
    }
}
