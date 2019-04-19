package org.oisp.transformation;

import org.apache.beam.sdk.transforms.Combine;
import org.oisp.collection.Rule;
import org.oisp.rules.ConditionOperators;

import java.util.List;

import static org.oisp.collection.Rule.FulfillmentState.FALSE;
import static org.oisp.collection.Rule.FulfillmentState.TRUE;

public class MonitorRule extends Combine.CombineFn<Rule, Rule, Rule> {

    @Override
    public Rule createAccumulator() {
        return new Rule();
    }

    @Override
    public Rule addInput(Rule accum, Rule input) {
        // When accum does not have a rule ID, take over whole rule
        if (accum.getId() == null) {
            accum = input;
        }
        else { // merge fulfillment state
            List<Rule.FulfillmentState> accumList = accum.getCondFulfilment();
            List<Rule.FulfillmentState> inputList = input.getCondFulfilment();
            assert(accumList.size() == inputList.size());
            for(int i = 0; i < accumList.size(); i++) {
                if (inputList.get(i) != Rule.FulfillmentState.UNDECIDED) {
                    accumList.set(i, inputList.get(i));
                }
            }
        }
        return accum;
    }

    @Override
    public Rule mergeAccumulators(Iterable<Rule> accums) {
        Rule merged = null;
        for (Rule rule : accums) {
            if (rule.getId() == null ) { //why would that happen?
                continue;
            }
            List<Rule.FulfillmentState> fulfillmentStates = rule.getCondFulfilment();
            for (int i = 0; i < fulfillmentStates.size(); i++) {
                if (merged == null ) {
                    merged = rule;
                }
                else {
                    if (fulfillmentStates.get(i) == TRUE) {
                        merged.getCondFulfilment().set(i, TRUE);
                    } else if (fulfillmentStates.get(i) == FALSE) {
                        merged.getCondFulfilment().set(i, FALSE);
                    }
                }
            }
        }
        return merged;
    }

    @Override
    public Rule extractOutput(Rule accum) {
        boolean fulfilled = false;
        if (accum.getConditionOperator() == ConditionOperators.AND) {
            fulfilled = accum.getCondFulfilment().stream().allMatch( c -> c == TRUE);
        }
        else {
            fulfilled = accum.getCondFulfilment().stream().anyMatch( c -> c == TRUE);
        }
        if (fulfilled) {
                return accum;
        }
        else {
            return null;
        }
  }
}
