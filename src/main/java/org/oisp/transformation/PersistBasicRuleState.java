package org.oisp.transformation;

import org.apache.beam.sdk.state.MapState;
import org.apache.beam.sdk.state.StateSpec;
import org.apache.beam.sdk.state.StateSpecs;
import org.apache.beam.sdk.state.ValueState;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;
import org.oisp.collection.Rule;
import org.oisp.collection.RuleAndRuleCondition;
import org.oisp.collection.RuleCondition;
import org.oisp.rules.ConditionOperators;

import java.io.Serializable;
import java.util.*;

public class PersistBasicRuleState extends DoFn<KV<String,Rule>, KV<String, Rule>> {

    @StateId("ruleState")
    private final StateSpec<ValueState<Rule>> ruleState = StateSpecs.value();

    @ProcessElement
    public void processElement(ProcessContext c,
                               @StateId("ruleState") ValueState<Rule> ruleState) {

        //Record all ruleconditions per Rule
        Rule rule = c.element().getValue();
        if (rule == null || rule.getId() == null) {
            return;
        }
        Rule rst = ruleState.read();
        if (rst == null) {
            ruleState.write(new Rule());
            rst = ruleState.read();
        }

        //send out all RuleConditions
        for (int i = 0; i < rule.getConditions().size(); i++) {
            RuleCondition rc = rule.getConditions().get(i);

            if (rc.getFulfilled()) {
                rst.getConditions().set(i, rc);
            }
        }
        ruleState.write(rst);

        Rule mutableRule = new Rule(rst);
        c.output(KV.of(mutableRule.getId(), mutableRule));
    }
}
