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

public class PersistRuleState extends DoFn<KV<String,RuleAndRuleCondition>, KV<String, Rule>> {

    class RuleConditionHash implements Serializable {
        Map<Integer, RuleCondition> rarch;
        RuleConditionHash() {
            rarch = new HashMap<Integer, RuleCondition>();
        }
    }
    @StateId("ruleCondHash")
    private final StateSpec<ValueState<RuleConditionHash>> ruleCondHash = StateSpecs.value();

    @ProcessElement
    public void processElement(ProcessContext c,
                               @StateId("ruleCondHash") ValueState<RuleConditionHash> ruleCondHash) {

        //Record all ruleconditions per Rule
        RuleAndRuleCondition rarc = c.element().getValue();
        Rule rule = rarc.getRule();
        RuleConditionHash rch = ruleCondHash.read();
        if (rch == null) {
            ruleCondHash.write(new RuleConditionHash());
            rch = ruleCondHash.read();
        }
        rch.rarch.put(rarc.getIndex(), rarc.getRc());

        //send out all RuleConditions
        Rule mutableRule = new Rule(rule);
        for (Map.Entry<Integer, RuleCondition> entry: rch.rarch.entrySet()) {
            Integer index = entry.getKey();
            mutableRule.getConditions().set(index, entry.getValue());
        }
        c.output(KV.of(mutableRule.getId(), mutableRule));
    }
}
