package org.oisp.transformation;

import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;
import org.oisp.collection.*;
import org.oisp.rules.conditions.ConditionFunctionChecker;
import org.oisp.utils.LogHelper;
import org.slf4j.Logger;

import java.util.ArrayList;
import java.util.List;

public class CheckTimeBasedRule extends DoFn<List<RulesWithObservation>, KV<String, RuleWithRuleConditions>> {
    private List<RulesWithObservation> observationRulesList;
    private List<RuleCondition> fullFilledRuleConditions;
    private static final Logger LOG = LogHelper.getLogger(CheckBasicRule.class);
    @ProcessElement
    public void processElement(ProcessContext c) {
        observationRulesList = c.element();
        sendFulfillmentState(c);
    }

    void sendFulfillmentState(ProcessContext c) {
        List<Rule> mutableRuleList = new ArrayList<>();
        for (RulesWithObservation rwo : observationRulesList) {
            for (Rule rule: rwo.getRules()) {
                Observation observation = rwo.getObservation();
                for (int i = 0; i< rule.getConditions().size();i++) {
                    RuleCondition rc = rule.getConditions().get(i);
                    if (!rc.isTimebased()) {
                        continue;
                    }
                    if (rc.getComponentId().equals(observation.getCid())) {
                        boolean result = false;
                        if (new ConditionFunctionChecker(rc).isConditionFulfilled(observation.getValue())) {
                            result = true;
                        } else {
                            result = false;
                        }
                        RuleCondition mutableRuleCondition = new RuleCondition(rc);
                        mutableRuleCondition.setObservation(observation);
                        RuleWithRuleConditions rarc = new RuleWithRuleConditions(rule, mutableRuleCondition, i);
                        c.output(KV.of(rule.getId(), rarc));
                    }
                }
            }
        }
    }

}
