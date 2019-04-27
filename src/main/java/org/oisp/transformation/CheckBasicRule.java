package org.oisp.transformation;

import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;
import org.oisp.collection.*;
import org.oisp.rules.conditions.BasicConditionChecker;
import org.oisp.utils.LogHelper;
import org.oisp.collection.RuleWithRuleConditions;
import org.slf4j.Logger;

import java.util.List;
import java.util.TreeMap;

public class CheckBasicRule extends DoFn<List<RulesWithObservation>, KV<String, RuleWithRuleConditions>> {

    private List<RulesWithObservation> observationRulesList;
    private List<RuleCondition> fullFilledRuleConditions;
    private static final Logger LOG = LogHelper.getLogger(CheckBasicRule.class);
    @ProcessElement
    public void processElement(ProcessContext c) {
        observationRulesList = c.element();
        sendFulfillmentState(c);
    }

    void sendFulfillmentState(ProcessContext c) {
        for (RulesWithObservation rwo : observationRulesList) {
            for (Rule rule: rwo.getRules()) {
                Observation observation = rwo.getObservation();
                RuleWithRuleConditions mutableRWRC = new RuleWithRuleConditions(rule);
                for (int i=0; i < rule.getConditions().size(); i++) {
                    RuleCondition rc = rule.getConditions().get(i);
                    Boolean condFulfillment;
                    if (rc.isTimebased() || rc.isStatistics()) {
                        continue;
                    }
                    if (rc.getComponentId().equals(observation.getCid())) {
                        if (new BasicConditionChecker(rc).isConditionFulfilled(observation)) {
                            condFulfillment = true;
                        } else {
                            condFulfillment = false;
                        }
                        RuleCondition mutableRuleCondition = new RuleCondition(rc);
                        mutableRuleCondition.setFulfilled(condFulfillment);
                        mutableRuleCondition.setObservation(observation);
                        mutableRWRC.addRC(i, mutableRuleCondition);
                    }
                }
                c.output(KV.of(mutableRWRC.getRule().getId(), mutableRWRC));

            }
        }
    }
}
