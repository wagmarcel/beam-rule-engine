package org.oisp.transformation;

import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;
import org.oisp.collection.*;
import org.oisp.rules.conditions.BasicConditionChecker;
import org.oisp.utils.LogHelper;
import org.slf4j.Logger;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import static org.oisp.collection.Rule.FulfillmentState.FALSE;
import static org.oisp.collection.Rule.FulfillmentState.TRUE;
import static org.oisp.collection.Rule.FulfillmentState.UNDECIDED;

public class CheckBasicRule extends DoFn<List<RulesWithObservation>, KV<String, Rule>> {

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
                Rule mutableRule = new Rule(rule);
                Observation observation = rwo.getObservation();
                List<Rule.FulfillmentState> ffS = new ArrayList<>();
                for (RuleCondition rc: mutableRule.getConditions()) {
                    if (rc.getComponentId().equals(observation.getCid()))
                        if (new BasicConditionChecker(rc).isConditionFulfilled(observation)){
                            ffS.add(TRUE);
                        } else {
                            ffS.add(FALSE);
                        }
                    else {
                        ffS.add(UNDECIDED);
                    }
                }
                mutableRule.setCondFulfilment(ffS);
                KV<String, Rule> kvOutput = KV.of(rule.getId(), mutableRule);
                c.output(kvOutput);
            }
        }
    }
}
