package org.oisp.transformation;

import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;
import org.oisp.collection.*;
import org.oisp.rules.conditions.BasicConditionChecker;
import org.oisp.utils.LogHelper;
import org.slf4j.Logger;

import java.util.HashMap;
import java.util.List;

public class CheckBasicRule extends DoFn<List<RulesWithObservation>, KV<String, RuleFulfilmentState>> {

    private List<RulesWithObservation> observationRulesList;
    private List<RuleCondition> fullFilledRuleConditions;
    private static final Logger LOG = LogHelper.getLogger(CheckBasicRule.class);
    @ProcessElement
    public void processElement(ProcessContext c) {
        //try {
            observationRulesList = c.element();
            sentFulfillmentState(c);

            //c.output(getActiveObservations());
        /*} catch (IOException e) {
            LOG.error("Error during processing of rules", e);*/
        //}
    }

    void sentFulfillmentState(ProcessContext c) {
        HashMap<String, RuleFulfilmentState> fulfilmentHash = new HashMap<>();
        for (RulesWithObservation rwo : observationRulesList) {
            for (Rule rule: rwo.getRules()) {
                fulfilmentHash.put(rule.getId(), new RuleFulfilmentState());
                Observation observation = rwo.getObservation();
                for (RuleCondition rc: rule.getConditions()) {
                    List<Integer> ffH = fulfilmentHash.get(rule.getId()).getCondFulfillment();
                    if (rc.getComponentId() == observation.getCid())
                        if (new BasicConditionChecker(rc).isConditionFulfilled(observation)){
                            ffH.add(1);
                        } else {
                            ffH.add(-1);
                        }
                    else {
                        ffH.add(0);
                    }
                }
                KV<String, RuleFulfilmentState> kvOutput = KV.of(rule.getId(), fulfilmentHash.get(rule.getId()));
                c.output(kvOutput);
            }
        }

    }
}
