package org.oisp.transformation;

import com.fasterxml.jackson.databind.introspect.TypeResolutionContext;
import org.apache.beam.sdk.transforms.DoFn;
import org.oisp.collection.Observation;
import org.oisp.collection.RuleCondition;
import org.oisp.collection.RulesWithObservation;
import org.oisp.rules.conditions.BasicConditionChecker;
import org.oisp.utils.LogHelper;
import org.slf4j.Logger;

import java.io.IOException;
import java.util.List;

import static java.util.stream.Collectors.toList;

public class CheckBasicRule extends DoFn<List<RulesWithObservation>, List<RulesWithObservation>> {

    private List<RulesWithObservation> observationRulesList;
    private List<RuleCondition> fullFilledRuleConditions;
    private static final Logger LOG = LogHelper.getLogger(CheckBasicRule.class);
    @ProcessElement
    public void processElement(ProcessContext c) {
        //try {
            observationRulesList = c.element();
            getFulfilledBasicRulesConditions();

            //c.output(getActiveObservations());
        /*} catch (IOException e) {
            LOG.error("Error during processing of rules", e);*/
        //}
    }

    void getFulfilledBasicRulesConditions() {
        for (RulesWithObservation rwo : observationRulesList) {
            Observation observation = rwo.getObservation();
            List<RuleCondition> ruleCondition = rwo.getRules().stream()
                    .flatMap(ru -> ru.getConditions().stream())
                    .filter(rc -> {
                        if (new BasicConditionChecker(rc).isConditionFulfilled(observation)){
                            rc.setFulfilled(true);
                        } else {
                            rc.setFulfilled(false);
                        };
                        return true;
                    })
                    .collect(toList());
        }
    }
}
