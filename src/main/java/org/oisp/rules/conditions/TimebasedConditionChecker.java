/*
 * Copyright (c) 2016 Intel Corporation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package org.oisp.rules.conditions;

import org.oisp.data.RuleConditionsRepository;
import org.oisp.data.alerts.ComponentObservation;
import org.oisp.data.alerts.ScanProperties;
import org.oisp.collection.Observation;
import org.oisp.collection.RuleCondition;
//import org.oisp.util.LogHelper;
import org.slf4j.Logger;

import java.io.IOException;

public class TimebasedConditionChecker extends BaseConditionChecker implements ConditionChecker {

    private final RuleConditionsRepository ruleConditionsRepository;
    private Observation observation;
    private ConditionFunctionChecker conditionFunctionChecker;

    //private static final Logger logger = LogHelper.getLogger(TimebasedConditionChecker.class);

    public TimebasedConditionChecker(RuleCondition ruleCondition, RuleConditionsRepository ruleConditionsRepository) {
        super(ruleCondition);
        this.ruleConditionsRepository = ruleConditionsRepository;
        this.conditionFunctionChecker = new ConditionFunctionChecker(ruleCondition);
    }

    @Override
    public boolean isConditionFulfilled(Observation observation) {
        this.observation = observation;
        try {
            return isConditionFulfilledForObservation() && isConditionFulfilledInTimeWindow();
        } catch (IOException e) {
            //logger.error("Unable to verify timebased condition for componentId - {}", observation.getCid(), e);
            return false;
        }
    }

    private boolean isConditionFulfilledInTimeWindow() throws IOException {
        //logger.debug("Time window length - {}, Time window start point - {}", getTimeWindowLength(), getTimeWindowStart());
        ComponentObservation componentObservation = findFirstFulfilledObservationOutsideTimeWidow();

        if (componentObservation != null) {
            return findNotFulfilledObservationInsideTimeWindow(componentObservation) == null;
        }
        return false;
    }

    private ComponentObservation findFirstFulfilledObservationOutsideTimeWidow() throws IOException {
        ScanProperties scanProperties = createScanProperties()
                .withStart(fromBeginning())
                .withStop(getTimeWindowStart());

        return ruleConditionsRepository.getLastTimebasedComponentObservation(scanProperties, true);
    }

    private ComponentObservation findNotFulfilledObservationInsideTimeWindow(ComponentObservation componentObservation) throws IOException {
        ScanProperties scanProperties = createScanProperties()
                .withStart(componentObservation.getTimestamp())
                .withStop(getTimeWindowEnd());
        return ruleConditionsRepository.getLastTimebasedComponentObservation(scanProperties, false);
    }

    private long getTimeWindowStart() {
        return observation.getOn() - getTimeWindowLength();
    }

    private long getTimeWindowEnd() {
        return observation.getOn();
    }

    private boolean isConditionFulfilledForObservation() {
        return conditionFunctionChecker.isConditionFulfilled(observation.getValue());
    }

    private ScanProperties createScanProperties() {
        return new ScanProperties()
                .withComponentId(observation.getCid())
                .withRuleId(getRuleCondition().getRuleId());
    }

    private long fromBeginning() {
        return 0;
    }
}
