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

package org.oisp.transformation;

import org.apache.beam.sdk.transforms.DoFn;
import org.oisp.data.RuleConditionsRepository;
import org.oisp.data.alerts.RuleConditionsHbaseRepository;
import org.oisp.rules.RulesChecker;
import org.oisp.collection.RulesWithObservation;
import org.oisp.collection.controllers.MessageReceiver;
import org.oisp.collection.Message;
import org.oisp.conf.Config;

import java.util.List;


public class CheckRulesTask extends DoFn<List<RulesWithObservation>, List<RulesWithObservation>> {

    private static final String TASK_NAME = "verifyAllRulesConditions";

    private final RuleConditionsRepository componentAlertsHbaseDao;

    public CheckRulesTask(Config userConf) {
        this(userConf, new RuleConditionsHbaseRepository(userConf));
    }

    public CheckRulesTask(Config userConf, RuleConditionsRepository componentAlertsHbaseDao) {
        this.componentAlertsHbaseDao = componentAlertsHbaseDao;
    }

    @ProcessElement
    public void processElement(ProcessContext c) {
        List<RulesWithObservation> rulesWithObservations = c.element();

        RulesChecker rulesChecker = new RulesChecker(rulesWithObservations, componentAlertsHbaseDao);
        c.output(rulesChecker.getCompletelyFulfilledRules());
    }

}
