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
import org.oisp.data.StatisticsRepository;
import org.oisp.data.alerts.RuleConditionsHbaseRepository;
import org.oisp.data.statistics.StatisticsHbaseRepository;
import org.oisp.rules.RulesObservationChecker;
import org.oisp.collection.RulesWithObservation;
import org.oisp.collection.controllers.MessageReceiver;
import org.oisp.collection.Message;
import org.oisp.conf.Config;

import java.util.List;
import java.util.stream.Collectors;


public class CheckObservationInRulesTask extends DoFn<List<RulesWithObservation>, List<RulesWithObservation>> {

    private static final String TASK_NAME = "verifySingleRulesCondition";

    private final RuleConditionsRepository ruleConditionsRepository;
    private final StatisticsRepository statisticsRepository;

    public CheckObservationInRulesTask(Config userConf) {
        this(userConf, new RuleConditionsHbaseRepository(userConf), new StatisticsHbaseRepository(userConf));
    }

    public CheckObservationInRulesTask(Config userConf,
                                       RuleConditionsRepository ruleConditionsRepository,
                                       StatisticsRepository statisticsRepository) {
        this.ruleConditionsRepository = ruleConditionsRepository;
        this.statisticsRepository = statisticsRepository;
    }

    @ProcessElement
    public void processElement(ProcessContext c) {

        List<RulesWithObservation> rulesWithObservation = c.element();
        List<RulesWithObservation> checkedRulesWithObservation =
                rulesWithObservation.stream()
                        .map(ruleWithObservation -> new RulesObservationChecker(ruleWithObservation, ruleConditionsRepository, statisticsRepository)
                                .checkRulesForObservation())
                        .collect(Collectors.toList());

        c.output(checkedRulesWithObservation);

    }

}
