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
import org.oisp.collection.Observation;
import org.oisp.collection.RulesWithObservation;
import org.oisp.collection.controllers.MessageReceiver;
import org.oisp.transformation.storage.RuleComponentsStorageManager;
import org.oisp.collection.Message;
import org.oisp.conf.Config;
import org.oisp.utils.LogHelper;
import org.slf4j.Logger;

import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;

public class PersistObservationTask extends DoFn<List<RulesWithObservation>, List<RulesWithObservation>> {

    private static final String TASK_NAME = "persistObservation";

    private final StatisticsRepository statisticsRepository;
    private final RuleConditionsRepository ruleConditionsRepository;
    private List<RulesWithObservation> rulesWithObservation;
    private static final Logger LOG = LogHelper.getLogger(PersistRulesTask.class);

    public PersistObservationTask(Config userConf) {
        this(userConf, new StatisticsHbaseRepository(userConf), new RuleConditionsHbaseRepository(userConf));
    }

    public PersistObservationTask(Config userConf, StatisticsRepository statisticsRepository, RuleConditionsRepository ruleConditionsRepository) {
        this.statisticsRepository = statisticsRepository;
        this.ruleConditionsRepository = ruleConditionsRepository;
        try {
            statisticsRepository.createTable();
        } catch (IOException e) {
            LOG.info("Unable to create statisticsRepository table", e);
        }
    }


    @ProcessElement
    public void processElement(ProcessContext c) {
        try {
            rulesWithObservation = c.element();

            //Replicate obsTimestampsWithFulfillment data in gearpump's hbase in order for executing statistics aggregations
            List<Observation> observationsForStatisticsRules = filterObservationWithStatisticsRules();
            if (!observationsForStatisticsRules.isEmpty()) {
                statisticsRepository.putObservationForStatisticsRuleCondition(observationsForStatisticsRules);
            }

            //Check if obsTimestampsWithFulfillment data met logical conditions specified in timebased rules (eg. Equal 10, Greater than 5)
            RuleComponentsStorageManager ruleComponentsStorageManager = new RuleComponentsStorageManager(ruleConditionsRepository, rulesWithObservation);
            ruleComponentsStorageManager.persistTimebasedRuleComponents();

            c.output(rulesWithObservation);
        } catch (IOException e) {
            LOG.error("Unable to persistBasicAndStatisticsRuleComponents observation in hbase", e);
        }
    }

    private List<Observation> filterObservationWithStatisticsRules() {
        return rulesWithObservation.stream()
                .filter(r -> hasStatisticConditionsForObservation(r))
                .map(r -> r.getObservation())
                .collect(Collectors.toList());
    }

    private boolean hasStatisticConditionsForObservation(RulesWithObservation rulesWithObservation) {
        return rulesWithObservation.getRules().
                stream().anyMatch(r -> r.hasStatisticsCondition());
    }

}
