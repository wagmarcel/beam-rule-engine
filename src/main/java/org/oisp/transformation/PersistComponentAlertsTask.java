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
import org.oisp.collection.RulesWithObservation;
import org.oisp.collection.controllers.MessageReceiver;
import org.oisp.transformation.storage.RuleComponentsStorageManager;
import org.oisp.collection.Message;
import org.oisp.conf.Config;
import org.oisp.utils.LogHelper;
import org.slf4j.Logger;


import java.io.IOException;
import java.util.List;

public class PersistComponentAlertsTask extends DoFn<List<RulesWithObservation>, List<RulesWithObservation>> {

    private static final String TASK_NAME = "persistComponentAlerts";
    private static final Logger LOG = LogHelper.getLogger(PersistRulesTask.class);

    private RuleConditionsRepository ruleConditionsRepository;

    public PersistComponentAlertsTask(Config userConf) {
        this(userConf, new RuleConditionsHbaseRepository(userConf));
    }

    public PersistComponentAlertsTask(Config userConf, RuleConditionsRepository ruleConditionsRepository) {
        this.ruleConditionsRepository = ruleConditionsRepository;
        try {
            ruleConditionsRepository.createTable();
        } catch (IOException ex) {
            LOG.warn("Initialization of hbase failed");
        }
    }


    @ProcessElement
    public void processElement(ProcessContext c) {
        try {
            List<RulesWithObservation> checkedRulesWithObservation = c.element();

            RuleComponentsStorageManager ruleComponentsStorageManager = new RuleComponentsStorageManager(ruleConditionsRepository, checkedRulesWithObservation);
            ruleComponentsStorageManager.persistBasicAndStatisticsRuleComponents();

            LOG.info("Sending message to check rullesss");
            c.output(checkedRulesWithObservation);
        } catch (IOException e) {
            LOG.error("Error during persisting rules in hbase.", e);
        }
    }

}
