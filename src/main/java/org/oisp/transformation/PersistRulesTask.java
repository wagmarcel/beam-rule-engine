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
import org.apache.beam.sdk.values.PDone;
import org.oisp.apiclients.DashboardConfigProvider;
import org.oisp.apiclients.InvalidDashboardResponseException;
import org.oisp.apiclients.rules.DashboardRulesApi;
import org.oisp.apiclients.rules.RulesApi;
import org.oisp.data.RulesRepository;
import org.oisp.data.rules.RulesHbaseRepository;
import org.oisp.collection.Rule;
import org.oisp.collection.controllers.InputMessageParser;
import org.oisp.transformation.InvalidMessageTypeException;
import org.oisp.conf.Config;
import org.oisp.collection.Message;

import org.oisp.utils.LogHelper;
import org.slf4j.Logger;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;


public class PersistRulesTask extends DoFn<Map<String, List<Rule>>, byte[]> {

    private static final String TASK_NAME = "persistRules";
    private RulesRepository rulesRepository;
    private final RulesApi rulesApi;
    private static final Logger LOG = LogHelper.getLogger(PersistRulesTask.class);

    public PersistRulesTask(Config userConfig) {
        this(userConfig, new RulesHbaseRepository(userConfig), new DashboardRulesApi(new DashboardConfigProvider(userConfig)));
    }

    public PersistRulesTask(Config userConfig, RulesRepository rulesRepository, RulesApi rulesApi) {
        this.rulesRepository = rulesRepository;
        this.rulesApi = rulesApi;
        try {
            rulesRepository.createTable();
        }
        catch(IOException e){
            LOG.error("Error while creating HBase Tables " + e);
        }
    }


    @ProcessElement
    public void processElement(ProcessContext c) {
        LOG.info("Persisting dashboard rules...");
        try {
            Map<String, List<Rule>> rules = c.element();

            if (!isRulesEmpty(rules)) {
                rulesRepository.putRulesAndRemoveNotExistingOnes(rules);
                rulesApi.markRulesSynchronized(getRulesIds(rules.values()));
            }
        } catch (IOException e) {
            LOG.error("Persisting error", e);
        } catch (InvalidDashboardResponseException e) {
            LOG.error("Unable to mark persisted rules as synchronized in Dashboard", e);
        }
    }

    private Set<String> getRulesIds(Collection<List<Rule>> ruleCollection) {
        return ruleCollection.stream()
                .flatMap(rules -> rules.stream())
                .map(rule -> rule.getId())
                .collect(Collectors.toSet());
    }

    private Map<String, List<Rule>> getInputMessage(Message message) throws InvalidMessageTypeException {
        return new InputMessageParser<Map<String, List<Rule>>>().parseInputMapMessage(message);
    }

    private boolean isRulesEmpty(Map<String, List<Rule>> rules) {
        return rules == null || rules.isEmpty();
    }


}
