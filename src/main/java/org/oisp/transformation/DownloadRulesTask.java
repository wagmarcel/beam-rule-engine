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

import com.google.gson.Gson;
import org.apache.beam.sdk.io.kafka.KafkaRecord;
import org.apache.beam.sdk.values.KV;
import org.oisp.apiclients.DashboardConfigProvider;
import org.oisp.apiclients.InvalidDashboardResponseException;
import org.oisp.apiclients.rules.DashboardRulesApi;
import org.oisp.apiclients.rules.RulesApi;
import org.oisp.apiclients.rules.model.ComponentRulesResponse;
import org.oisp.parsers.RuleParser;
import org.oisp.collection.Rule;
import org.oisp.utils.LogHelper;
import org.oisp.conf.Config;
import org.apache.beam.sdk.transforms.DoFn;

import java.io.Serializable;
import java.util.List;
import java.util.Map;
import org.slf4j.Logger;

@SuppressWarnings({"checkstyle:illegalcatch", "PMD.AvoidCatchingGenericException"})
public class DownloadRulesTask  extends DoFn<KV<String, String>, Map<String, List<Rule>>>{

    private static final String TASK_NAME = "downloadRules";
    private final RulesApi rulesApi;
    private Map<String, List<Rule>> componentsRules;
    private static final Logger LOG = LogHelper.getLogger(DownloadRulesTask.class);
    public DownloadRulesTask(Config userConfig) {
        this(userConfig, new DashboardRulesApi(new DashboardConfigProvider(userConfig)));
    }

    public DownloadRulesTask(Config userConfig, RulesApi rulesApi) {
        //super(context, userConfig);
        this.rulesApi = rulesApi;
    }

//    public void onStart(StartTime startTime) {
//        getLogger().debug("DownloadRulesTask starting...");
//        self().tell(new Message(START_MSG, now()), self());
//    }

    @ProcessElement
    public void processElement(ProcessContext c) {
        //getLogger().info("Synchronizing dashboard rules...");
        try {
            componentsRules = getComponentsRules();
          //  getLogger().debug("Components Rules: {}", new Gson().toJson(componentsRules));
          c.output(componentsRules);
        } catch (InvalidDashboardResponseException e) {
            //getLogger().error("Unable to get active rules", e);
        } catch (Exception e) {
            LOG.error("Unknown error during rules downloading.", e);
        }
    }

    private Map<String, List<Rule>> getComponentsRules() throws InvalidDashboardResponseException {
        List<ComponentRulesResponse> componentsRules = rulesApi.getActiveComponentsRules();
        RuleParser ruleParser = new RuleParser(componentsRules);
        Map<String, List<Rule>> result = ruleParser.getComponentRules();
        return result;
    }
//    private Message getOutputMessage() {
//        return new OutputMessageCreator<Map<String, List<Rule>>>().createOutputMessage(componentsRules);
//    }
//
//    public static Processor getProcessor(UserConfig config, int parallelProcessorNumber) {
//        return createProcessor(DownloadRulesTask.class, config, parallelProcessorNumber, TASK_NAME);
//    }

}
