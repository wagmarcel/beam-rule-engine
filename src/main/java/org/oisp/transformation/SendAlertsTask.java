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
import org.oisp.apiclients.DashboardConfigProvider;
import org.oisp.apiclients.alerts.AlertsApi;
import org.oisp.apiclients.alerts.DashboardAlertsApi;
import org.oisp.collection.RulesWithObservation;
import org.oisp.collection.controllers.MessageReceiver;
import org.oisp.conf.Config;
import org.oisp.utils.LogHelper;
import org.slf4j.Logger;


import java.util.List;
import java.util.stream.Collectors;

@SuppressWarnings({"checkstyle:illegalcatch", "PMD.AvoidCatchingGenericException"})
public class SendAlertsTask extends DoFn<List<RulesWithObservation>, byte[]> {

    private static final String TASK_NAME = "sendAlerts";

    private List<RulesWithObservation> fulfilledRules;
    private static final Logger LOG = LogHelper.getLogger(PersistRulesTask.class);

    private final AlertsApi alertsApi;

    public SendAlertsTask(Config userConfig) {
        this(userConfig, new DashboardAlertsApi(new DashboardConfigProvider(userConfig)));
    }

    public SendAlertsTask(Config userConfig, AlertsApi alertsApi) {
        this.alertsApi = alertsApi;
    }

    @ProcessElement
    public void processElement(ProcessContext c) {

            fulfilledRules = c.element()
                    .stream().filter(r -> hasFulfilledRules(r))
                    .collect(Collectors.toList());

            sendAlerts();

        //getContext().output(new Message(message, now()));
    }

    private void sendAlerts() {
        try {
            if (fulfilledRules != null && !fulfilledRules.isEmpty()) {
                alertsApi.pushAlert(fulfilledRules);
            }
        } catch (Exception e) {
            LOG.error("Unable to send alerts for fulfilled rules", e);
        }
    }

    public static boolean hasFulfilledRules(RulesWithObservation rulesWithObservation) {
        return rulesWithObservation != null && rulesWithObservation.getRules() != null && rulesWithObservation.getRules().size() > 0;
    }
}
