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

import org.oisp.data.RulesRepository;
import org.oisp.data.rules.RulesHbaseRepository;
import org.oisp.collection.Observation;
import org.oisp.collection.Rule;
import org.oisp.collection.RulesWithObservation;
import org.oisp.conf.Config;
import org.oisp.utils.LogHelper;


import org.slf4j.Logger;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;


public class GetComponentRulesTask extends DoFn<List<Observation>, List<RulesWithObservation>> {

    private List<Observation> observations;
    private static final Logger LOG = LogHelper.getLogger(PersistRulesTask.class);

    private RulesRepository rulesRepository;

    public GetComponentRulesTask(Config userConfig) {
        this(new RulesHbaseRepository(userConfig));
    }

    public GetComponentRulesTask(RulesRepository rulesRepository) {
        this.rulesRepository = rulesRepository;
    }

    @ProcessElement
    public void processElement(ProcessContext c) {
        try {
            observations = c.element();
            c.output(getActiveObservations());
        } catch (IOException e) {
            LOG.error("Error during searching rules in hbase - ", e);
        }
    }

    private List<RulesWithObservation> getActiveObservations() throws IOException {
        List<RulesWithObservation> rulesWithObservations = getRulesWithObservation(observations);

        List<RulesWithObservation> observationsWithActiveRules = rulesWithObservations.stream()
                .filter(r -> hasObservationRules(r))
                .collect(Collectors.toList());

        return observationsWithActiveRules;
    }

    private List<RulesWithObservation> getRulesWithObservation(List<Observation> observations) throws IOException {
        Set<String> componentsIds = observations.stream()
                .map(o -> o.getCid())
                .collect(Collectors.toSet());

        String accountId = observations.stream()
                .findFirst().get().getAid();

        Map<String, List<Rule>> componentsRules = rulesRepository.getComponentsRules(accountId, componentsIds);

        return observations.stream()
                .map(observation -> new RulesWithObservation(observation, componentsRules.get(observation.getCid())))
                .collect(Collectors.toList());
    }

    private boolean hasObservationRules(RulesWithObservation rulesWithObservation) {
        return rulesWithObservation.getRules() != null && rulesWithObservation.getRules().size() > 0;
    }

}
