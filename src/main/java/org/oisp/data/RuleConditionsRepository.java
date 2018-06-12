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

package org.oisp.data;

import org.oisp.data.alerts.ComponentObservation;
import org.oisp.data.alerts.ScanProperties;
import org.oisp.collection.RulesWithObservation;

import java.io.IOException;
import java.util.List;


public interface RuleConditionsRepository {

    List<ComponentObservation> getFulfilledConditionsForComponent(ScanProperties scanProperties) throws IOException;

    void createTable() throws IOException;

    void putFulfilledConditionsForObservation(List<RulesWithObservation> rulesWithObservation) throws IOException;

    void putTimebasedRuleComponents(List<RulesWithObservation> rulesWithObservation, boolean isFulfilled) throws IOException;

    ComponentObservation getLastTimebasedComponentObservation(ScanProperties scanProperties, boolean isFulfilled) throws IOException;

}
