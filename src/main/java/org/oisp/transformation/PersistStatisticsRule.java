package org.oisp.transformation;

import org.apache.beam.sdk.coders.MapCoder;
import org.apache.beam.sdk.coders.SerializableCoder;
import org.apache.beam.sdk.coders.VarIntCoder;
import org.apache.beam.sdk.state.StateSpec;
import org.apache.beam.sdk.state.StateSpecs;
import org.apache.beam.sdk.state.ValueState;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;
import org.oisp.collection.Observation;
import org.oisp.collection.Rule;
import org.oisp.collection.RuleCondition;
import org.oisp.collection.RuleWithRuleConditions;
import org.oisp.collection.subCollections.NormalizedStatisticsValues;
import org.oisp.rules.StatisticsRuleConditionChecker;
import org.oisp.rules.conditions.StatisticsConditionChecker;

import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;

public class PersistStatisticsRule extends DoFn<KV<String,RuleWithRuleConditions>, KV<String, RuleWithRuleConditions>> {

    @DoFn.StateId("ruleCondHash") //contains the RC with statistic state (i.e. StatisticValue)
    private final StateSpec<ValueState<Map<Integer, RuleCondition>>> state =
            StateSpecs.value(MapCoder.<Integer, RuleCondition>of(VarIntCoder.of(), SerializableCoder.of(RuleCondition.class)));

    @ProcessElement
    public void processElement(ProcessContext c,
                               @StateId("ruleCondHash") ValueState<Map<Integer, RuleCondition>> condSamples) {
        //Record all ruleconditions per Rule
        RuleWithRuleConditions rwRC = c.element().getValue();
        Rule rule = rwRC.getRule();
        Map<Integer, RuleCondition> state = condSamples.read();
        if (state == null) {
            condSamples.write(new TreeMap<Integer, RuleCondition>());
            state = condSamples.read();
        }
        SortedMap<Integer, RuleCondition> rch = rwRC.getRcHash();
        if (rch == null) {
            return;
        }
        for (SortedMap.Entry<Integer, RuleCondition> entry : rch.entrySet()) {
            //get state RC lists and merge it
            if (state.get(entry.getKey()) == null) {
                state.put(entry.getKey(), entry.getValue().clone());
            } else { //first check fulfillment of most recent element
                Boolean result = checkFulfillment(state.get(entry.getKey()), entry.getValue().getObservation());
                state.get(entry.getKey()).setFulfilled(result);
                if (result) {
                    //add Observation which triggered rule
                    state.get(entry.getKey()).setObservation(entry.getValue().getObservation());
                }
                //merge in new observation(s)
                merge(state.get(entry.getKey()), entry.getValue());
                //remove older samples
                cleanup(state.get(entry.getKey()));
                state.put(entry.getKey(), state.get(entry.getKey()));
            }
        }
        condSamples.write(state);
        RuleWithRuleConditions mutableRWRC = new RuleWithRuleConditions(rule);
        SortedMap<Integer, RuleCondition> mutableSM = new TreeMap<Integer, RuleCondition>(state);
        mutableRWRC.setRcHash(mutableSM);
        c.output(KV.of(rule.getId(), mutableRWRC));
    }

    //Merge two StatisticsValues
    private void merge(RuleCondition mutableRc, RuleCondition otherRC) {

        if (otherRC.getStatisticsValues() == null) {
            return;
        }
        if (otherRC.getStatisticsValues() == null) {
            mutableRc.setStatisticsValues(new NormalizedStatisticsValues());
            return;
        }
        // merge normalizedValues
        mutableRc.getStatisticsValues().add(otherRC.getStatisticsValues());
    }

    //check whether statistics rule is triggering
    private Boolean checkFulfillment(RuleCondition rc, Observation obs) {
        NormalizedStatisticsValues nSV = rc.getStatisticsValues();
        //TODO: where to find the stddev value?
        Boolean result = new StatisticsRuleConditionChecker(rc, nSV).isConditionFulfilled(obs);
        return result;
    }

    //cleanup: remove older samples no longer relevant for time window
    private void cleanup(RuleCondition rc) {
        NormalizedStatisticsValues nSV = rc.getStatisticsValues();
        Long recentTS = nSV.getTimeWindowEnd();
        Long oldestTS = nSV.getTimeWindowStart();
        if (recentTS == -1 || oldestTS == -1) {
            return;
        }
        Long windowLength = rc.getTimeLimit() * 1000;
        while(recentTS - oldestTS > windowLength) {
            nSV.remove(oldestTS);
            oldestTS = nSV.getTimeWindowStart();
            if (oldestTS == -1) {
                return; //should never happen?
            }
        }

    }
}