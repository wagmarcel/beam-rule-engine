package org.oisp.transformation;

import org.apache.beam.sdk.state.StateSpec;
import org.apache.beam.sdk.state.StateSpecs;
import org.apache.beam.sdk.state.ValueState;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;
import org.oisp.collection.Rule;
import org.oisp.collection.RuleWithRuleConditions;

import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;

public class PersistTimeBasedRuleState extends DoFn<KV<String,RuleWithRuleConditions>, KV<String, Rule>> {
    @DoFn.StateId("condSamples") //contains the evaluated elements with timestamp
    // e.g. time 0=false, 1=true, 2=false,3=false, 4=true, 5=true, 6=true looks like
    // (0,false),(1,true),(2,false),(3,false),(4,true), (5, true), 6(true)
    private final StateSpec<ValueState<SortedMap<Long, Boolean>>> condSamples = StateSpecs.value();

    @ProcessElement
    public void processElement(ProcessContext c,
                               @StateId("condSamples") ValueState<SortedMap<Long, Boolean>> condSamples) {

        //Record all ruleconditions per Rule
        RuleWithRuleConditions rarc = c.element().getValue();
        Rule rule = rarc.getRule();
        SortedMap<Long, Boolean> state = condSamples.read();
        if (state == null) {
            condSamples.write(new TreeMap<Long, Boolean>());
            state = condSamples.read();
        }
        //Long timestamp = rarc.getRc().getObservation().getOn();
        //state.put(timestamp, rarc.getRc().getFulfilled());
        //cleanup(state, rarc.getRc().getTimeLimit());
    }
    private void cleanup(SortedMap<Long, Boolean> state, Long timeLimit) {
        //condSamples always start with unfulfilled, so remove all fulfilled
        //There is no way to determine the interval to early fulfilled
        for (Map.Entry<Long, Boolean> entry : state.entrySet()) {
            Boolean fulFillmentValue = entry.getValue();
            if (fulFillmentValue) {
                break;
            }
            Long ts = entry.getKey();
            state.remove(ts);
        }
        //Then look for the next unfulfilled and remove if smaller than timelimit
        if (state.size() == 0) {
            return;
        }
        Long firstTS = state.firstKey();
        Long nextTS = firstTS;
        while(true) {
            for (Map.Entry<Long, Boolean> entry : state.headMap(firstTS).entrySet()) {
                Boolean fulFillmentValue = entry.getValue();
                if (!fulFillmentValue) {
                    nextTS = entry.getKey();
                }
            }
            if (nextTS == firstTS) {
                break; //no next TS found, so no further cleaning possible
            }
            if (nextTS != firstTS && nextTS - firstTS < timeLimit) {
                //Max possible limit is smaller than timeLimit so discard the content
                //Except the nextTS which will become the firstTS
                Set<Long> toRemove = state.tailMap(nextTS - 1).keySet();
                state.keySet().removeAll(toRemove);
            }
        }
    }
}