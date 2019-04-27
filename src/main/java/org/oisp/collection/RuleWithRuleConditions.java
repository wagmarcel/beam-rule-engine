package org.oisp.collection;

import org.apache.beam.sdk.coders.DefaultCoder;
import org.oisp.coder.RuleCoder;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;

public class RuleWithRuleConditions implements Serializable {

    private final Rule rule;
    private final SortedMap<Integer, RuleCondition> rcHash;

    public RuleWithRuleConditions(){
        rule = null;
        rcHash = new TreeMap<Integer, RuleCondition>();
    }
    public RuleWithRuleConditions(Rule rule, RuleCondition rc, int index) {
        this.rule = rule;
        rcHash = new TreeMap<Integer, RuleCondition>();
        rcHash.put(index, rc);
    }

    public RuleWithRuleConditions(RuleWithRuleConditions other){
        rule = other.rule;
        rcHash = new TreeMap<Integer, RuleCondition>(other.rcHash);
    }

    public Map<Integer, RuleCondition> getRcHash() {
        return rcHash;
    }

    public Rule getRule() {

        return rule;
    }
}
