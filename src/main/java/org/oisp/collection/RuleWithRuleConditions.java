package org.oisp.collection;

import org.apache.beam.sdk.coders.DefaultCoder;
import org.oisp.coder.RuleCoder;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

public class RuleWithRuleConditions implements Serializable {

    private final Rule rule;
    private final Map<Integer, RuleCondition> rcHash;

    public RuleWithRuleConditions(){
        rule = null;
        rcHash = new HashMap<Integer, RuleCondition>();
    }
    public RuleWithRuleConditions(Rule rule, RuleCondition rc, int index) {
        this.rule = rule;
        rcHash = new HashMap<Integer, RuleCondition>();
        rcHash.put(index, rc);
    }

    public RuleWithRuleConditions(RuleWithRuleConditions other){
        rule = other.rule;
        rcHash = new HashMap<Integer, RuleCondition>(other.rcHash);
    }

    public Map<Integer, RuleCondition> getRcHash() {
        return rcHash;
    }

    public Rule getRule() {

        return rule;
    }
}
