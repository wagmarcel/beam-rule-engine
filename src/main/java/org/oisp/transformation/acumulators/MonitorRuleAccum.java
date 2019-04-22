package org.oisp.transformation.acumulators;

import org.oisp.collection.Rule;
import org.oisp.collection.RuleCondition;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

public class MonitorRuleAccum implements Serializable {
    private Rule rule;
    private Map<Integer, RuleCondition> ruleconditions;

    public MonitorRuleAccum() {
        rule = null;
        ruleconditions = new HashMap<>();
    }

    public Rule getRule() {
        return rule;
    }

    public void setRule(Rule rule) {
        this.rule = rule;
    }

    public Map<Integer, RuleCondition> getRuleconditions() {
        return ruleconditions;
    }

    public void setRuleconditions(Map<Integer, RuleCondition> ruleconditions) {
        this.ruleconditions = ruleconditions;
    }
}
