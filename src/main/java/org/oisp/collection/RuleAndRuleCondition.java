package org.oisp.collection;

import org.apache.beam.sdk.coders.DefaultCoder;
import org.oisp.coder.RuleCoder;

import java.io.Serializable;

public class RuleAndRuleCondition implements Serializable {

    private final Rule rule;
    private final RuleCondition rc;
    private final int index;

    public RuleAndRuleCondition(){
        rule = null;
        rc = null;
        index = 0;
    }
    public RuleAndRuleCondition(Rule rule, RuleCondition rc, int index) {
        this.rule = rule;
        this.rc = rc;
        this.index = index;
    }

    public int getIndex() {
        return index;
    }

    public Rule getRule() {

        return rule;
    }

    public RuleCondition getRc() {
        return rc;
    }
}
