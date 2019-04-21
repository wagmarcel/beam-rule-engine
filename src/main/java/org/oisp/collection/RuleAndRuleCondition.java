package org.oisp.collection;

import java.io.Serializable;

public class RuleAndRuleCondition implements Serializable {

    private final Rule rule;
    private final RuleCondition rc;
    private final int index;

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
