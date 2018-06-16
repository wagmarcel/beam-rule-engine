package org.oisp.coder;

import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CoderException;
import org.oisp.collection.Rule;

import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Collections;
import java.util.List;

public class RuleCoder extends Coder<Rule> {
    public static RuleCoder of(){
        return  INSTANCE;
    }

    RuleCoder(){
    }

    private static final RuleCoder INSTANCE = new RuleCoder();

    public List<RuleCoder> getCoderArguments(){
        return Collections.emptyList();
    }

    @Override
    public void encode(Rule rule, OutputStream outStream) throws IOException, CoderException {
        new DataOutputStream(outStream).writeChars("Hello World");
    }
    @Override
    public Rule decode(InputStream inStream){
        return new Rule();
    }

    @Override
    public void verifyDeterministic() {
    }
}


