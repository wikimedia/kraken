package org.wikimedia.analytics.kraken.pig.bags;

import datafu.pig.util.SimpleEvalFunc;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.python.google.common.collect.Iterators;

import java.io.IOException;

/**
 * Returns the first tuple from a bag. Optional second parameter will be returned if the bag is empty, otherwise null.
 *
 * Example:
 * <pre>
 * {@code
 * define LastTupleFromBag org.wikimedia.analytics.kraken.pig.LastTupleFromBag();
 *
 * -- input:
 * -- ({(a,1), (b,2), (c,3)})
 * input = LOAD 'input' AS (B: bag {T: tuple(alpha:CHARARRAY, numeric:INT)});
 *
 * output = FOREACH input GENERATE LastTupleFromBag(B); -- same as LastTupleFromBag(B, null)
 *
 * -- output:
 * -- (c,3)
 * }
 * </pre>
 */
public class LastTupleFromBag extends SimpleEvalFunc<Tuple> {

    public Tuple call(DataBag bag) throws IOException {
        return call(bag, null);
    }

    public Tuple call(DataBag bag, Tuple defaultValue) throws IOException {
        return Iterators.getLast(bag.iterator(), defaultValue);
    }

    @Override
    public Schema outputSchema(Schema input) {
        try {
            return new Schema(input.getField(0).schema);
        }
        catch (Exception e) {
            return null;
        }
    }
}

