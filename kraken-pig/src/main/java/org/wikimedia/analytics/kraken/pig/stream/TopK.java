package org.wikimedia.analytics.kraken.pig.stream;

import com.clearspring.analytics.stream.StreamSummary;
import org.apache.pig.Accumulator;
import org.apache.pig.EvalFunc;
import org.apache.pig.data.BagFactory;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.logicalLayer.schema.Schema;

import java.io.IOException;

public class TopK extends EvalFunc<DataBag> implements Accumulator<DataBag> {
    protected final int k;
    protected final int capacity;
    protected StreamSummary<Tuple> summary;


    public TopK(int k) {
        this(k, k * 2);
    }

    public TopK(int k, int capacity) {
        this.k = k;
        this.capacity = capacity;
        cleanup();
    }

    @Override
    public DataBag exec(Tuple input) throws IOException {
        accumulate(input);
        DataBag outputBag = getValue();
        cleanup();
        return outputBag;
    }

    @Override
    public void accumulate(Tuple input) throws IOException {
        summary.offer(input);
    }

    @Override
    public DataBag getValue() {
        final DataBag output = BagFactory.getInstance().newDefaultBag();
        for (Tuple t : summary.peek(k)) {
            output.add(t);
        }
        return output;
    }

    @Override
    public void cleanup() {
        summary = new StreamSummary<Tuple>(capacity);
    }

    @Override
    public Schema outputSchema(Schema input) {
        try {
            Schema.FieldSchema inputFieldSchema = input.getField(0);
            if (inputFieldSchema.type != DataType.BAG) {
                throw new RuntimeException("Expected a BAG as input");
            }

            Schema inputBagSchema = inputFieldSchema.schema;
            if (inputBagSchema.getField(0).type != DataType.TUPLE) {
                throw new RuntimeException(String.format("Expected input bag to contain a TUPLE, but instead found %s",
                        DataType.findTypeName(inputBagSchema.getField(0).type)));
            }

            Schema inputTupleSchema = inputBagSchema.getField(0).schema;
            Schema outputTupleSchema = inputTupleSchema.clone();
            return new Schema(
                new Schema.FieldSchema(
                    getSchemaName(this.getClass().getName().toLowerCase(), input),
                    outputTupleSchema,
                    DataType.BAG
                )
            );

        } catch (CloneNotSupportedException e) {
            throw new RuntimeException(e);

        } catch (FrontendException e) {
            throw new RuntimeException(e);
        }
    }

}
