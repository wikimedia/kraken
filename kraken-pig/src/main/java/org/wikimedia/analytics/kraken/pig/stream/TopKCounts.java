package org.wikimedia.analytics.kraken.pig.stream;

import com.clearspring.analytics.stream.Counter;
import org.apache.pig.data.*;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.logicalLayer.schema.Schema;

public class TopKCounts extends TopK {
    private boolean flatten = false;


    public TopKCounts(int k) {
        this(k, null);
    }

    public TopKCounts(int k, String optFlatten) {
        this(k, optFlatten, k * 2);
    }

    public TopKCounts(int k, String optFlatten, int capacity) {
        super(k, capacity);
        if (optFlatten != null && optFlatten.toLowerCase().equals("flatten")) {
            flatten = true;
        }
    }

    @Override
    public DataBag getValue() {
        final DataBag output = BagFactory.getInstance().newDefaultBag();
        for (Counter<Tuple> counter : summary.topK(k)) {
            Tuple innerTuple = TupleFactory.getInstance().newTuple(counter.getItem().getAll());
            Tuple outputTuple = null;
            if (flatten) {
                innerTuple.append(counter.getCount());
                outputTuple = innerTuple;
            } else {
                outputTuple = TupleFactory.getInstance().newTuple();
                outputTuple.append(innerTuple);
                outputTuple.append(counter.getCount());
            }
            output.add(outputTuple);
        }
        return output;
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
            if (inputTupleSchema == null) inputTupleSchema = new Schema();
            Schema outputTupleSchema = null;

            if (flatten) {
                outputTupleSchema = inputTupleSchema.clone();
                outputTupleSchema.add(new Schema.FieldSchema("count", DataType.INTEGER));
            } else {
                outputTupleSchema = new Schema();
                outputTupleSchema.add(new Schema.FieldSchema("tuple_schema", inputTupleSchema.clone(), DataType.TUPLE));
                outputTupleSchema.add(new Schema.FieldSchema("count", DataType.INTEGER));
            }
            return new Schema(
                new Schema.FieldSchema(
                    getSchemaName(this.getClass().getName().toLowerCase(), input),
                    outputTupleSchema,
                    DataType.BAG
                )
            );
        }

        catch (CloneNotSupportedException e) {
            throw new RuntimeException(e);

        } catch (FrontendException e) {
            throw new RuntimeException(e);
        }
    }

}
