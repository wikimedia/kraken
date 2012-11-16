package org.wikimedia.analytics.kraken.pig;

import org.apache.pig.builtin.PigStorage;
import org.apache.pig.EvalFunc;
import org.apache.pig.data.Tuple;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.PigSplit;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.pig.impl.util.StorageUtil;

import java.io.IOException;

public class PigStorageWithInputPath extends PigStorage {
    Path path = null;

    // SHOULD BE THIS: but error is thrown in grunt, so hardcoded for now
    // public PigStorageWithInputPath(String delim) {
    //     super(delim);
    // }

    public PigStorageWithInputPath() {
        super(" ");
    }

    @Override
    public void prepareToRead(RecordReader reader, PigSplit split) {
        super.prepareToRead(reader, split);
        path = ((FileSplit)split.getWrappedSplit()).getPath();
    }

    @Override
    public Tuple getNext() throws IOException {
        Tuple myTuple = super.getNext();
        if (myTuple != null)
            myTuple.append(path.toString());
        return myTuple;
    }
}