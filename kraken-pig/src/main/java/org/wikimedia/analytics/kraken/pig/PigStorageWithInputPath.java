/**
 *Copyright (C) 2012-2013  Wikimedia Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.wikimedia.analytics.kraken.pig;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.PigSplit;
import org.apache.pig.builtin.PigStorage;
import org.apache.pig.data.Tuple;

import java.io.IOException;

/**
 *
 */
@Deprecated
public class PigStorageWithInputPath extends PigStorage {
    Path path = null;

    // SHOULD BE THIS: but error is thrown in grunt, so hardcoded for now
    // public PigStorageWithInputPath(String delim) {
    //     super(delim);
    // }

    /**
     * <p>Constructor for PigStorageWithInputPath.</p>
     */
    public PigStorageWithInputPath() {
        super("\t");
    }

    /** {@inheritDoc} */
    @Override
    public void prepareToRead(RecordReader reader, PigSplit split) {
        super.prepareToRead(reader, split);
        path = ((FileSplit) split.getWrappedSplit()).getPath();
    }

    /** {@inheritDoc} */
    @Override
    public Tuple getNext() throws IOException {
        Tuple myTuple = super.getNext();
        if (myTuple != null)
            myTuple.append(path.toString());
        return myTuple;
    }
}
