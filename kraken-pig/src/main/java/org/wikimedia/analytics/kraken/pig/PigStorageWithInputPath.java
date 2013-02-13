/**
 *Copyright (C) 2012-2013  Wikimedia Foundation
 *
 *This program is free software; you can redistribute it and/or
 *modify it under the terms of the GNU General Public License
 *as published by the Free Software Foundation; either version 2
 *of the License, or (at your option) any later version.
 *
 *This program is distributed in the hope that it will be useful,
 *but WITHOUT ANY WARRANTY; without even the implied warranty of
 *MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *GNU General Public License for more details.
 *
 *You should have received a copy of the GNU General Public License
 *along with this program; if not, write to the Free Software
 *Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.

 */
package org.wikimedia.analytics.kraken.pig;

import org.apache.pig.builtin.PigStorage;
import org.apache.pig.data.Tuple;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.PigSplit;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import java.io.IOException;

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
        super(" ");
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
