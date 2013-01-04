package org.wikimedia.analytics.kraken.pig;

import static org.junit.Assert.*;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.IOException;

import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.BagFactory;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.junit.Test;

public class FunnelTest {
	private Funnel funnel = new Funnel();
	private TupleFactory tupleFactory = TupleFactory.getInstance();

	@Test
	public void testExec1() throws IOException {
		Tuple input = defaultInput();
		Tuple output = funnel.exec(input);
		assertNotNull(output);
		assertTrue((Boolean) output.get(0));
		assertNull(output.get(1));
	}
	
	@Test
	public void testExec2() throws IOException {
		Tuple input = defaultInput();
		Tuple row1 = tupleFactory.newTuple(3);
		Tuple row2 = tupleFactory.newTuple(3);
		row1.set(0, "1");
		row2.set(0, "1");
		row1.set(1, 5);
		row2.set(1, 10);
		row1.set(2, "http://www.wikimedia.org/A");
		row2.set(2, "http://www.wikimedia.org/C");
		DataBag bag = BagFactory.getInstance().newDefaultBag();
		bag.add(row1);
		bag.add(row2);
		input.set(0, bag);
		Tuple output = funnel.exec(input);
		assertNotNull(output);
		assertTrue(!(Boolean) output.get(0));
		assertNotNull(output.get(1));
		assertEquals(output.get(1), "http://www.wikimedia.org/C");
	}

	private Tuple defaultInput() throws ExecException {
		DataBag bag = BagFactory.getInstance().newDefaultBag();
		Tuple urls = tupleFactory.newTuple(4);
		urls.set(0, "http://www.wikimedia.org/A");
		urls.set(1, "http://www.wikimedia.org/B");
		urls.set(2, "http://www.wikimedia.org/C");
		urls.set(3, "http://www.wikimedia.org/D");
		Tuple input = tupleFactory.newTuple(4);
		Tuple DAG = tupleFactory.newTuple(4);
		Tuple edge1 = tupleFactory.newTuple(2);
		Tuple edge2 = tupleFactory.newTuple(2);
		Tuple edge3 = tupleFactory.newTuple(2);
		Tuple edge4 = tupleFactory.newTuple(2);
		edge1.set(0, 0);
		edge1.set(1	, 1);
		edge2.set(0	, 0);
		edge2.set(1	, 2);
		edge3.set(0	, 1);
		edge3.set(1	, 3);
		edge4.set(0	, 2);
		edge4.set(1	, 3);
		DAG.set(0, edge1);
		DAG.set(1, edge2);
		DAG.set(2, edge3);
		DAG.set(3, edge4);
		Tuple row1 = tupleFactory.newTuple(3);
		Tuple row2 = tupleFactory.newTuple(3);
		Tuple row3 = tupleFactory.newTuple(3);
		row1.set(0, "1");
		row2.set(0, "1");
		row3.set(0, "1");
		row1.set(1, 5);
		row2.set(1, 10);
		row3.set(1, 15);
		row1.set(2, "http://www.wikimedia.org/A");
		row2.set(2, "http://www.wikimedia.org/C");
		row3.set(2, "http://www.wikimedia.org/D");
		bag.add(row1);
		bag.add(row2);
		bag.add(row3);
		input.set(0, bag);
		input.set(1, urls);
		input.set(2, DAG);
		input.set(3, 100);
		return input;
	}
	
}
