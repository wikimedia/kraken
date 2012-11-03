/**
Copyright (C) 2012  Wikimedia Foundation

This program is free software; you can redistribute it and/or
modify it under the terms of the GNU General Public License
as published by the Free Software Foundation; either version 2
of the License, or (at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public License
along with this program; if not, write to the Free Software
Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA. 
 */

package org.wikimedia.analytics.kraken.pig;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.junit.Before;
import org.junit.Test;

import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;


public class ParseWikiUrlTest {
	
	private ParseWikiUrl parseWikiUrl = new ParseWikiUrl(true);
	private TupleFactory tupleFactory = TupleFactory.getInstance();
	private Tuple defaultOutput = tupleFactory.newTuple(3);
	
	@Before public void before() throws ExecException {
		defaultOutput.set(0, "N/A");
		defaultOutput.set(1, null);
		defaultOutput.set(2, "N/A");
	}
	
	/**
	 * average url
	 * @throws ExecException
	 */
	@Test
	public void testExec1() throws ExecException {
		Tuple input = tupleFactory.newTuple(1);
		input.set(0, " http://en.wikipedia.org/wiki/Main_Page");
		Tuple output = parseWikiUrl.exec(input);
		assertNotNull(output);
		String outputLang = (String) output.get(0);
		Boolean outputMobile = (Boolean) output.get(1);
		String outputDomain = (String) output.get(2);
		assertEquals(outputLang, "en");
		assertFalse(outputMobile);
		assertEquals(outputDomain,  "wikipedia.org");
	}
	
	/**
	 * mobile site
	 * @throws ExecException
	 */
	@Test
	public void testExec2() throws ExecException {
		Tuple input = tupleFactory.newTuple(1);
		input.set(0, "http://en.m.wikipedia.org/");
		Tuple output = parseWikiUrl.exec(input);
		assertNotNull(output);
		String outputLang = (String) output.get(0);
		Boolean outputMobile = (Boolean) output.get(1);
		String outputDomain = (String) output.get(2);
		assertEquals(outputLang, "en");
		assertTrue(outputMobile);
		assertEquals(outputDomain,  "wikipedia.org");
	}

	/**
	 * malformed url
	 * @throws ExecException
	 */
	@Test
	public void testExec3() throws ExecException {
		Tuple input = tupleFactory.newTuple(1);
		input.set(0, "http:/en.wikipedia.org/");
		Tuple output = parseWikiUrl.exec(input);
		assertEquals(output, defaultOutput);
	}
	
	/**
	 * null input
	 * @throws ExecException
	 */
	@Test
	public void testExec4() throws ExecException {
		Tuple input = tupleFactory.newTuple(1);
		Tuple output = parseWikiUrl.exec(input);
		assertEquals(output, defaultOutput);
	}
	
	/**
	 * malformed url (no protocol)
	 * @throws ExecException
	 */
	@Test
	public void testExec5() throws ExecException {
		Tuple input = tupleFactory.newTuple(1);
		input.set(0, "en.m.wikipedia.org");
		Tuple output = parseWikiUrl.exec(input);
		assertEquals(output, defaultOutput);
	}
	
	/**
	 * no language
	 * @throws ExecException
	 */
	@Test
	public void testExec6() throws ExecException {
		Tuple input = tupleFactory.newTuple(1);
		input.set(0, "http://www.wikipedia.org/");
		Tuple output = parseWikiUrl.exec(input);	
		assertNotNull(output);
		String outputLang = (String) output.get(0);
		Boolean outputMobile = (Boolean) output.get(1);
		String outputDomain = (String) output.get(2);
		assertEquals(outputLang, "N/A");
		assertFalse(outputMobile);
		assertEquals(outputDomain,  "wikipedia.org");
	}
	
	/**
	 * less than two subdomain
	 * @throws ExecException
	 */
	@Test
	public void testExec7() throws ExecException {
		Tuple input = tupleFactory.newTuple(1);
		input.set(0, "http://wikipedia");
		Tuple output = parseWikiUrl.exec(input);
		assertEquals(output, defaultOutput);
	}
}
