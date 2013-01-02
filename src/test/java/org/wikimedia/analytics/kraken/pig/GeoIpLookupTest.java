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
import static org.junit.Assert.assertNull;

import java.io.IOException;

import org.junit.Test;

import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;


public class GeoIpLookupTest {
	private GeoIpLookup geo = new GeoIpLookup("/usr/share/GeoIP/GeoIPCity.dat");
	private TupleFactory tupleFactory = TupleFactory.getInstance();
	private Tuple input = tupleFactory.newTuple(1);
	
	
	/**
	 * ipAddress
	 * @throws IOException 
	 */
	@Test
	public void testExec1() throws IOException {
		input.set(0, "71.217.23.156");
		Tuple geoData = geo.exec(input);
		assertNotNull(geoData);
		String countryCode = (String) geoData.get(1);
		assertEquals("US", countryCode);
		String continentCode = (String) geoData.get(6);
		assertEquals("NA", continentCode);
	}
	
	/**
	 * localhost
	 * @throws IOException 
	 */
	@Test
	public void testExec2() throws IOException {
		input.set(0, "127.0.0.1");
		Tuple output = geo.exec(input);
		assertNull(output);
	}
	
	/**
	 * Wikipedia
	 * @throws IOException 
	 */
	@Test
	public void testExec3() throws IOException {
		input.set(0, "wikipedia.org");
		String output = (String) geo.exec(input).get(1);	
		assertEquals("US", output);
	}


}
