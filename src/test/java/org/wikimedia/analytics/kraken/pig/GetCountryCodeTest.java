package org.wikimedia.analytics.kraken.pig;

import org.junit.Test;

public class GetCountryCodeTest {

	@Test
	public void runCountryTest() {
		CountryLookupTest.main(null);
		CountryLookupTestV6.main(null);
	}
	
}
