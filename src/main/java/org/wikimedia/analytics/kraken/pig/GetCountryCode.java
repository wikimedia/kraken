/** Wrapper class for getCountry and getCountryIpV6 */

package org.wikimedia.analytics.kraken.pig;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;

import org.apache.pig.EvalFunc;
import org.apache.pig.PigWarning;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;

import com.maxmind.geoip.Country;
import com.maxmind.geoip.LookupService;

public class GetCountryCode extends EvalFunc<Tuple> {

	private static String lookupFileName;

	private LookupService lookupService;
	private TupleFactory tupleFactory = TupleFactory.getInstance();

	public GetCountryCode(String lookupFilename) {
		GetCountryCode.lookupFileName = lookupFilename;
	}

	@Override
	public List<String> getCacheFiles() {
		List<String> cacheFiles = new ArrayList<String>(1);
		// Note that this forces us to use basenames only. If we need
		// to support other paths, we either need two arguments in the
		// constructor, or to parse the filename to extract the basename.
		cacheFiles.add(lookupFileName + "#" + lookupFileName);
		return cacheFiles;
	}

	@Override
	public Tuple exec(Tuple input) throws IOException {
		Tuple output = null;
		if (input == null || input.size() == 0) {
			return null;
		}

		if (lookupService == null) {
			lookupService = new LookupService(lookupFileName);
		}

		String ip = (String) input.get(0);
		Country country = Pattern.matches("^(?:[0-9]{1,3}\\.){3}[0-9]{1,3}$",ip) ? lookupService.getCountry(ip) : lookupService.getCountryV6(ip);

		if (country != null) {
			output = tupleFactory.newTuple(1);
			output.set(0, country.getCode());
		}
		else {
			warn("Couldn't get country for the ip: " + ip, PigWarning.UDF_WARNING_1);
		}

		return output;

	}

}
