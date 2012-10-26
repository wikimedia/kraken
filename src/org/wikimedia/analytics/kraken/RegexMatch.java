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

package org.wikimedia.analytics.kraken;

import java.io.IOException;
import java.util.*;
import java.util.regex.Pattern;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.*;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.impl.logicalLayer.schema.Schema.FieldSchema;

public class RegexMatch extends EvalFunc<String> {
	protected Pattern pattern;

	public RegexMatch() {
		pattern = null;
	}

	public RegexMatch(String regex) {
		// use this if you want to define your regex at compile-time
		pattern = Pattern.compile(regex);
	}

	public String exec(Tuple input) throws IOException {
		String inputString = (String) input.get(0);
		// return null if input is null
		if (inputString == null) {
			return "false";
		}
		// compile the given regex if it has not been defined yet
		if (pattern == null) {
			pattern = Pattern.compile((String) input.get(1));
		}
		return pattern.matcher(inputString).matches() ? "true" : "false";
	}

	public Schema outputSchema(Schema input) {
		List<FieldSchema> arguments = new LinkedList<FieldSchema>();
		arguments.add(new FieldSchema(null, DataType.CHARARRAY));
		// require a pattern if it hasn't been defined yet
		if (pattern == null) {
			arguments.add(new FieldSchema(null, DataType.CHARARRAY));
		}
		Schema inputModel = new Schema(arguments);
		// check if input fits schema model
		if (!Schema.equals(inputModel, input, true, true)) {
			String msg = "";
			if (arguments.size() == 1) {
				msg = "\n you already defined a regex in the UDF definition, delete it if you want to use another one";
			}
			throw new IllegalArgumentException("Expected input schema "
					+ inputModel + ", received schema " + input + msg);
		}
		// output schema will be: (chararray).
		return new Schema(new FieldSchema(null, DataType.CHARARRAY));
	}
}
