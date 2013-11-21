package org.wikimedia.analytics.kraken.hive;
import java.io.IOException;

import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.io.Text;

//import ua_parser.Device;
import ua_parser.Parser;
import ua_parser.Client;


@Description(
		name = "uadetect",
		value = "_FUNC_(str) - Finds OS of useragent",
		extended = "Example:\n" +
		"  > SELECT uadetect(author_name) FROM varnish2;\n" +
		"  iPhone"
		)
public class UADetect extends UDF {
	
	public Parser p;
	public UADetect() {
		try {
			p = new ua_parser.Parser();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		};
	}
	
	
    public Text evaluate(Text s) {
		Text to_value = new Text("");
		if (s != null) {
		    try {
		    	Client c = p.parse(s.toString());
		    	System.err.println("");
				to_value.set(c.os.family);
		    } catch (Exception e) { // Should never happen
		    	System.out.println("ERR could not classify\n");
				to_value = new Text("ERR_cant_classify");
		    }
		}
		return to_value;
    }
}
