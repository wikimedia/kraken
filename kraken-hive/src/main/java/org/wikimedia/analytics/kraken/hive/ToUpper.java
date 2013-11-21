package org.wikimedia.analytics.kraken.hive;

import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.io.Text;

// See http://snowplowanalytics.com/blog/2013/02/08/writing-hive-udfs-and-serdes/
@Description(
	name = "toupper",
	value = "_FUNC_(str) - Converts a string to uppercase",
	extended = "Example:\n" +
	"  > SELECT toupper(author_name) FROM authors a;\n" +
	"  STEPHEN KING"
	)
public class ToUpper extends UDF {

    public Text evaluate(Text s) {
		Text to_value = new Text("");
		if (s != null) {
		    try {
				to_value.set(s.toString().toUpperCase());
		    } catch (Exception e) { // Should never happen
				to_value = new Text(s);
		    }
		}
		return to_value;
    }
}
