package org.wikimedia.analytics.kraken.eventlogging;

import static org.junit.Assert.*;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URL;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.Charset;
import java.util.Enumeration;

import org.junit.Before;
import org.junit.Test;

public class ParserTest {

	@Before
	public void setUp() throws Exception {
	}

	@Test
	public void test() throws FileNotFoundException, IOException {
		Parser parser = new Parser();
		Enumeration<URL> urls = null;
		urls = getClass().getClassLoader().getResources("funnel/src/test/resources/");
		FileInputStream stream = null;
		String jsonSchema;
		URL url;
		while (urls.hasMoreElements()) {
			url = urls.nextElement();
			System.out.println("Reading file: " + url.toString());
			stream = new FileInputStream(url.getFile());
			FileChannel fc = stream.getChannel();
			MappedByteBuffer bb = fc.map(FileChannel.MapMode.READ_ONLY, 0, fc.size());
			/* Instead of using default, pass in a decoder. */
			jsonSchema = Charset.defaultCharset().decode(bb).toString();
			System.out.println(jsonSchema.toString());
			parser.parseEventLoggingJsonSchem(jsonSchema);
		}
		if (stream != null) {
			stream.close();
		}
	}
}
