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

import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.logicalLayer.schema.Schema;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import junit.framework.TestCase;


public class ZeroFilterFuncTest extends TestCase {

    /** Factory to generate Pig tuples */
    private TupleFactory tupleFactory = TupleFactory.getInstance();

    public void testExecProperRequest() throws IOException {
        Tuple input = tupleFactory.newTuple(9);
        input.set(0, "http://en.m.wikipedia.org/wiki/James_Ingram"); // uri
        input.set(1, "-"); // referrer
        input.set(2, "-"); // user agent
        input.set(3, "hit/200"); // http status
        input.set(4, "209.34.2.203"); // remote_addr (random)
        input.set(5, "text/html; charset=UTF-8"); // content type
        input.set(6, "GET"); // request-method
        input.set(7, "zero=639-07;"); // analytics
        input.set(8, "2013-07-09T06:27:08.096"); // timestamp

        ZeroFilterFunc zero = new ZeroFilterFunc();

        boolean isZeroPageView = zero.exec(input);

        assertTrue("No zero page view, although it should be", isZeroPageView);
    }

    public void testExecProperRequestWithoutKey() throws IOException {
        Tuple input = tupleFactory.newTuple(9);
        input.set(0, "http://en.m.wikipedia.org/wiki/James_Ingram"); // uri
        input.set(1, "-"); // referrer
        input.set(2, "-"); // user agent
        input.set(3, "hit/200"); // http status
        input.set(4, "209.34.2.203"); // remote_addr (random)
        input.set(5, "text/html; charset=UTF-8"); // content type
        input.set(6, "GET"); // request-method
        input.set(7, "639-07;"); // analytics
        input.set(8, "2013-07-09T06:27:08.096"); // timestamp

        ZeroFilterFunc zero = new ZeroFilterFunc();

        boolean isZeroPageView = zero.exec(input);

        assertTrue("No zero page view, although it should be", isZeroPageView);
    }

    public void testExecProperRequestWithoutLanguage() throws IOException {
        Tuple input = tupleFactory.newTuple(9);
        input.set(0, "http://m.wikipedia.org/wiki/James_Ingram"); // uri
        input.set(1, "-"); // referrer
        input.set(2, "-"); // user agent
        input.set(3, "hit/200"); // http status
        input.set(4, "209.34.2.203"); // remote_addr (random)
        input.set(5, "text/html; charset=UTF-8"); // content type
        input.set(6, "GET"); // request-method
        input.set(7, "zero=639-07;"); // analytics
        input.set(8, "2013-07-09T06:27:08.096"); // timestamp

        ZeroFilterFunc zero = new ZeroFilterFunc();

        boolean isZeroPageView = zero.exec(input);

        assertTrue("No zero page view, although it should be", isZeroPageView);
    }

    public void testExecProperRequestWithoutSemicolon() throws IOException {
        Tuple input = tupleFactory.newTuple(9);
        input.set(0, "http://en.m.wikipedia.org/wiki/James_Ingram"); // uri
        input.set(1, "-"); // referrer
        input.set(2, "-"); // user agent
        input.set(3, "hit/200"); // http status
        input.set(4, "209.34.2.203"); // remote_addr (random)
        input.set(5, "text/html; charset=UTF-8"); // content type
        input.set(6, "GET"); // request-method
        input.set(7, "zero=639-07"); // analytics
        input.set(8, "2013-07-09T06:27:08.096"); // timestamp

        ZeroFilterFunc zero = new ZeroFilterFunc();

        boolean isZeroPageView = zero.exec(input);

        assertTrue("No zero page view, although it should be", isZeroPageView);
    }

    public void testExecAnalyticsEmpty() throws IOException {
        Tuple input = tupleFactory.newTuple(9);
        input.set(0, "http://en.zero.wikipedia.org/wiki/James_Ingram"); // uri
        input.set(1, "-"); // referrer
        input.set(2, "-"); // user agent
        input.set(3, "hit/200"); // http status
        input.set(4, "209.34.2.203"); // remote_addr (random)
        input.set(5, "text/html; charset=UTF-8"); // content type
        input.set(6, "GET"); // request-method
        input.set(7, ""); // analytics
        input.set(8, "2013-07-09T06:27:08.096"); // timestamp

        ZeroFilterFunc zero = new ZeroFilterFunc();

        boolean isZeroPageView = zero.exec(input);

        assertFalse("Zero page view, although it should not be", isZeroPageView);
    }

    public void testExecAnalyticsNull() throws IOException {
        Tuple input = tupleFactory.newTuple(9);
        input.set(0, "http://en.zero.wikipedia.org/wiki/James_Ingram"); // uri
        input.set(1, "-"); // referrer
        input.set(2, "-"); // user agent
        input.set(3, "hit/200"); // http status
        input.set(4, "209.34.2.203"); // remote_addr (random)
        input.set(5, "text/html; charset=UTF-8"); // content type
        input.set(6, "GET"); // request-method
        input.set(7, null); // analytics
        input.set(8, "2013-07-09T06:27:08.096"); // timestamp

        ZeroFilterFunc zero = new ZeroFilterFunc();

        boolean isZeroPageView = zero.exec(input);

        assertFalse("Zero page view, although it should not be", isZeroPageView);
    }

    public void testExecAnalyticsDash() throws IOException {
        Tuple input = tupleFactory.newTuple(9);
        input.set(0, "http://en.zero.wikipedia.org/wiki/James_Ingram"); // uri
        input.set(1, "-"); // referrer
        input.set(2, "-"); // user agent
        input.set(3, "hit/200"); // http status
        input.set(4, "209.34.2.203"); // remote_addr (random)
        input.set(5, "text/html; charset=UTF-8"); // content type
        input.set(6, "GET"); // request-method
        input.set(7, "-"); // analytics
        input.set(8, "2013-07-09T06:27:08.096"); // timestamp

        ZeroFilterFunc zero = new ZeroFilterFunc();

        boolean isZeroPageView = zero.exec(input);

        assertFalse("Zero page view, although it should not be", isZeroPageView);
    }

    public void testExecZeroDomainAlthoughNotFree() throws IOException {
        // For this test to work, assure that for the zero partner with
        // MCC-MNC 612-03 requests to the m domain are not free.

        Tuple input = tupleFactory.newTuple(9);
        input.set(0, "http://en.zero.wikipedia.org/wiki/James_Ingram"); // uri
        input.set(1, "-"); // referrer
        input.set(2, "-"); // user agent
        input.set(3, "hit/200"); // http status
        input.set(4, "209.34.2.203"); // remote_addr (random)
        input.set(5, "text/html; charset=UTF-8"); // content type
        input.set(6, "GET"); // request-method
        input.set(7, "zero=612-03;"); // analytics
        input.set(8, "2013-07-09T06:27:08.096"); // timestamp

        ZeroFilterFunc zero = new ZeroFilterFunc();

        boolean isZeroPageView = zero.exec(input);

        assertFalse("Zero page view, although it should not be", isZeroPageView);
    }

    public void testExecMobileDomainIfOnlyMobileIsFree() throws IOException {
        // For this test to work, assure that for the zero partner with
        // MCC-MNC 612-03 requests to the zero domain are not free, but
        // requests to the m domain are free.

        Tuple input = tupleFactory.newTuple(9);
        input.set(0, "http://en.m.wikipedia.org/wiki/James_Ingram"); // uri
        input.set(1, "-"); // referrer
        input.set(2, "-"); // user agent
        input.set(3, "hit/200"); // http status
        input.set(4, "209.34.2.203"); // remote_addr (random)
        input.set(5, "text/html; charset=UTF-8"); // content type
        input.set(6, "GET"); // request-method
        input.set(7, "zero=612-03;"); // analytics
        input.set(8, "2013-07-09T06:27:08.096"); // timestamp

        ZeroFilterFunc zero = new ZeroFilterFunc();

        boolean isZeroPageView = zero.exec(input);

        assertTrue("No zero page view, although it should be", isZeroPageView);
    }

    public void testExecMobileDomainAlthoughNotFree() throws IOException {
        // For this test to work, assure that for the zero partner with
        // MCC-MNC 520-18 requests to the m domain are not free.

        Tuple input = tupleFactory.newTuple(9);
        input.set(0, "http://en.m.wikipedia.org/wiki/James_Ingram"); // uri
        input.set(1, "-"); // referrer
        input.set(2, "-"); // user agent
        input.set(3, "hit/200"); // http status
        input.set(4, "209.34.2.203"); // remote_addr (random)
        input.set(5, "text/html; charset=UTF-8"); // content type
        input.set(6, "GET"); // request-method
        input.set(7, "zero=520-18;"); // analytics
        input.set(8, "2013-07-09T06:27:08.096"); // timestamp

        ZeroFilterFunc zero = new ZeroFilterFunc();

        boolean isZeroPageView = zero.exec(input);

        assertFalse("Zero page view, although it should not be", isZeroPageView);
    }

    public void testExecZeroDomainIfOnlyZeroIsFree() throws IOException {
        // For this test to work, assure that for the zero partner with
        // MCC-MNC 520-18 requests to the zero domain are not free, but
        // requests to the m domain are free.

        Tuple input = tupleFactory.newTuple(9);
        input.set(0, "http://en.zero.wikipedia.org/wiki/James_Ingram"); // uri
        input.set(1, "-"); // referrer
        input.set(2, "-"); // user agent
        input.set(3, "hit/200"); // http status
        input.set(4, "209.34.2.203"); // remote_addr (random)
        input.set(5, "text/html; charset=UTF-8"); // content type
        input.set(6, "GET"); // request-method
        input.set(7, "zero=520-18;"); // analytics
        input.set(8, "2013-07-09T06:27:08.096"); // timestamp

        ZeroFilterFunc zero = new ZeroFilterFunc();

        boolean isZeroPageView = zero.exec(input);

        assertTrue("No zero page view, although it should be", isZeroPageView);
    }

    public void testExecNonMobileNonZeroDomain() throws IOException {
        Tuple input = tupleFactory.newTuple(9);
        input.set(0, "http://en.DifferentDomain.wikipedia.org/wiki/James_Ingram"); // uri
        input.set(1, "-"); // referrer
        input.set(2, "-"); // user agent
        input.set(3, "hit/200"); // http status
        input.set(4, "209.34.2.203"); // remote_addr (random)
        input.set(5, "text/html; charset=UTF-8"); // content type
        input.set(6, "GET"); // request-method
        input.set(7, "zero=Some-Partner-MCC-MNC;"); // analytics
        input.set(8, "2013-07-09T06:27:08.096"); // timestamp

        ZeroFilterFunc zero = new ZeroFilterFunc();

        boolean isZeroPageView = zero.exec(input);

        assertFalse("Zero page view, although it should not be", isZeroPageView);
    }

    public void testExecLanguangeLimitationFreeLanguage() throws IOException {
        // For this test to work, assure that for the zero partner with
        // MCC-MNC 624-02 requests to "eo" language are free for the "m".

        Tuple input = tupleFactory.newTuple(9);
        input.set(0, "http://eo.m.wikipedia.org/wiki/James_Ingram"); // uri
        input.set(1, "-"); // referrer
        input.set(2, "-"); // user agent
        input.set(3, "hit/200"); // http status
        input.set(4, "209.34.2.203"); // remote_addr (random)
        input.set(5, "text/html; charset=UTF-8"); // content type
        input.set(6, "GET"); // request-method
        input.set(7, "zero=624-02;"); // analytics
        input.set(8, "2013-07-09T06:27:08.096"); // timestamp

        ZeroFilterFunc zero = new ZeroFilterFunc();

        boolean isZeroPageView = zero.exec(input);

        assertTrue("No zero page view, although it should be", isZeroPageView);
    }

    public void testExecLanguageLimitationNonFreelanguage() throws IOException {
        // For this test to work, assure that for the zero partner with
        // MCC-MNC 624-02 requests to "it" language are not free for the "m" domain.

        Tuple input = tupleFactory.newTuple(9);
        input.set(0, "http://it.m.wikipedia.org/wiki/James_Ingram"); // uri
        input.set(1, "-"); // referrer
        input.set(2, "-"); // user agent
        input.set(3, "hit/200"); // http status
        input.set(4, "209.34.2.203"); // remote_addr (random)
        input.set(5, "text/html; charset=UTF-8"); // content type
        input.set(6, "GET"); // request-method
        input.set(7, "zero=624-02;"); // analytics
        input.set(8, "2013-07-09T06:27:08.096"); // timestamp

        ZeroFilterFunc zero = new ZeroFilterFunc();

        boolean isZeroPageView = zero.exec(input);

        assertFalse("Zero page view, although it should not be", isZeroPageView);
    }

    public void testExecZeitVorher() throws IOException {
        Tuple input = tupleFactory.newTuple(9);
        input.set(0, "http://en.m.wikipedia.org/wiki/James_Ingram"); // uri
        input.set(1, "-"); // referrer
        input.set(2, "-"); // user agent
        input.set(3, "hit/200"); // http status
        input.set(4, "209.34.2.203"); // remote_addr (random)
        input.set(5, "text/html; charset=UTF-8"); // content type
        input.set(6, "GET"); // request-method
        input.set(7, "zero=404-01;"); // analytics
        input.set(8, "2013-07-24T23:59:59.999"); // timestamp

        ZeroFilterFunc zero = new ZeroFilterFunc();

        boolean isZeroPageView = zero.exec(input);

        assertFalse("Zero page view, although it should not be", isZeroPageView);
    }

    public void testExecZeitNachher() throws IOException {
        Tuple input = tupleFactory.newTuple(9);
        input.set(0, "http://en.m.wikipedia.org/wiki/James_Ingram"); // uri
        input.set(1, "-"); // referrer
        input.set(2, "-"); // user agent
        input.set(3, "hit/200"); // http status
        input.set(4, "209.34.2.203"); // remote_addr (random)
        input.set(5, "text/html; charset=UTF-8"); // content type
        input.set(6, "GET"); // request-method
        input.set(7, "zero=404-01;"); // analytics
        input.set(8, "2013-07-25T04:59:08.096"); // timestamp

        ZeroFilterFunc zero = new ZeroFilterFunc();

        boolean isZeroPageView = zero.exec(input);

        assertTrue("No zero page view, although it should be", isZeroPageView);
    }

    public void testExecZeitVorherOhneMS() throws IOException {
        Tuple input = tupleFactory.newTuple(9);
        input.set(0, "http://en.m.wikipedia.org/wiki/James_Ingram"); // uri
        input.set(1, "-"); // referrer
        input.set(2, "-"); // user agent
        input.set(3, "hit/200"); // http status
        input.set(4, "209.34.2.203"); // remote_addr (random)
        input.set(5, "text/html; charset=UTF-8"); // content type
        input.set(6, "GET"); // request-method
        input.set(7, "zero=404-01;"); // analytics
        input.set(8, "2013-07-24T23:59:59"); // timestamp

        ZeroFilterFunc zero = new ZeroFilterFunc();

        boolean isZeroPageView = zero.exec(input);

        assertFalse("Zero page view, although it should not be", isZeroPageView);
    }

    public void testExecZeitNachherOhneMS() throws IOException {
        Tuple input = tupleFactory.newTuple(9);
        input.set(0, "http://en.m.wikipedia.org/wiki/James_Ingram"); // uri
        input.set(1, "-"); // referrer
        input.set(2, "-"); // user agent
        input.set(3, "hit/200"); // http status
        input.set(4, "209.34.2.203"); // remote_addr (random)
        input.set(5, "text/html; charset=UTF-8"); // content type
        input.set(6, "GET"); // request-method
        input.set(7, "zero=404-01;"); // analytics
        input.set(8, "2013-07-25T04:59:08"); // timestamp

        ZeroFilterFunc zero = new ZeroFilterFunc();

        boolean isZeroPageView = zero.exec(input);

        assertTrue("No zero page view, although it should be", isZeroPageView);
    }

    public void testExecZeitVorherOhneVolleMS() throws IOException {
        Tuple input = tupleFactory.newTuple(9);
        input.set(0, "http://en.m.wikipedia.org/wiki/James_Ingram"); // uri
        input.set(1, "-"); // referrer
        input.set(2, "-"); // user agent
        input.set(3, "hit/200"); // http status
        input.set(4, "209.34.2.203"); // remote_addr (random)
        input.set(5, "text/html; charset=UTF-8"); // content type
        input.set(6, "GET"); // request-method
        input.set(7, "zero=404-01;"); // analytics
        input.set(8, "2013-07-24T23:59:59:9"); // timestamp

        ZeroFilterFunc zero = new ZeroFilterFunc();

        boolean isZeroPageView = zero.exec(input);

        assertFalse("Zero page view, although it should not be", isZeroPageView);
    }

    public void testExecZeitNachherOhneVolleMS() throws IOException {
        Tuple input = tupleFactory.newTuple(9);
        input.set(0, "http://en.m.wikipedia.org/wiki/James_Ingram"); // uri
        input.set(1, "-"); // referrer
        input.set(2, "-"); // user agent
        input.set(3, "hit/200"); // http status
        input.set(4, "209.34.2.203"); // remote_addr (random)
        input.set(5, "text/html; charset=UTF-8"); // content type
        input.set(6, "GET"); // request-method
        input.set(7, "zero=404-01;"); // analytics
        input.set(8, "2013-07-25T04:59:08.3"); // timestamp

        ZeroFilterFunc zero = new ZeroFilterFunc();

        boolean isZeroPageView = zero.exec(input);

        assertTrue("No zero page view, although it should be", isZeroPageView);
    }

    public void testExecApiZeroConfig() throws IOException {
        Tuple input = tupleFactory.newTuple(9);
        input.set(0, "http://en.m.wikipedia.org/w/api.php?format=json&action=zeroconfig&type=config"); // uri
        input.set(1, "http://en.m.wikipedia.org/wiki/Main_Page"); // referrer
        input.set(2, "-"); // user agent
        input.set(3, "hit/200"); // http status
        input.set(4, "209.34.2.203"); // remote_addr (random)
        input.set(5, "application/json; charset=utf-8"); // content type
        input.set(6, "GET"); // request-method
        input.set(7, "zero=404-01;"); // analytics
        input.set(8, "2013-07-25T04:59:08.3"); // timestamp

        ZeroFilterFunc zero = new ZeroFilterFunc();

        boolean isZeroPageView = zero.exec(input);

        assertFalse("Zero page view, although it should not be", isZeroPageView);
    }

    public void testOutputSchemaMatch() throws IOException {
        List<Schema.FieldSchema> fields = new ArrayList<Schema.FieldSchema>(9);
        fields.add(new Schema.FieldSchema(null, DataType.CHARARRAY));
        fields.add(new Schema.FieldSchema(null, DataType.CHARARRAY));
        fields.add(new Schema.FieldSchema(null, DataType.CHARARRAY));
        fields.add(new Schema.FieldSchema(null, DataType.CHARARRAY));
        fields.add(new Schema.FieldSchema(null, DataType.CHARARRAY));
        fields.add(new Schema.FieldSchema(null, DataType.CHARARRAY));
        fields.add(new Schema.FieldSchema(null, DataType.CHARARRAY));
        fields.add(new Schema.FieldSchema(null, DataType.CHARARRAY));
        fields.add(new Schema.FieldSchema(null, DataType.CHARARRAY));

        Schema schema = new Schema(fields);

        ZeroFilterFunc zero = new ZeroFilterFunc();

        Schema output = zero.outputSchema(schema);

        assertNotNull("Output schema was null", output);
        List<Schema.FieldSchema> outputFields = output.getFields();

        assertEquals("size of output schema does not match", 1,
                outputFields.size());

        assertEquals("Data type of field #0 does not match expected type",
                DataType.BOOLEAN, outputFields.get(0).type);

    }

    public void testOutputSchemaInputWrongSize() throws IOException {
        List<Schema.FieldSchema> fields = new ArrayList<Schema.FieldSchema>(1);
        fields.add(new Schema.FieldSchema(null, DataType.CHARARRAY));

        Schema schema = new Schema(fields);

        ZeroFilterFunc zero = new ZeroFilterFunc();

        try {
            zero.outputSchema(schema);
            fail("schema did match although it should not");
        } catch (Exception e) {
            String msg = e.getMessage();
            assertNotNull("Exception's message was null", msg);
            assertTrue("Exception's message does not contain 'size', but "
                    + "was " + msg, msg.contains("size"));
        }
    }

    public void testOutputSchemaInputWrongType() throws IOException {
        List<Schema.FieldSchema> fields = new ArrayList<Schema.FieldSchema>(9);
        fields.add(new Schema.FieldSchema(null, DataType.CHARARRAY));
        fields.add(new Schema.FieldSchema(null, DataType.CHARARRAY));
        fields.add(new Schema.FieldSchema(null, DataType.CHARARRAY));
        fields.add(new Schema.FieldSchema(null, DataType.CHARARRAY));
        fields.add(new Schema.FieldSchema(null, DataType.BOOLEAN));
        fields.add(new Schema.FieldSchema(null, DataType.CHARARRAY));
        fields.add(new Schema.FieldSchema(null, DataType.CHARARRAY));
        fields.add(new Schema.FieldSchema(null, DataType.CHARARRAY));
        fields.add(new Schema.FieldSchema(null, DataType.CHARARRAY));

        Schema schema = new Schema(fields);

        ZeroFilterFunc zero = new ZeroFilterFunc();

        try {
            zero.outputSchema(schema);
            fail("schema did match although it should not");
        } catch (Exception e) {
            String msg = e.getMessage();
            assertNotNull("Exception's message was null", msg);
            assertTrue("Exception's message does not contain 'not match', but "
                    + "was " + msg, msg.contains("not match"));
        }
    }
}
