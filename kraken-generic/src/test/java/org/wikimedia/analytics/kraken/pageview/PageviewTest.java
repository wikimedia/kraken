/**
 * Copyright (C) 2012  Wikimedia Foundation

 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or (at your option) any later version.

 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.

 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.
 */
package org.wikimedia.analytics.kraken.pageview;


import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class PageviewTest {

    Pageview pageview;

    @Test
    public void test1Pageview() {
        String logLines = "ssl1002,362176022,1970-01-01T00:00:00.000,0.086,0.0.0.0,FAKE_CACHE_STATUS/301,680,GET https://fr.wikipedia.org/wiki/Discussion:Histoire du Racing Club de Strasbourg,NONE/wikipedia,-,-,-,Mozilla/5.0%20(compatible;%20Googlebot/2.1;%20+http://www.google.com/bot.html),-,-";
        String[] logFields = logLines.split(",");
        pageview = new Pageview(logFields[8], logFields[12], logFields[14], logFields[5], logFields[6], logFields[11], logFields[7]);
        assertEquals(pageview.validate(), false);
    }
}
