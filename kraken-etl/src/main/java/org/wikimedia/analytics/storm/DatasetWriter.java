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

package org.wikimedia.analytics.storm;


import redis.clients.jedis.Jedis;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.Set;


public class DatasetWriter {

    private Jedis jedis;

    private Calendar calendar;


    public void init(final String redisHost, final Integer redisPort) {
        jedis = new Jedis(redisHost, redisPort);
        jedis.connect();
    }

    public void prepare() {
        //determine last finished hour
        setTimetoNowMinusOneHour();
        //create filehandle to write output
    }

    public void run() {
        File file = new File("");

        BufferedWriter bw = null;
        if (!file.exists()) {
            try {
                file.createNewFile();
                FileWriter fw = new FileWriter(file.getAbsoluteFile());
                bw = new BufferedWriter(fw);
            } catch (IOException e) {
                e.printStackTrace();
            }

            if (bw != null) {
                String count;
                String line;
                String[] patterns = {"en*", "!en*"} ;
                for (String pattern : patterns) {
                    Set<String> keys = jedis.keys(pattern);
                    for (String key : keys) {
                        count = jedis.hget(key, this.calendar.toString());
                        line = String.format("%s\t%s\n", key, count);
                        try {
                            bw.write(line);
                        } catch (IOException e) {
                            e.printStackTrace();
                        }
                    }
                }
                try {
                    bw.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    /**
     *
     */
    private void setTimetoNowMinusOneHour() {
        Date now = new java.util.Date();
        Calendar calendar = GregorianCalendar.getInstance();
        calendar.setTime(now);
        calendar.add(Calendar.HOUR, -1);
        this.calendar = calendar;
    }
}

