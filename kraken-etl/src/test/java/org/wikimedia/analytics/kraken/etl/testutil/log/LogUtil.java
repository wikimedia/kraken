// Copyright (C) 2013 Wikimedia Foundation
//
// This program is free software; you can redistribute it and/or
// modify it under the terms of the GNU General Public License
// as published by the Free Software Foundation; either version 2
// of the License, or (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU General Public License for more details.
//
// You should have received a copy of the GNU General Public License
// along with this program; if not, write to the Free Software
// Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.

package org.wikimedia.analytics.kraken.etl.testutil.log;

import org.apache.log4j.Appender;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.log4j.spi.LoggingEvent;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Enumeration;
import java.util.List;

public class LogUtil {
  /**
   * Change logger's setting so it only logs to a collection.
   *
   * @param logName Name of the logger to modify.
   * @param collection The collection to log into.
   * @return The logger's original settings.
   */
  public static LoggerSettings logToCollection(String logName,
      Collection<LoggingEvent> collection) {
    Logger logger = LogManager.getLogger(logName);
    LoggerSettings loggerSettings = new LoggerSettings(logger);
    logger.removeAllAppenders();
    logger.setAdditivity(false);
    CollectionAppender listAppender = new CollectionAppender(collection);
    logger.addAppender(listAppender);
    return loggerSettings;
  }

  /**
   * Capsule for a logger's settings that get mangled by rerouting logging to a collection
   */
  public static class LoggerSettings {
    private final boolean additive;
    private final List<Appender> appenders;

    /**
     * Read off logger settings from an instance.
     *
     * @param logger The logger to read the settings off from.
     */
    private LoggerSettings(Logger logger) {
        this.additive = logger.getAdditivity();

        Enumeration<?> appenders = logger.getAllAppenders();
        this.appenders = new ArrayList<Appender>();
        while (appenders.hasMoreElements()) {
            Object appender = appenders.nextElement();
            if (appender instanceof Appender) {
                this.appenders.add((Appender)appender);
            } else {
                throw new RuntimeException("getAllAppenders of " + logger
                        + " contained an object that is not an Appender");
            }
        }
    }

    /**
     * Pushes this settings back onto a logger.
     *
     * @param logger the logger on which to push the settings.
     */
    public void pushOntoLogger(Logger logger) {
      logger.setAdditivity(additive);
             logger.removeAllAppenders();
      for (Appender appender : appenders) {
        logger.addAppender(appender);
      }
    }
  }
}
