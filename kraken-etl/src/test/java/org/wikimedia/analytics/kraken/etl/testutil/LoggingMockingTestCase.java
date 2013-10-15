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

package org.wikimedia.analytics.kraken.etl.testutil;

import com.google.common.collect.Lists;

import org.wikimedia.analytics.kraken.etl.testutil.log.LogUtil;
import org.apache.log4j.spi.LoggingEvent;
import org.junit.After;

import java.util.Iterator;

import org.apache.log4j.Level;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

public abstract class LoggingMockingTestCase extends MockingTestCase {
  private String loggerName;
  private LogUtil.LoggerSettings loggerSettings;
  private java.util.Collection<LoggingEvent> loggedEvents;

  protected final void assertLogMessageContains(String needle, Level level) {
    LoggingEvent hit = null;
    Iterator<LoggingEvent> iter = loggedEvents.iterator();
    while (hit == null && iter.hasNext()) {
      LoggingEvent event = iter.next();
      if (event.getRenderedMessage().contains(needle)) {
        if (level == null || level.equals(event.getLevel())) {
          hit = event;
        }
      }
    }
    assertNotNull("Could not find log message containing '" + needle + "'",
        hit);
    assertTrue("Could not remove log message containing '" + needle + "'",
        loggedEvents.remove(hit));
  }

  protected final void assertLogMessageContains(String needle) {
    assertLogMessageContains(needle, null);
  }

  protected final void assertLogThrowableMessageContains(String needle) {
    LoggingEvent hit = null;
    Iterator<LoggingEvent> iter = loggedEvents.iterator();
    while (hit == null && iter.hasNext()) {
      LoggingEvent event = iter.next();

      if (event.getThrowableInformation().getThrowable().toString()
          .contains(needle)) {
        hit = event;
      }
    }
    assertNotNull("Could not find log message with a Throwable containing '"
        + needle + "'", hit);
    assertTrue("Could not remove log message with a Throwable containing '"
        + needle + "'", loggedEvents.remove(hit));
  }

  // As the PowerMock runner does not pass through runTest, we inject log
  // verification through @After
  @After
  public final void assertNoUnassertedLogEvents() {
    if (loggedEvents.size() > 0) {
      LoggingEvent event = loggedEvents.iterator().next();
      String msg = "Found untreated logged events. First one is:\n";
      msg += event.getRenderedMessage();
      if (event.getThrowableInformation() != null) {
        msg += "\n" + event.getThrowableInformation().getThrowable();
      }
      fail(msg);
    }
  }

  @Override
  public void setUp() throws Exception {
    super.setUp();
    loggedEvents = Lists.newArrayList();

    // The logger we're interested is class name without the trailing "Test".
    // While this is not the most general approach it is sufficient for now,
    // and we can improve later to allow tests to specify which loggers are
    // to check.
    loggerName = this.getClass().getCanonicalName();
    loggerName = loggerName.substring(0, loggerName.length()-4);
    loggerSettings = LogUtil.logToCollection(loggerName, loggedEvents);
  }

  @Override
  protected void runTest() throws Throwable {
    super.runTest();
    // Plain JUnit runner does not pick up @After, so we add it here
    // explicitly. Note, that we cannot put this into tearDown, as failure
    // to verify mocks would bail out and might leave open resources from
    // subclasses open.
    assertNoUnassertedLogEvents();
  }

  @Override
  public void tearDown() throws Exception {
    if (loggerName != null && loggerSettings != null) {
      Logger logger = LogManager.getLogger(loggerName);
      loggerSettings.pushOntoLogger(logger);
    }
    super.tearDown();
  }
}
