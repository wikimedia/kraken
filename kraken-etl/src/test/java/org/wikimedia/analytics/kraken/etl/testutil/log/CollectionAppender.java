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

import com.google.common.collect.Lists;

import org.apache.log4j.AppenderSkeleton;
import org.apache.log4j.spi.LoggingEvent;

import java.util.Collection;
import java.util.LinkedList;

/**
 * Log4j appender that logs into a list
 */
public class CollectionAppender extends AppenderSkeleton {
  private Collection<LoggingEvent> events;

  public CollectionAppender() {
    events = new LinkedList<LoggingEvent>();
  }

  public CollectionAppender(Collection<LoggingEvent> events) {
    this.events = events;
  }

  @Override
  public boolean requiresLayout() {
    return false;
  }

  @Override
  protected void append(LoggingEvent event) {
    if (! events.add(event)) {
      throw new RuntimeException("Could not append event " + event);
    }
  }

  @Override
  public void close() {
  }

  public Collection<LoggingEvent> getLoggedEvents() {
    return Lists.newLinkedList(events);
  }
}
