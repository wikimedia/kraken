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

import junit.framework.TestCase;

import org.easymock.EasyMock;
import org.easymock.IMocksControl;
import org.junit.After;
import org.junit.runner.RunWith;
import org.powermock.api.easymock.PowerMock;
import org.powermock.modules.junit4.PowerMockRunner;

import java.util.ArrayList;
import java.util.Collection;

/**
 * Test case with some support for automatically verifying mocks.
 */
public abstract class MockingTestCase extends TestCase {
  private Collection<Object> mocks;
  private Collection<IMocksControl> mockControls;
  private boolean mocksReplayed;
  private boolean usePowerMock;

  /**
   * Create and register a mock control.
   *
   * @return The mock control instance.
   */
  protected final IMocksControl createMockControl() {
    IMocksControl mockControl = EasyMock.createControl();
    assertTrue("Adding mock control failed", mockControls.add(mockControl));
    return mockControl;
  }

  /**
   * Create and register a mock.
   *
   * Creates a mock and registers it in the list of created mocks, so it gets
   * treated automatically upon {@code replay} and {@code verify};
   * @param toMock The class to create a mock for.
   * @return The mock instance.
   */
  protected final <T> T createMock(Class<T> toMock) {
    return createMock(toMock, null);
  }

  /**
   * Create a mock for a mock control and register a mock.
   *
   * Creates a mock and registers it in the list of created mocks, so it gets
   * treated automatically upon {@code replay} and {@code verify};
   * @param toMock The class to create a mock for.
   * @param control The mock control to create the mock on. If null, do not use
   *    a specific control.
   * @return The mock instance.
   */
  protected final <T> T createMock(Class<T> toMock, IMocksControl control) {
    assertFalse("Mocks have already been set to replay", mocksReplayed);
    final T mock;
    if (control == null) {
      if (usePowerMock) {
        mock = PowerMock.createMock(toMock);
      } else {
        mock = EasyMock.createMock(toMock);
      }
      assertTrue("Adding " + toMock.getName() + " mock failed",
          mocks.add(mock));
    } else {
      mock = control.createMock(toMock);
    }
    return mock;
  }

  /**
   * Set all registered mocks to replay
   */
  protected final void replayMocks() {
    assertFalse("Mocks have already been set to replay", mocksReplayed);
    if (usePowerMock) {
      PowerMock.replayAll();
    } else {
      EasyMock.replay(mocks.toArray());
    }
    for (IMocksControl mockControl : mockControls) {
      mockControl.replay();
    }
    mocksReplayed = true;
  }

  /**
   * Verify all registered mocks
   *
   * This method is called automatically at the end of a test. Nevertheless,
   * it is safe to also call it beforehand, if this better meets the
   * verification part of a test.
   */
  // As the PowerMock runner does not pass through runTest, we inject mock
  // verification through @After
  @After
  public final void verifyMocks() {
    if (!mocks.isEmpty() || !mockControls.isEmpty()) {
      assertTrue("Created mocks have not been set to replay. Call replayMocks "
          + "within the test", mocksReplayed);
      if (usePowerMock) {
        PowerMock.verifyAll();
      } else {
        EasyMock.verify(mocks.toArray());
      }
      for (IMocksControl mockControl : mockControls) {
        mockControl.verify();
      }
    }
  }

  @Override
  public void setUp() throws Exception {
    super.setUp();

    usePowerMock = false;
    RunWith runWith = this.getClass().getAnnotation(RunWith.class);
    if (runWith != null) {
      usePowerMock = PowerMockRunner.class.isAssignableFrom(runWith.value());
    }

    mocks = new ArrayList<Object>();
    mockControls = new ArrayList<IMocksControl>();
    mocksReplayed = false;
  }

  @Override
  protected void runTest() throws Throwable {
    super.runTest();
    // Plain JUnit runner does not pick up @After, so we add it here
    // explicitly. Note, that we cannot put this into tearDown, as failure
    // to verify mocks would bail out and might leave open resources from
    // subclasses open.
    verifyMocks();
  }
}
