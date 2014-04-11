/**
 *Copyright (C) 2012-2013  Wikimedia Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.wikimedia.analytics.kraken.exceptions;

// TODO: Auto-generated Javadoc
/**
 * The Class MalformedFunnelException.
 */
public class MalformedFunnelException extends Exception {

	/** The Constant serialVersionUID. */
	private static final long serialVersionUID = 1L;

	/**
	 * The Constructor.
	 */
	public MalformedFunnelException() {
	}

	/**
	 * The Constructor.
	 *
	 * @param message the message
	 */
	public MalformedFunnelException(String message) {
		super(message);
	}

	/**
	 * The Constructor.
	 *
	 * @param cause the cause
	 */
	public MalformedFunnelException(Throwable cause) {
		super(cause);
	}

	/**
	 * The Constructor.
	 *
	 * @param message the message
	 * @param cause the cause
	 */
	public MalformedFunnelException(String message, Throwable cause) {
		super(message, cause);
	}

}
