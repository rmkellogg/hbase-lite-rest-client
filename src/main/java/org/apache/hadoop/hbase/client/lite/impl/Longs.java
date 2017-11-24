/*
 * Copyright (C) 2008 The Guava Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.hadoop.hbase.client.lite.impl;

/**
 * Static utility methods pertaining to {@code long} primitives, that are not already found in
 * either {@link Long} or {@link Arrays}.
 * 
 * Copied from Google Guava
 */ 
public class Longs {
	  /**
	   * Compares the two specified {@code long} values. The sign of the value returned is the same as
	   * that of {@code ((Long) a).compareTo(b)}.
	   *
	   * <p><b>Note for Java 7 and later:</b> this method should be treated as deprecated; use the
	   * equivalent {@link Long#compare} method instead.
	   *
	   * @param a the first {@code long} to compare
	   * @param b the second {@code long} to compare
	   * @return a negative value if {@code a} is less than {@code b}; a positive value if {@code a} is
	   *     greater than {@code b}; or zero if they are equal
	   */
	  public static int compare(long a, long b) {
	    return (a < b) ? -1 : ((a > b) ? 1 : 0);
	  }
}
