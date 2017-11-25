/*
*
* Licensed to the Apache Software Foundation (ASF) under one
* or more contributor license agreements.  See the NOTICE file
* distributed with this work for additional information
* regarding copyright ownership.  The ASF licenses this file
* to you under the Apache License, Version 2.0 (the
* "License"); you may not use this file except in compliance
* with the License.  You may obtain a copy of the License at
*
*     http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*/
package org.apache.hadoop.hbase.client.lite.impl;

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;

public class HConstants {
	 /**
	  * Default character set used for column families, qualifiers, etc.
	  */
	 public static final Charset DEF_CHARSET = StandardCharsets.UTF_8;

	  /** delimiter used between portions of a region name */
	  public static final int DELIMITER = ',';
	  
	  /**
	   * An empty instance.
	   */
	  public static final byte [] EMPTY_BYTE_ARRAY = new byte [0];

	  /**
	   * Used by scanners, etc when they want to start at the beginning of a region
	   */
	  public static final byte [] EMPTY_START_ROW = EMPTY_BYTE_ARRAY;

	  /**
	   * Last row in a table.
	   */
	  public static final byte [] EMPTY_END_ROW = EMPTY_START_ROW;

	  /**
	    * Used by scanners and others when they're trying to detect the end of a
	    * table
	    */
	  public static final byte [] LAST_ROW = EMPTY_BYTE_ARRAY;

	  /**
	   * Max length a row can have because of the limitation in TFile.
	   */
	  public static final int MAX_ROW_LENGTH = Short.MAX_VALUE;

	  /**
	   * Timestamp to use when we want to refer to the latest cell.
	   * This is the timestamp sent by clients when no timestamp is specified on
	   * commit.
	   */
	  public static final long LATEST_TIMESTAMP = Long.MAX_VALUE;

	  /**
	   * Timestamp to use when we want to refer to the oldest cell.
	   * Special! Used in fake Cells only. Should never be the timestamp on an actual Cell returned to
	   * a client.
	   * @deprecated Should not be public since hbase-1.3.0. For internal use only. Move internal to
	   * Scanners flagged as special timestamp value never to be returned as timestamp on a Cell.
	   */
	  @Deprecated
	  public static final long OLDEST_TIMESTAMP = Long.MIN_VALUE;

	  /**
	   * LATEST_TIMESTAMP in bytes form
	   */
	  public static final byte [] LATEST_TIMESTAMP_BYTES = {
	    // big-endian
	    (byte) (LATEST_TIMESTAMP >>> 56),
	    (byte) (LATEST_TIMESTAMP >>> 48),
	    (byte) (LATEST_TIMESTAMP >>> 40),
	    (byte) (LATEST_TIMESTAMP >>> 32),
	    (byte) (LATEST_TIMESTAMP >>> 24),
	    (byte) (LATEST_TIMESTAMP >>> 16),
	    (byte) (LATEST_TIMESTAMP >>> 8),
	    (byte) LATEST_TIMESTAMP,
	  };

	  public static final String VERSIONS = "VERSIONS";

	  /** Maximum value length, enforced on KeyValue construction */
	  public static final int MAXIMUM_VALUE_LENGTH = Integer.MAX_VALUE - 1;	  
}
