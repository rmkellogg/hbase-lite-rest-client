/**
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
package org.apache.hadoop.hbase.client.lite;

import java.math.BigDecimal;
import java.util.NavigableMap;

public interface Result {
	  /**
	   * Method for retrieving the row key that corresponds to
	   * the row from which this Result was created.
	   * @return row
	   */
	  byte [] getRow(); 
	  
	  /**
	   * Get the latest version of the specified column as a byte array.
	   * Note: this call clones the value content of the hosting Cell. 
	   * @param family family name
	   * @param qualifier column qualifier
	   * @return value of latest version of column, null if none found
	   */
	  byte[] getValue(byte [] family, byte [] qualifier);

	  /**
	   * Get the latest version of the specified column as a String (UTF-8).
	   * Note: this call clones the value content of the hosting Cell. 
	   * @param family family name
	   * @param qualifier column qualifier
	   * @param defaultValue Value returned if value does not exist.
	   * @return value of latest version of column, defaultValue if none found
	   */
	  String getStringValue(String family, String qualifier, String defaultValue);

	  /**
	   * Get the latest version of the specified column as a boolean.
	   * Note: this call clones the value content of the hosting Cell. 
	   * @param family family name
	   * @param qualifier column qualifier
	   * @param defaultValue Value returned if value does not exist.
	   * @return value of latest version of column, defaultValue if none found
	   */
	  boolean getBooleanValue(String family, String qualifier, boolean defaultValue);

	  /**
	   * Get the latest version of the specified column as a float.
	   * Note: this call clones the value content of the hosting Cell. 
	   * @param family family name
	   * @param qualifier column qualifier
	   * @param defaultValue Value returned if value does not exist.
	   * @return value of latest version of column, defaultValue if none found
	   */
	  float getFloatValue(String family, String qualifier, float defaultValue);
	    
	  /**
	   * Get the latest version of the specified column as a double.
	   * Note: this call clones the value content of the hosting Cell. 
	   * @param family family name
	   * @param qualifier column qualifier
	   * @param defaultValue Value returned if value does not exist.
	   * @return value of latest version of column, defaultValue if none found
	   */
	  double getDoubleValue(String family, String qualifier, double defaultValue);
	
	  /**
	   * Get the latest version of the specified column as a BigDecimal.
	   * Note: this call clones the value content of the hosting Cell. 
	   * @param family family name
	   * @param qualifier column qualifier
	   * @param defaultValue Value returned if value does not exist.
	   * @return value of latest version of column, defaultValue if none found
	   */
	  BigDecimal getBigDecimal(String family, String qualifier, BigDecimal defaultValue);
	
	  /**
	   * Get the latest version of the specified column as a byte.
	   * Note: this call clones the value content of the hosting Cell. 
	   * @param family family name
	   * @param qualifier column qualifier
	   * @param defaultValue Value returned if value does not exist.
	   * @return value of latest version of column, defaultValue if none found
	   */
	  byte getByteValue(String family, String qualifier, byte defaultValue);
	
	  /**
	   * Get the latest version of the specified column as a int.
	   * Note: this call clones the value content of the hosting Cell. 
	   * @param family family name
	   * @param qualifier column qualifier
	   * @param defaultValue Value returned if value does not exist.
	   * @return value of latest version of column, defaultValue if none found
	   */
	  int getIntegerValue(String family, String qualifier, int defaultValue);
	
	  /**
	   * Get the latest version of the specified column as a short.
	   * Note: this call clones the value content of the hosting Cell. 
	   * @param family family name
	   * @param qualifier column qualifier
	   * @param defaultValue Value returned if value does not exist.
	   * @return value of latest version of column, defaultValue if none found
	   */
	  short getShortValue(String family, String qualifier, short defaultValue);
	
	  /**
	   * Get the latest version of the specified column as a double.
	   * Note: this call clones the value content of the hosting Cell. 
	   * @param family family name
	   * @param qualifier column qualifier
	   * @param defaultValue Value returned if value does not exist.
	   * @return value of latest version of column, defaultValue if none found
	   */
	  long getDoubleValue(String family, String qualifier, long defaultValue);

	  /**
	   * Checks if the specified column contains a non-empty value (not a zero-length byte array).
	   *
	   * @param family family name
	   * @param qualifier column qualifier
	   *
	   * @return whether or not a latest value exists and is not empty
	   */
	  boolean containsNonEmptyColumn(byte [] family, byte [] qualifier);

	  /**
	   * Checks if the specified column contains a non-empty value (not a zero-length byte array).
	   *
	   * @param family family name
	   * @param qualifier column qualifier
	   *
	   * @return whether or not a latest value exists and is not empty
	   */
	  boolean containsNonEmptyColumn(String family, String qualifier);

	  /**
	   * Checks if the specified column contains an empty value (a zero-length byte array).
	   *
	   * @param family family name
	   * @param qualifier column qualifier
	   *
	   * @return whether or not a latest value exists and is empty
	   */
	  boolean containsEmptyColumn(byte [] family, byte [] qualifier);

	  /**
	   * Checks if the specified column contains an empty value (a zero-length byte array).
	   *
	   * @param family family name
	   * @param qualifier column qualifier
	   *
	   * @return whether or not a latest value exists and is empty
	   */
	  boolean containsEmptyColumn(String family, String qualifier);

	  /**
	   * Checks for existence of a value for the specified column (empty or not).
	   *
	   * @param family family name
	   * @param qualifier column qualifier
	   *
	   * @return true if at least one value exists in the result, false if not
	   */
	  boolean containsColumn(byte [] family, byte [] qualifier);	  

	  /**
	   * Checks for existence of a value for the specified column (empty or not).
	   *
	   * @param family family name
	   * @param qualifier column qualifier
	   *
	   * @return true if at least one value exists in the result, false if not
	   */
	  boolean containsColumn(String family, String qualifier);	  

	  /**
	   * Map of families to all versions of its qualifiers and values.
	   * <p>
	   * Returns a three level Map of the form:
	   * <code>Map&amp;family,Map&lt;qualifier,Map&lt;timestamp,value&gt;&gt;&gt;</code>
	   * <p>
	   * Note: All other map returning methods make use of this map internally.
	   * @return map from families to qualifiers to versions
	   */
	  NavigableMap<byte[], NavigableMap<byte[], NavigableMap<Long, byte[]>>> getMap();
	  
	  /**
	   * Map of families to their most recent qualifiers and values.
	   * <p>
	   * Returns a two level Map of the form: <code>Map&amp;family,Map&lt;qualifier,value&gt;&gt;</code>
	   * <p>
	   * The most recent version of each qualifier will be used.
	   * @return map from families to qualifiers and value
	   */
	  NavigableMap<byte[], NavigableMap<byte[], byte[]>> getNoVersionMap();	  
	  
	  /**
	   * Map of qualifiers to values.
	   * <p>
	   * Returns a Map of the form: <code>Map&lt;qualifier,value&gt;</code>
	   * @param family column family to get
	   * @return map of qualifiers to values
	   */
	  NavigableMap<byte[], byte[]> getFamilyMap(byte [] family);
	  
	  /**
	   * Check if the underlying cells arrays is empty or not
	   * @return true if empty
	   */
	  boolean isEmpty();
	  
	  /**
	   * @return the size of the underlying cells arrau
	   */
	  int size();
}
