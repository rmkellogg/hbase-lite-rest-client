package org.apache.hadoop.hbase.client.lite;

import java.util.NavigableMap;

public interface Result {
	  /**
	   * Method for retrieving the row key that corresponds to
	   * the row from which this Result was created.
	   * @return row
	   */
	  byte [] getRow(); 
	  
	  /**
	   * Get the latest version of the specified column.
	   * Note: this call clones the value content of the hosting Cell. See
	   * {@link #getValueAsByteBuffer(byte[], byte[])}, etc., or {@link #listCells()} if you would
	   * avoid the cloning.
	   * @param family family name
	   * @param qualifier column qualifier
	   * @return value of latest version of column, null if none found
	   */
	  byte[] getValue(byte [] family, byte [] qualifier);
	  
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
