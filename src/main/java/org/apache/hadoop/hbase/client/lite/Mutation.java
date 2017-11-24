//package org.apache.hadoop.hbase.client.lite.impl;
//
//public class Mutation {
//
//	  /**
//	   * @param row Row to check
//	   * @throws IllegalArgumentException Thrown if <code>row</code> is empty or null or
//	   * &gt; {@link HConstants#MAX_ROW_LENGTH}
//	   * @return <code>row</code>
//	   */
//	  public static byte [] checkRow(final byte [] row) {
//	    return checkRow(row, 0, row == null? 0: row.length);
//	  }
//
//	  /**
//	   * @param row Row to check
//	   * @param offset
//	   * @param length
//	   * @throws IllegalArgumentException Thrown if <code>row</code> is empty or null or
//	   * &gt; {@link HConstants#MAX_ROW_LENGTH}
//	   * @return <code>row</code>
//	   */
//	  public static byte [] checkRow(final byte [] row, final int offset, final int length) {
//	    if (row == null) {
//	      throw new IllegalArgumentException("Row buffer is null");
//	    }
//	    if (length == 0) {
//	      throw new IllegalArgumentException("Row length is 0");
//	    }
//	    if (length > HConstants.MAX_ROW_LENGTH) {
//	      throw new IllegalArgumentException("Row length " + length + " is > " +
//  	        HConstants.MAX_ROW_LENGTH);
//	    }
//	    return row;
//	  }
//}

/*
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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;

import org.apache.hadoop.hbase.client.lite.impl.Bytes;
import org.apache.hadoop.hbase.client.lite.impl.HConstants;
import org.apache.hadoop.hbase.client.lite.impl.KeyValue;

public abstract class Mutation {
  protected byte [] row = null;
  protected long ts = HConstants.LATEST_TIMESTAMP;

  // A Map sorted by column family.
  protected NavigableMap<byte [], List<Cell>> familyMap = new TreeMap<>(Bytes.BYTES_COMPARATOR);

  /**
   * Creates an empty list if one doesn't exist for the given column family
   * or else it returns the associated list of Cell objects.
   *
   * @param family column family
   * @return a list of Cell objects, returns an empty list if one doesn't exist.
   */
  protected List<Cell> getCellList(byte[] family) {
    List<Cell> list = this.familyMap.get(family);
    if (list == null) {
      list = new ArrayList<>();
      this.familyMap.put(family, list);
    }
    return list;
  }

  /*
   * Create a KeyValue with this objects row key and the Put identifier.
   *
   * @return a KeyValue with this objects row key and the Put identifier.
   */
  protected KeyValue createPutKeyValue(byte[] family, byte[] qualifier, long ts, byte[] value) {
    return new KeyValue(this.row, family, qualifier, ts, KeyValue.Type.Put, value);
  }

  /**
   * Compile the column family (i.e. schema) information
   * into a Map. Useful for parsing and aggregation by debugging,
   * logging, and administration tools.
   * @return Map
   */
  public Map<String, Object> getFingerprint() {
    Map<String, Object> map = new HashMap<>();
    List<String> families = new ArrayList<>(this.familyMap.entrySet().size());
    // ideally, we would also include table information, but that information
    // is not stored in each Operation instance.
    map.put("families", families);
    for (Map.Entry<byte [], List<Cell>> entry : this.familyMap.entrySet()) {
      families.add(Bytes.toStringBinary(entry.getKey()));
    }
    return map;
  }

  /**
   * Compile the details beyond the scope of getFingerprint (row, columns,
   * timestamps, etc.) into a Map along with the fingerprinted information.
   * Useful for debugging, logging, and administration tools.
   * @param maxCols a limit on the number of columns output prior to truncation
   * @return Map
   */
  public Map<String, Object> toMap(int maxCols) {
    // we start with the fingerprint map and build on top of it.
    Map<String, Object> map = getFingerprint();
    // replace the fingerprint's simple list of families with a
    // map from column families to lists of qualifiers and kv details
    Map<String, List<Map<String, Object>>> columns = new HashMap<>();
    map.put("families", columns);
    map.put("row", Bytes.toStringBinary(this.row));
    int colCount = 0;
    // iterate through all column families affected
    for (Map.Entry<byte [], List<Cell>> entry : this.familyMap.entrySet()) {
      // map from this family to details for each cell affected within the family
      List<Map<String, Object>> qualifierDetails = new ArrayList<>();
      columns.put(Bytes.toStringBinary(entry.getKey()), qualifierDetails);
      colCount += entry.getValue().size();
      if (maxCols <= 0) {
        continue;
      }
      // add details for each cell
      for (Cell cell: entry.getValue()) {
        if (--maxCols <= 0) {
          continue;
        }
        Map<String, Object> cellMap = cellToStringMap(cell);
        qualifierDetails.add(cellMap);
      }
    }
    map.put("totalColumns", colCount);
    return map;
  }

  private static Map<String, Object> cellToStringMap(Cell c) {
    Map<String, Object> stringMap = new HashMap<>();
    stringMap.put("qualifier", Bytes.toStringBinary(c.getQualifierArray(), c.getQualifierOffset(),
                c.getQualifierLength()));
    stringMap.put("timestamp", c.getTimestamp());
    stringMap.put("vlen", c.getValueLength());
    return stringMap;
  }

  /**
   * Method for retrieving the put's familyMap
   * @return familyMap
   */
  public NavigableMap<byte [], List<Cell>> getFamilyCellMap() {
    return this.familyMap;
  }

  /**
   * Method for setting the put's familyMap
   */
  public Mutation setFamilyCellMap(NavigableMap<byte [], List<Cell>> map) {
    // TODO: Shut this down or move it up to be a Constructor.  Get new object rather than change
    // this internal data member.
    this.familyMap = map;
    return this;
  }

  /**
   * Method to check if the familyMap is empty
   * @return true if empty, false otherwise
   */
  public boolean isEmpty() {
    return familyMap.isEmpty();
  }

  /**
   * Method for retrieving the delete's row
   * @return row
   */
//  @Override
  public byte [] getRow() {
    return this.row;
  }

//  @Override
//  public int compareTo(final Row d) {
//    return Bytes.compareTo(this.getRow(), d.getRow());
//  }

  /**
   * Method for retrieving the timestamp
   * @return timestamp
   */
  public long getTimeStamp() {
    return this.ts;
  }

  /**
   * Number of KeyValues carried by this Mutation.
   * @return the total number of KeyValues
   */
  public int size() {
    int size = 0;
    for (List<Cell> cells : this.familyMap.values()) {
      size += cells.size();
    }
    return size;
  }

  /**
   * @return the number of different families
   */
  public int numFamilies() {
    return familyMap.size();
  }

  /**
   * @param row Row to check
   * @throws IllegalArgumentException Thrown if <code>row</code> is empty or null or
   * &gt; {@link HConstants#MAX_ROW_LENGTH}
   * @return <code>row</code>
   */
//  static byte [] checkRow(final byte [] row) {
  public static byte [] checkRow(final byte [] row) {
    return checkRow(row, 0, row == null? 0: row.length);
  }

  /**
   * @param row Row to check
   * @param offset
   * @param length
   * @throws IllegalArgumentException Thrown if <code>row</code> is empty or null or
   * &gt; {@link HConstants#MAX_ROW_LENGTH}
   * @return <code>row</code>
   */
//  static byte [] checkRow(final byte [] row, final int offset, final int length) {
  public static byte [] checkRow(final byte [] row, final int offset, final int length) {
    if (row == null) {
      throw new IllegalArgumentException("Row buffer is null");
    }
    if (length == 0) {
      throw new IllegalArgumentException("Row length is 0");
    }
    if (length > HConstants.MAX_ROW_LENGTH) {
      throw new IllegalArgumentException("Row length " + length + " is > " +
        HConstants.MAX_ROW_LENGTH);
    }
    return row;
  }
}