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

///package org.apache.hadoop.hbase.client;
package org.apache.hadoop.hbase.client.lite;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NavigableSet;
import java.util.TreeMap;
import java.util.TreeSet;

import org.apache.hadoop.hbase.client.lite.impl.Bytes;
import org.apache.hadoop.hbase.client.lite.impl.ClientUtil;
import org.apache.hadoop.hbase.client.lite.impl.HConstants;

/**
* Used to perform Scan operations.
* <p>
* All operations are identical to {@link Get} with the exception of instantiation. Rather than
* specifying a single row, an optional startRow and stopRow may be defined. If rows are not
* specified, the Scanner will iterate over all rows.
* <p>
* To get all columns from all rows of a Table, create an instance with no constraints; use the
* {@link #Scan()} constructor. To constrain the scan to specific column families, call
* {@link #addFamily(byte[]) addFamily} for each family to retrieve on your Scan instance.
* <p>
* To get specific columns, call {@link #addColumn(byte[], byte[]) addColumn} for each column to
* retrieve.
* <p>
* To only retrieve columns within a specific range of version timestamps, call
* {@link #setTimeRange(long, long) setTimeRange}.
* <p>
* To only retrieve columns with a specific timestamp, call {@link #setTimeStamp(long) setTimestamp}
* .
* <p>
* To limit the number of versions of each column to be returned, call {@link #setMaxVersions(int)
* setMaxVersions}.
* <p>
* To limit the maximum number of values returned for each call to next(), call
* {@link #setBatch(int) setBatch}.
* <p>
*/
public class Scan {
 private byte[] startRow = HConstants.EMPTY_START_ROW;
 private boolean includeStartRow = true;
 private byte[] stopRow  = HConstants.EMPTY_END_ROW;
 private boolean includeStopRow = false;
 private int maxVersions = 1;

 private int storeLimit = -1;

 private long maxResultSize = -1;
 private boolean reversed = false;
 private TimeRange tr = new TimeRange();
 private Map<byte [], NavigableSet<byte []>> familyMap =
   new TreeMap<byte [], NavigableSet<byte []>>(Bytes.BYTES_COMPARATOR);

 /**
  * Create a Scan operation across all rows.
  */
 public Scan() {}

 /**
  * Create a Scan operation starting at the specified row.
  * <p>
  * If the specified row does not exist, the Scanner will start from the next closest row after the
  * specified row.
  * @param startRow row to start scanner at or after
  * @deprecated use {@code new Scan().withStartRow(startRow)} instead.
  */
 public Scan(byte[] startRow) {
   setStartRow(startRow);
 }

 /**
  * Create a Scan operation for the range of rows specified.
  * @param startRow row to start scanner at or after (inclusive)
  * @param stopRow row to stop scanner before (exclusive)
  * @deprecated use {@code new Scan().withStartRow(startRow).withStopRow(stopRow)} instead.
  */
 public Scan(byte[] startRow, byte[] stopRow) {
   setStartRow(startRow);
   setStopRow(stopRow);
 }

 /**
  * Get all columns from the specified family.
  * <p>
  * Overrides previous calls to addColumn for this family.
  * @param family family name
  * @return this
  */
 public Scan addFamily(byte [] family) {
   familyMap.remove(family);
   familyMap.put(family, null);
   return this;
 }

 /**
  * Get the column from the specified family with the specified qualifier.
  * <p>
  * Overrides previous calls to addFamily for this family.
  * @param family family name
  * @param qualifier column qualifier
  * @return this
  */
 public Scan addColumn(byte [] family, byte [] qualifier) {
   NavigableSet<byte []> set = familyMap.get(family);
   if(set == null) {
     set = new TreeSet<>(Bytes.BYTES_COMPARATOR);
     familyMap.put(family, set);
   }
   if (qualifier == null) {
     qualifier = HConstants.EMPTY_BYTE_ARRAY;
   }
   set.add(qualifier);
   return this;
 }

 /**
  * Get versions of columns only within the specified timestamp range,
  * [minStamp, maxStamp).  Note, default maximum versions to return is 1.  If
  * your time range spans more than one version and you want all versions
  * returned, up the number of versions beyond the default.
  * @param minStamp minimum timestamp value, inclusive
  * @param maxStamp maximum timestamp value, exclusive
  * @see #setMaxVersions()
  * @see #setMaxVersions(int)
  * @return this
  */
 public Scan setTimeRange(long minStamp, long maxStamp) throws IOException {
   tr = new TimeRange(minStamp, maxStamp);
   return this;
 }

 /**
  * Get versions of columns with the specified timestamp. Note, default maximum
  * versions to return is 1.  If your time range spans more than one version
  * and you want all versions returned, up the number of versions beyond the
  * defaut.
  * @param timestamp version timestamp
  * @see #setMaxVersions()
  * @see #setMaxVersions(int)
  * @return this
  */
 public Scan setTimeStamp(long timestamp)
 throws IOException {
   try {
     tr = new TimeRange(timestamp, timestamp+1);
   } catch(Exception e) {
     // This should never happen, unless integer overflow or something extremely wrong...
//     LOG.error("TimeRange failed, likely caused by integer overflow. ", e);
     throw e;
   }
   return this;
 }

 /**
  * Set the start row of the scan.
  * <p>
  * If the specified row does not exist, the Scanner will start from the next closest row after the
  * specified row.
  * @param startRow row to start scanner at or after
  * @return this
  * @throws IllegalArgumentException if startRow does not meet criteria for a row key (when length
  *           exceeds {@link HConstants#MAX_ROW_LENGTH})
  * @deprecated use {@link #withStartRow(byte[])} instead. This method may change the inclusive of
  *             the stop row to keep compatible with the old behavior.
  */
 protected Scan setStartRow(byte[] startRow) {
   withStartRow(startRow);
   if (ClientUtil.areScanStartRowAndStopRowEqual(this.startRow, this.stopRow)) {
     // for keeping the old behavior that a scan with the same start and stop row is a get scan.
     this.includeStopRow = true;
   }
   return this;
 }

 /**
  * Set the start row of the scan.
  * <p>
  * If the specified row does not exist, the Scanner will start from the next closest row after the
  * specified row.
  * @param startRow row to start scanner at or after
  * @return this
  * @throws IllegalArgumentException if startRow does not meet criteria for a row key (when length
  *           exceeds {@link HConstants#MAX_ROW_LENGTH})
  */
 public Scan withStartRow(byte[] startRow) {
   return withStartRow(startRow, true);
 }

 /**
  * Set the start row of the scan.
  * <p>
  * If the specified row does not exist, or the {@code inclusive} is {@code false}, the Scanner
  * will start from the next closest row after the specified row.
  * @param startRow row to start scanner at or after
  * @param inclusive whether we should include the start row when scan
  * @return this
  * @throws IllegalArgumentException if startRow does not meet criteria for a row key (when length
  *           exceeds {@link HConstants#MAX_ROW_LENGTH})
  */
 public Scan withStartRow(byte[] startRow, boolean inclusive) {
   if (Bytes.len(startRow) > HConstants.MAX_ROW_LENGTH) {
     throw new IllegalArgumentException("startRow's length must be less than or equal to "
         + HConstants.MAX_ROW_LENGTH + " to meet the criteria" + " for a row key.");
   }
   this.startRow = startRow;
   this.includeStartRow = inclusive;
   return this;
 }

 /**
  * Set the stop row of the scan.
  * <p>
  * The scan will include rows that are lexicographically less than the provided stopRow.
  * <p>
  * <b>Note:</b> When doing a filter for a rowKey <u>Prefix</u> use
  * {@link #setRowPrefixFilter(byte[])}. The 'trailing 0' will not yield the desired result.
  * </p>
  * @param stopRow row to end at (exclusive)
  * @return this
  * @throws IllegalArgumentException if stopRow does not meet criteria for a row key (when length
  *           exceeds {@link HConstants#MAX_ROW_LENGTH})
  * @deprecated use {@link #withStartRow(byte[])} instead. This method may change the inclusive of
  *             the stop row to keep compatible with the old behavior.
  */
 protected Scan setStopRow(byte[] stopRow) {
   withStopRow(stopRow);
   if (ClientUtil.areScanStartRowAndStopRowEqual(this.startRow, this.stopRow)) {
     // for keeping the old behavior that a scan with the same start and stop row is a get scan.
     this.includeStopRow = true;
   }
   return this;
 }

 /**
  * Set the stop row of the scan.
  * <p>
  * The scan will include rows that are lexicographically less than the provided stopRow.
  * <p>
  * <b>Note:</b> When doing a filter for a rowKey <u>Prefix</u> use
  * {@link #setRowPrefixFilter(byte[])}. The 'trailing 0' will not yield the desired result.
  * </p>
  * @param stopRow row to end at (exclusive)
  * @return this
  * @throws IllegalArgumentException if stopRow does not meet criteria for a row key (when length
  *           exceeds {@link HConstants#MAX_ROW_LENGTH})
  */
 public Scan withStopRow(byte[] stopRow) {
   return withStopRow(stopRow, false);
 }

 /**
  * Set the stop row of the scan.
  * <p>
  * The scan will include rows that are lexicographically less than (or equal to if
  * {@code inclusive} is {@code true}) the provided stopRow.
  * @param stopRow row to end at
  * @param inclusive whether we should include the stop row when scan
  * @return this
  * @throws IllegalArgumentException if stopRow does not meet criteria for a row key (when length
  *           exceeds {@link HConstants#MAX_ROW_LENGTH})
  */
 public Scan withStopRow(byte[] stopRow, boolean inclusive) {
   if (Bytes.len(stopRow) > HConstants.MAX_ROW_LENGTH) {
     throw new IllegalArgumentException("stopRow's length must be less than or equal to "
         + HConstants.MAX_ROW_LENGTH + " to meet the criteria" + " for a row key.");
   }
   this.stopRow = stopRow;
   this.includeStopRow = inclusive;
   return this;
 }

 /**
  * <p>Set a filter (using stopRow and startRow) so the result set only contains rows where the
  * rowKey starts with the specified prefix.</p>
  * <p>This is a utility method that converts the desired rowPrefix into the appropriate values
  * for the startRow and stopRow to achieve the desired result.</p>
  * <p>This can safely be used in combination with setFilter.</p>
  * <p><b>NOTE: Doing a {@link #setStartRow(byte[])} and/or {@link #setStopRow(byte[])}
  * after this method will yield undefined results.</b></p>
  * @param rowPrefix the prefix all rows must start with. (Set <i>null</i> to remove the filter.)
  * @return this
  */
 public Scan setRowPrefixFilter(byte[] rowPrefix) {
   if (rowPrefix == null) {
     setStartRow(HConstants.EMPTY_START_ROW);
     setStopRow(HConstants.EMPTY_END_ROW);
   } else {
     this.setStartRow(rowPrefix);
     this.setStopRow(calculateTheClosestNextRowKeyForPrefix(rowPrefix));
   }
   return this;
 }

 /**
  * <p>When scanning for a prefix the scan should stop immediately after the the last row that
  * has the specified prefix. This method calculates the closest next rowKey immediately following
  * the given rowKeyPrefix.</p>
  * <p><b>IMPORTANT: This converts a rowKey<u>Prefix</u> into a rowKey</b>.</p>
  * <p>If the prefix is an 'ASCII' string put into a byte[] then this is easy because you can
  * simply increment the last byte of the array.
  * But if your application uses real binary rowids you may run into the scenario that your
  * prefix is something like:</p>
  * &nbsp;&nbsp;&nbsp;<b>{ 0x12, 0x23, 0xFF, 0xFF }</b><br/>
  * Then this stopRow needs to be fed into the actual scan<br/>
  * &nbsp;&nbsp;&nbsp;<b>{ 0x12, 0x24 }</b> (Notice that it is shorter now)<br/>
  * This method calculates the correct stop row value for this usecase.
  *
  * @param rowKeyPrefix the rowKey<u>Prefix</u>.
  * @return the closest next rowKey immediately following the given rowKeyPrefix.
  */
 private byte[] calculateTheClosestNextRowKeyForPrefix(byte[] rowKeyPrefix) {
   // Essentially we are treating it like an 'unsigned very very long' and doing +1 manually.
   // Search for the place where the trailing 0xFFs start
   int offset = rowKeyPrefix.length;
   while (offset > 0) {
     if (rowKeyPrefix[offset - 1] != (byte) 0xFF) {
       break;
     }
     offset--;
   }

   if (offset == 0) {
     // We got an 0xFFFF... (only FFs) stopRow value which is
     // the last possible prefix before the end of the table.
     // So set it to stop at the 'end of the table'
     return HConstants.EMPTY_END_ROW;
   }

   // Copy the right length of the original
   byte[] newStopRow = Arrays.copyOfRange(rowKeyPrefix, 0, offset);
   // And increment the last one
   newStopRow[newStopRow.length - 1]++;
   return newStopRow;
 }

 /**
  * Get all available versions.
  * @return this
  */
 public Scan readAllVersions() {
   this.maxVersions = Integer.MAX_VALUE;
   return this;
 }

 /**
  * Get up to the specified number of versions of each column.
  * @param versions specified number of versions for each column
  * @return this
  */
 public Scan readVersions(int versions) {
   this.maxVersions = versions;
   return this;
 }

 /**
  * Set the maximum number of values to return per row per Column Family
  * @param limit the maximum number of values returned / row / CF
  */
 public Scan setMaxResultsPerColumnFamily(int limit) {
   this.storeLimit = limit;
   return this;
 }

 /**
  * @return the maximum result size in bytes. See {@link #setMaxResultSize(long)}
  */
 public long getMaxResultSize() {
   return maxResultSize;
 }

 /**
  * Set the maximum result size. The default is -1; this means that no specific
  * maximum result size will be set for this scan, and the global configured
  * value will be used instead. (Defaults to unlimited).
  *
  * @param maxResultSize The maximum result size in bytes.
  */
 public Scan setMaxResultSize(long maxResultSize) {
   this.maxResultSize = maxResultSize;
   return this;
 }

 /**
  * Setting the familyMap
  * @param familyMap map of family to qualifier
  * @return this
  */
 public Scan setFamilyMap(Map<byte [], NavigableSet<byte []>> familyMap) {
   this.familyMap = familyMap;
   return this;
 }

 /**
  * Getting the familyMap
  * @return familyMap
  */
 public Map<byte [], NavigableSet<byte []>> getFamilyMap() {
   return this.familyMap;
 }

 /**
  * @return the number of families in familyMap
  */
 public int numFamilies() {
   if(hasFamilies()) {
     return this.familyMap.size();
   }
   return 0;
 }

 /**
  * @return true if familyMap is non empty, false otherwise
  */
 public boolean hasFamilies() {
   return !this.familyMap.isEmpty();
 }

 /**
  * @return the keys of the familyMap
  */
 public byte[][] getFamilies() {
   if(hasFamilies()) {
     return this.familyMap.keySet().toArray(new byte[0][0]);
   }
   return null;
 }

 /**
  * @return the startrow
  */
 public byte [] getStartRow() {
   return this.startRow;
 }

 /**
  * @return if we should include start row when scan
  */
 public boolean includeStartRow() {
   return includeStartRow;
 }

 /**
  * @return the stoprow
  */
 public byte[] getStopRow() {
   return this.stopRow;
 }

 /**
  * @return if we should include stop row when scan
  */
 public boolean includeStopRow() {
   return includeStopRow;
 }

 /**
  * @return the max number of versions to fetch
  */
 public int getMaxVersions() {
   return this.maxVersions;
 }

 /**
  * @return maximum number of values to return per row per CF
  */
 public int getMaxResultsPerColumnFamily() {
   return this.storeLimit;
 }

 /**
  * @return TimeRange
  */
 public TimeRange getTimeRange() {
   return this.tr;
 }

 /**
  * Set whether this scan is a reversed one
  * <p>
  * This is false by default which means forward(normal) scan.
  *
  * @param reversed if true, scan will be backward order
  * @return this
  */
 public Scan setReversed(boolean reversed) {
   this.reversed = reversed;
   return this;
 }

 /**
  * Get whether this scan is a reversed one.
  * @return true if backward scan, false if forward(default) scan
  */
 public boolean isReversed() {
   return reversed;
 }

 /**
  * Compile the table and column family (i.e. schema) information
  * into a String. Useful for parsing and aggregation by debugging,
  * logging, and administration tools.
  * @return Map
  */
 public Map<String, Object> getFingerprint() {
   Map<String, Object> map = new HashMap<>();
   List<String> families = new ArrayList<>();
   if(this.familyMap.isEmpty()) {
     map.put("families", "ALL");
     return map;
   } else {
     map.put("families", families);
   }
   for (Map.Entry<byte [], NavigableSet<byte[]>> entry :
       this.familyMap.entrySet()) {
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
   // start with the fingerpring map and build on top of it
   Map<String, Object> map = getFingerprint();
   // map from families to column list replaces fingerprint's list of families
   Map<String, List<String>> familyColumns = new HashMap<>();
   map.put("families", familyColumns);
   // add scalar information first
   map.put("startRow", Bytes.toStringBinary(this.startRow));
   map.put("stopRow", Bytes.toStringBinary(this.stopRow));
   map.put("maxVersions", this.maxVersions);
   map.put("maxResultSize", this.maxResultSize);
   List<Long> timeRange = new ArrayList<>(2);
   timeRange.add(this.tr.getMin());
   timeRange.add(this.tr.getMax());
   map.put("timeRange", timeRange);
   int colCount = 0;
   // iterate through affected families and list out up to maxCols columns
   for (Map.Entry<byte [], NavigableSet<byte[]>> entry :
     this.familyMap.entrySet()) {
     List<String> columns = new ArrayList<>();
     familyColumns.put(Bytes.toStringBinary(entry.getKey()), columns);
     if(entry.getValue() == null) {
       colCount++;
       --maxCols;
       columns.add("ALL");
     } else {
       colCount += entry.getValue().size();
       if (maxCols <= 0) {
         continue;
       }
       for (byte [] column : entry.getValue()) {
         if (--maxCols <= 0) {
           continue;
         }
         columns.add(Bytes.toStringBinary(column));
       }
     }
   }
   map.put("totalColumns", colCount);
   return map;
 }
}
