/**
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

//package org.apache.hadoop.hbase.client;
package org.apache.hadoop.hbase.client.lite;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NavigableSet;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

import org.apache.hadoop.hbase.client.lite.impl.Bytes;
import org.apache.hadoop.hbase.client.lite.impl.HConstants;

/**
* Used to perform Get operations on a single row.
* <p>
* To get everything for a row, instantiate a Get object with the row to get.
* To further narrow the scope of what to Get, use the methods below.
* <p>
* To get all columns from specific families, execute {@link #addFamily(byte[]) addFamily}
* for each family to retrieve.
* <p>
* To get specific columns, execute {@link #addColumn(byte[], byte[]) addColumn}
* for each column to retrieve.
* <p>
* To only retrieve columns within a specific range of version timestamps,
* execute {@link #setTimeRange(long, long) setTimeRange}.
* <p>
* To only retrieve columns with a specific timestamp, execute
* {@link #setTimeStamp(long) setTimestamp}.
* <p>
* To limit the number of versions of each column to be returned, execute
* {@link #setMaxVersions(int) setMaxVersions}.
*/
public class Get {
 private byte [] row = null;
 private int maxVersions = 1;
 private TimeRange tr = new TimeRange();
 private Map<byte [], NavigableSet<byte []>> familyMap = new TreeMap<>(Bytes.BYTES_COMPARATOR);

 /**
  * Create a Get operation for the specified row.
  * <p>
  * If no further operations are done, this will get the latest version of
  * all columns in all families of the specified row.
  * @param row row key
  */
 public Get(String row) {
   byte[] data = row.getBytes(HConstants.DEF_CHARSET); 
   Mutation.checkRow(data);
   this.row = data;
 }
 
 /**
  * Create a Get operation for the specified row.
  * <p>
  * If no further operations are done, this will get the latest version of
  * all columns in all families of the specified row.
  * @param row row key
  */
 public Get(byte [] row) {
   Mutation.checkRow(row);
   this.row = row;
 }


 /**
  * Get all columns from the specified family.
  * <p>
  * Overrides previous calls to addColumn for this family.
  * @param family family name
  * @return the Get object
  */
 public Get addFamily(String family) {
	 return addFamily(family.getBytes(HConstants.DEF_CHARSET));
 }
 
 /**
  * Get all columns from the specified family.
  * <p>
  * Overrides previous calls to addColumn for this family.
  * @param family family name
  * @return the Get object
  */
 public Get addFamily(byte [] family) {
   familyMap.remove(family);
   familyMap.put(family, null);
   return this;
 }

 /**
  * Get the column from the specific family with the specified qualifier.
  * <p>
  * Overrides previous calls to addFamily for this family.
  * @param family family name
  * @param qualifier column qualifier
  * @return the Get objec
  */
 public Get addColumn(String family, String qualifier) {
	 return addColumn(family.getBytes(HConstants.DEF_CHARSET), qualifier.getBytes(HConstants.DEF_CHARSET));
 }
 
 /**
  * Get the column from the specific family with the specified qualifier.
  * <p>
  * Overrides previous calls to addFamily for this family.
  * @param family family name
  * @param qualifier column qualifier
  * @return the Get objec
  */
 public Get addColumn(byte [] family, byte [] qualifier) {
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
  * [minStamp, maxStamp).
  * @param minStamp minimum timestamp value, inclusive
  * @param maxStamp maximum timestamp value, exclusive
  * @throws IOException
  * @return this for invocation chaining
  */
 public Get setTimeRange(long minStamp, long maxStamp) throws IOException {
   tr = new TimeRange(minStamp, maxStamp);
   return this;
 }

 /**
  * Get all available versions.
  * @return this for invocation chaining
  */
 public Get readAllVersions() {
   this.maxVersions = Integer.MAX_VALUE;
   return this;
 }

 /**
  * Get up to the specified number of versions of each column.
  * @param versions specified number of versions for each column
  * @throws IOException if invalid number of versions
  * @return this for invocation chaining
  */
 public Get readVersions(int versions) throws IOException {
   if (versions <= 0) {
     throw new IOException("versions must be positive");
   }
   this.maxVersions = versions;
   return this;
 }

 /**
  * Method for retrieving the get's row
  * @return row
  */
 public byte [] getRow() {
   return this.row;
 }

 /**
  * Method for retrieving the get's maximum number of version
  * @return the maximum number of version to fetch for this get
  */
 public int getMaxVersions() {
   return this.maxVersions;
 }

 /**
  * Method for retrieving the get's TimeRange
  * @return timeRange
  */
 public TimeRange getTimeRange() {
   return this.tr;
 }

 /**
  * Method for retrieving the keys in the familyMap
  * @return keys in the current familyMap
  */
 public Set<byte[]> familySet() {
   return this.familyMap.keySet();
 }

 /**
  * Method for retrieving the number of families to get from
  * @return number of families
  */
 public int numFamilies() {
   return this.familyMap.size();
 }

 /**
  * Method for checking if any families have been inserted into this Get
  * @return true if familyMap is non empty false otherwise
  */
 public boolean hasFamilies() {
   return !this.familyMap.isEmpty();
 }

 /**
  * Method for retrieving the get's familyMap
  * @return familyMap
  */
 public Map<byte[],NavigableSet<byte[]>> getFamilyMap() {
   return this.familyMap;
 }

 /**
  * Compile the table and column family (i.e. schema) information
  * into a String. Useful for parsing and aggregation by debugging,
  * logging, and administration tools.
  * @return Map
  */
 public Map<String, Object> getFingerprint() {
   Map<String, Object> map = new HashMap<>();
   List<String> families = new ArrayList<>(this.familyMap.entrySet().size());
   map.put("families", families);
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
   // we start with the fingerprint map and build on top of it.
   Map<String, Object> map = getFingerprint();
   // replace the fingerprint's simple list of families with a
   // map from column families to lists of qualifiers and kv details
   Map<String, List<String>> columns = new HashMap<>();
   map.put("families", columns);
   // add scalar information first
   map.put("row", Bytes.toStringBinary(this.row));
   map.put("maxVersions", this.maxVersions);
   List<Long> timeRange = new ArrayList<>(2);
   timeRange.add(this.tr.getMin());
   timeRange.add(this.tr.getMax());
   map.put("timeRange", timeRange);
   int colCount = 0;
   // iterate through affected families and add details
   for (Map.Entry<byte [], NavigableSet<byte[]>> entry :
     this.familyMap.entrySet()) {
     List<String> familyList = new ArrayList<>();
     columns.put(Bytes.toStringBinary(entry.getKey()), familyList);
     if(entry.getValue() == null) {
       colCount++;
       --maxCols;
       familyList.add("ALL");
     } else {
       colCount += entry.getValue().size();
       if (maxCols <= 0) {
         continue;
       }
       for (byte [] column : entry.getValue()) {
         if (--maxCols <= 0) {
           continue;
         }
         familyList.add(Bytes.toStringBinary(column));
       }
     }
   }
   map.put("totalColumns", colCount);
   return map;
 }
}
