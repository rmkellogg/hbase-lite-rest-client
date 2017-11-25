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

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;

import org.apache.hadoop.hbase.client.lite.Cell;
import org.apache.hadoop.hbase.client.lite.Get;
import org.apache.hadoop.hbase.client.lite.Result;

/**
* Single row result of a {@link Get} or {@link Scan} query.<p>
*
* This class is <b>NOT THREAD SAFE</b>.<p>
*
* Convenience methods are available that return various {@link Map}
* structures and values directly.<p>
*
* To get a complete mapping of all cells in the Result, which can include
* multiple families and multiple versions, use {@link #getMap()}.<p>
*
* To get a mapping of each family to its columns (qualifiers and values),
* including only the latest version of each, use {@link #getNoVersionMap()}.
*
* To get a mapping of qualifiers to latest values for an individual family use
* {@link #getFamilyMap(byte[])}.<p>
*
* To get the latest value for a specific family and qualifier use
* {@link #getValue(byte[], byte[])}.
*
* A Result is backed by an array of {@link Cell} objects, each representing
* an HBase cell defined by the row, family, qualifier, timestamp, and value.<p>
*
* The underlying {@link Cell} objects can be accessed through the method {@link #listCells()}.
* This will create a List from the internal Cell []. Better is to exploit the fact that
* a new Result instance is a primed {@link CellScanner}; just call {@link #advance()} and
* {@link #current()} to iterate over Cells as you would any {@link CellScanner}.
* Call {@link #cellScanner()} to reset should you need to iterate the same Result over again
* ({@link CellScanner}s are one-shot).
*/
public class ResultImpl implements Result {
 private Cell[] cells;
 private Boolean exists; // if the query was just to check existence.
 private boolean stale = false;

 /**
  * See {@link #mayHaveMoreCellsInRow()}.
  */
 private boolean mayHaveMoreCellsInRow = false;
 // We're not using java serialization.  Transient here is just a marker to say
 // that this is where we cache row if we're ever asked for it.
 private transient byte [] row = null;
 // Ditto for familyMap.  It can be composed on fly from passed in kvs.
 private transient NavigableMap<byte[], NavigableMap<byte[], NavigableMap<Long, byte[]>>>
     familyMap = null;

 public static final ResultImpl EMPTY_RESULT = new ResultImpl(true);

 private final boolean readonly;

 /**
  * Creates an empty Result w/ no KeyValue payload; returns null if you call {@link #rawCells()}.
  * Use this to represent no results if {@code null} won't do or in old 'mapred' as opposed
  * to 'mapreduce' package MapReduce where you need to overwrite a Result instance with a
  * {@link #copyFrom(ResultImpl)} call.
  */
 public ResultImpl() {
   this(false);
 }

 /**
  * Allows to construct special purpose immutable Result objects,
  * such as EMPTY_RESULT.
  * @param readonly whether this Result instance is readonly
  */
 private ResultImpl(boolean readonly) {
   this.readonly = readonly;
 }

 /**
  * Instantiate a Result with the specified List of KeyValues.
  * <br><strong>Note:</strong> You must ensure that the keyvalues are already sorted.
  * @param cells List of cells
  */
 public static ResultImpl create(List<Cell> cells) {
   return create(cells, null);
 }

 public static ResultImpl create(List<Cell> cells, Boolean exists) {
   return create(cells, exists, false);
 }

 public static ResultImpl create(List<Cell> cells, Boolean exists, boolean stale) {
   return create(cells, exists, stale, false);
 }

 public static ResultImpl create(List<Cell> cells, Boolean exists, boolean stale,
     boolean mayHaveMoreCellsInRow) {
   if (exists != null){
     return new ResultImpl(null, exists, stale, mayHaveMoreCellsInRow);
   }
   return new ResultImpl(cells.toArray(new Cell[cells.size()]), null, stale, mayHaveMoreCellsInRow);
 }

 /**
  * Instantiate a Result with the specified array of KeyValues.
  * <br><strong>Note:</strong> You must ensure that the keyvalues are already sorted.
  * @param cells array of cells
  */
 public static ResultImpl create(Cell[] cells) {
   return create(cells, null, false);
 }

 public static ResultImpl create(Cell[] cells, Boolean exists, boolean stale) {
   return create(cells, exists, stale, false);
 }

 public static ResultImpl create(Cell[] cells, Boolean exists, boolean stale,
     boolean mayHaveMoreCellsInRow) {
   if (exists != null) {
     return new ResultImpl(null, exists, stale, mayHaveMoreCellsInRow);
   }
   return new ResultImpl(cells, null, stale, mayHaveMoreCellsInRow);
 }

 /** Private ctor. Use {@link #create(Cell[])}. */
 private ResultImpl(Cell[] cells, Boolean exists, boolean stale, boolean mayHaveMoreCellsInRow) {
   this.cells = cells;
   this.exists = exists;
   this.stale = stale;
   this.mayHaveMoreCellsInRow = mayHaveMoreCellsInRow;
   this.readonly = false;
 }

 /**
  * Method for retrieving the row key that corresponds to
  * the row from which this Result was created.
  * @return row
  */
 @Override
 public byte [] getRow() {
   if (this.row == null) {
     this.row = (this.cells == null || this.cells.length == 0) ?
         null :
         CellUtil.cloneRow(this.cells[0]);
   }
   return this.row;
 }

 /**
  * Return the array of Cells backing this Result instance.
  *
  * The array is sorted from smallest -&gt; largest using the
  * {@link CellComparator}.
  *
  * The array only contains what your Get or Scan specifies and no more.
  * For example if you request column "A" 1 version you will have at most 1
  * Cell in the array. If you request column "A" with 2 version you will
  * have at most 2 Cells, with the first one being the newer timestamp and
  * the second being the older timestamp (this is the sort order defined by
  * {@link CellComparator}).  If columns don't exist, they won't be
  * present in the result. Therefore if you ask for 1 version all columns,
  * it is safe to iterate over this array and expect to see 1 Cell for
  * each column and no more.
  *
  * This API is faster than using getFamilyMap() and getMap()
  *
  * @return array of Cells; can be null if nothing in the result
  */
 public Cell[] rawCells() {
   return cells;
 }

 /**
  * Create a sorted list of the Cell's in this result.
  *
  * Since HBase 0.20.5 this is equivalent to raw().
  *
  * @return sorted List of Cells; can be null if no cells in the result
  */
 public List<Cell> listCells() {
   return isEmpty()? null: Arrays.asList(rawCells());
 }

 /**
  * Return the Cells for the specific column.  The Cells are sorted in
  * the {@link CellComparator} order.  That implies the first entry in
  * the list is the most recent column.  If the query (Scan or Get) only
  * requested 1 version the list will contain at most 1 entry.  If the column
  * did not exist in the result set (either the column does not exist
  * or the column was not selected in the query) the list will be empty.
  *
  * Also see getColumnLatest which returns just a Cell
  *
  * @param family the family
  * @param qualifier
  * @return a list of Cells for this column or empty list if the column
  * did not exist in the result set
  */
 public List<Cell> getColumnCells(byte [] family, byte [] qualifier) {
   List<Cell> result = new ArrayList<>();

   Cell [] kvs = rawCells();

   if (kvs == null || kvs.length == 0) {
     return result;
   }
   int pos = binarySearch(kvs, family, qualifier);
   if (pos == -1) {
     return result; // cant find it
   }

   for (int i = pos; i < kvs.length; i++) {
     if (CellUtil.matchingColumn(kvs[i], family,qualifier)) {
       result.add(kvs[i]);
     } else {
       break;
     }
   }

   return result;
 }

 private byte[] notNullBytes(final byte[] bytes) {
   if (bytes == null) {
     return HConstants.EMPTY_BYTE_ARRAY;
   } else {
     return bytes;
   }
 }

 protected int binarySearch(final Cell [] kvs,
                            final byte [] family,
                            final byte [] qualifier) {
   byte[] familyNotNull = notNullBytes(family);
   byte[] qualifierNotNull = notNullBytes(qualifier);
   Cell searchTerm =
       PrivateCellUtil.createFirstOnRow(kvs[0].getRowArray(),
           kvs[0].getRowOffset(), kvs[0].getRowLength(),
           familyNotNull, 0, (byte)familyNotNull.length,
           qualifierNotNull, 0, qualifierNotNull.length);

   // pos === ( -(insertion point) - 1)
   int pos = Arrays.binarySearch(kvs, searchTerm, CellComparatorImpl.COMPARATOR);
   // never will exact match
   if (pos < 0) {
     pos = (pos+1) * -1;
     // pos is now insertion point
   }
   if (pos == kvs.length) {
     return -1; // doesn't exist
   }
   return pos;
 }

 /**
  * The Cell for the most recent timestamp for a given column.
  *
  * @param family
  * @param qualifier
  *
  * @return the Cell for the column, or null if no value exists in the row or none have been
  * selected in the query (Get/Scan)
  */
 public Cell getColumnLatestCell(byte [] family, byte [] qualifier) {
   Cell [] kvs = rawCells(); // side effect possibly.
   if (kvs == null || kvs.length == 0) {
     return null;
   }
   int pos = binarySearch(kvs, family, qualifier);
   if (pos == -1) {
     return null;
   }
   if (CellUtil.matchingColumn(kvs[pos], family, qualifier)) {
     return kvs[pos];
   }
   return null;
 }

 /**
  * Get the latest version of the specified column as a byte array.
  * Note: this call clones the value content of the hosting Cell. 
  * @param family family name
  * @param qualifier column qualifier
  * @return value of latest version of column, null if none found
  */
 @Override
 public byte[] getValue(byte [] family, byte [] qualifier) {
   Cell kv = getColumnLatestCell(family, qualifier);
   if (kv == null) {
     return null;
   }
   return CellUtil.cloneValue(kv);
 }

	 /**
	  * Get the latest version of the specified column as a String (UTF-8).
	  * Note: this call clones the value content of the hosting Cell. 
	  * @param family family name
	  * @param qualifier column qualifier
	  * @param defaultValue Value returned if value does not exist.
	  * @return value of latest version of column, defaultValue if none found
	  */
	@Override
	public String getStringValue(String family, String qualifier, String defaultValue)
	{
		String value = defaultValue;
		
		byte[] data = getValue(family.getBytes(HConstants.DEF_CHARSET), qualifier.getBytes(HConstants.DEF_CHARSET));
		
		if (data != null)
		{
			value = new String(data,HConstants.DEF_CHARSET);
		}
	
		return value;
	}
	
	 /**
	  * Get the latest version of the specified column as a float.
	  * Note: this call clones the value content of the hosting Cell. 
	  * @param family family name
	  * @param qualifier column qualifier
	  * @param defaultValue Value returned if value does not exist.
	  * @return value of latest version of column, defaultValue if none found
	  */
	@Override
	public float getFloatValue(String family, String qualifier, float defaultValue)
	{
		float value = defaultValue;
		
		byte[] data = getValue(family.getBytes(HConstants.DEF_CHARSET), qualifier.getBytes(HConstants.DEF_CHARSET));
			
		if (data != null)
		{
			value = Bytes.toFloat(data);
		}
		
		return value;
	}
	
	 /**
	  * Get the latest version of the specified column as a double.
	  * Note: this call clones the value content of the hosting Cell. 
	  * @param family family name
	  * @param qualifier column qualifier
	  * @param defaultValue Value returned if value does not exist.
	  * @return value of latest version of column, defaultValue if none found
	  */
	@Override
	public double getDoubleValue(String family, String qualifier, double defaultValue)
	{
		double value = defaultValue;
		
		byte[] data = getValue(family.getBytes(HConstants.DEF_CHARSET), qualifier.getBytes(HConstants.DEF_CHARSET));
		
		if (data != null)
		{
			value = Bytes.toDouble(data);
		}

		return value;
	}

	 /**
	  * Get the latest version of the specified column as a BigDecimal.
	  * Note: this call clones the value content of the hosting Cell. 
	  * @param family family name
	  * @param qualifier column qualifier
	  * @param defaultValue Value returned if value does not exist.
	  * @return value of latest version of column, defaultValue if none found
	  */
	@Override
	public BigDecimal getBigDecimal(String family, String qualifier, BigDecimal defaultValue)
	{
		BigDecimal value = defaultValue;
		
		byte[] data = getValue(family.getBytes(HConstants.DEF_CHARSET), qualifier.getBytes(HConstants.DEF_CHARSET));
		
		if (data != null)
		{
			value = Bytes.toBigDecimal(data);
		}

		return value;
	}

	 /**
	  * Get the latest version of the specified column as a byte.
	  * Note: this call clones the value content of the hosting Cell. 
	  * @param family family name
	  * @param qualifier column qualifier
	  * @param defaultValue Value returned if value does not exist.
	  * @return value of latest version of column, defaultValue if none found
	  */
	@Override
	public byte getByteValue(String family, String qualifier, byte defaultValue)
	{
		byte value = defaultValue;
		
		byte[] data = getValue(family.getBytes(HConstants.DEF_CHARSET), qualifier.getBytes(HConstants.DEF_CHARSET));
			
		if (data != null)
		{
			value = data[0];
		}

		return value;
	}

	 /**
	  * Get the latest version of the specified column as a int.
	  * Note: this call clones the value content of the hosting Cell. 
	  * @param family family name
	  * @param qualifier column qualifier
	  * @param defaultValue Value returned if value does not exist.
	  * @return value of latest version of column, defaultValue if none found
	  */
	@Override
	public int getIntegerValue(String family, String qualifier, int defaultValue)
	{
		int value = defaultValue;
		
		byte[] data = getValue(family.getBytes(HConstants.DEF_CHARSET), qualifier.getBytes(HConstants.DEF_CHARSET));
			
		if (data != null)
		{
			value = Bytes.toInt(data);
		}

		return value;
	}

	 /**
	  * Get the latest version of the specified column as a short.
	  * Note: this call clones the value content of the hosting Cell. 
	  * @param family family name
	  * @param qualifier column qualifier
	  * @param defaultValue Value returned if value does not exist.
	  * @return value of latest version of column, defaultValue if none found
	  */
	@Override
	public short getShortValue(String family, String qualifier, short defaultValue)
	{
		short value = defaultValue;
		
		byte[] data = getValue(family.getBytes(HConstants.DEF_CHARSET), qualifier.getBytes(HConstants.DEF_CHARSET));
			
		if (data != null)
		{
			value = Bytes.toShort(data);
		}

		return value;
	}

	 /**
	  * Get the latest version of the specified column as a double.
	  * Note: this call clones the value content of the hosting Cell. 
	  * @param family family name
	  * @param qualifier column qualifier
	  * @param defaultValue Value returned if value does not exist.
	  * @return value of latest version of column, defaultValue if none found
	  */
	@Override
	public long getDoubleValue(String family, String qualifier, long defaultValue)
	{
		long value = defaultValue;
		
		byte[] data = getValue(family.getBytes(HConstants.DEF_CHARSET), qualifier.getBytes(HConstants.DEF_CHARSET));
		
		if (data != null)
		{
			value = Bytes.toLong(data);
		}
		
		return value;
	}

 /**
  * Checks for existence of a value for the specified column (empty or not).
  *
  * @param family family name
  * @param qualifier column qualifier
  *
  * @return true if at least one value exists in the result, false if not
  */
@Override
public boolean containsColumn(byte [] family, byte [] qualifier) {
   Cell kv = getColumnLatestCell(family, qualifier);
   return kv != null;
 }

/**
 * Checks for existence of a value for the specified column (empty or not).
 *
 * @param family family name
 * @param qualifier column qualifier
 *
 * @return true if at least one value exists in the result, false if not
 */
@Override
public boolean containsColumn(String family, String qualifier) {
  Cell kv = getColumnLatestCell(family.getBytes(HConstants.DEF_CHARSET), qualifier.getBytes(HConstants.DEF_CHARSET));
  return kv != null;
}

 /**
  * Map of families to all versions of its qualifiers and values.
  * <p>
  * Returns a three level Map of the form:
  * <code>Map&amp;family,Map&lt;qualifier,Map&lt;timestamp,value&gt;&gt;&gt;</code>
  * <p>
  * Note: All other map returning methods make use of this map internally.
  * @return map from families to qualifiers to versions
  */
 @Override
 public NavigableMap<byte[], NavigableMap<byte[], NavigableMap<Long, byte[]>>> getMap() {
   if (this.familyMap != null) {
     return this.familyMap;
   }
   if(isEmpty()) {
     return null;
   }
   this.familyMap = new TreeMap<>(Bytes.BYTES_COMPARATOR);
   for(Cell kv : this.cells) {
     byte [] family = CellUtil.cloneFamily(kv);
     NavigableMap<byte[], NavigableMap<Long, byte[]>> columnMap = familyMap.get(family);
     if(columnMap == null) {
       columnMap = new TreeMap<>(Bytes.BYTES_COMPARATOR);
       familyMap.put(family, columnMap);
     }
     byte [] qualifier = CellUtil.cloneQualifier(kv);
     NavigableMap<Long, byte[]> versionMap = columnMap.get(qualifier);
     if(versionMap == null) {
       versionMap = new TreeMap<>(new Comparator<Long>() {
         @Override
         public int compare(Long l1, Long l2) {
           return l2.compareTo(l1);
         }
       });
       columnMap.put(qualifier, versionMap);
     }
     Long timestamp = kv.getTimestamp();
     byte [] value = CellUtil.cloneValue(kv);

     versionMap.put(timestamp, value);
   }
   return this.familyMap;
 }

 /**
  * Map of families to their most recent qualifiers and values.
  * <p>
  * Returns a two level Map of the form: <code>Map&amp;family,Map&lt;qualifier,value&gt;&gt;</code>
  * <p>
  * The most recent version of each qualifier will be used.
  * @return map from families to qualifiers and value
  */
@Override
public NavigableMap<byte[], NavigableMap<byte[], byte[]>> getNoVersionMap() {
   if(this.familyMap == null) {
     getMap();
   }
   if(isEmpty()) {
     return null;
   }
   NavigableMap<byte[], NavigableMap<byte[], byte[]>> returnMap = new TreeMap<>(Bytes.BYTES_COMPARATOR);
   for(Map.Entry<byte[], NavigableMap<byte[], NavigableMap<Long, byte[]>>>
     familyEntry : familyMap.entrySet()) {
     NavigableMap<byte[], byte[]> qualifierMap = new TreeMap<>(Bytes.BYTES_COMPARATOR);
     for(Map.Entry<byte[], NavigableMap<Long, byte[]>> qualifierEntry :
       familyEntry.getValue().entrySet()) {
       byte [] value =
         qualifierEntry.getValue().get(qualifierEntry.getValue().firstKey());
       qualifierMap.put(qualifierEntry.getKey(), value);
     }
     returnMap.put(familyEntry.getKey(), qualifierMap);
   }
   return returnMap;
 }

 /**
  * Map of qualifiers to values.
  * <p>
  * Returns a Map of the form: <code>Map&lt;qualifier,value&gt;</code>
  * @param family column family to get
  * @return map of qualifiers to values
  */
 @Override
 public NavigableMap<byte[], byte[]> getFamilyMap(byte [] family) {
   if(this.familyMap == null) {
     getMap();
   }
   if(isEmpty()) {
     return null;
   }
   NavigableMap<byte[], byte[]> returnMap = new TreeMap<>(Bytes.BYTES_COMPARATOR);
   NavigableMap<byte[], NavigableMap<Long, byte[]>> qualifierMap =
     familyMap.get(family);
   if(qualifierMap == null) {
     return returnMap;
   }
   for(Map.Entry<byte[], NavigableMap<Long, byte[]>> entry :
     qualifierMap.entrySet()) {
     byte [] value =
       entry.getValue().get(entry.getValue().firstKey());
     returnMap.put(entry.getKey(), value);
   }
   return returnMap;
 }

 /**
  * Returns the value of the first column in the Result.
  * @return value of the first column
  */
 public byte [] value() {
   if (isEmpty()) {
     return null;
   }
   return CellUtil.cloneValue(cells[0]);
 }

 /**
  * Check if the underlying Cell [] is empty or not
  * @return true if empty
  */
 @Override
 public boolean isEmpty() {
   return this.cells == null || this.cells.length == 0;
 }

 /**
  * @return the size of the underlying Cell []
  */
 @Override
 public int size() {
   return this.cells == null? 0: this.cells.length;
 }

 /**
  * @return String
  */
 @Override
 public String toString() {
   StringBuilder sb = new StringBuilder();
   sb.append("keyvalues=");
   if(isEmpty()) {
     sb.append("NONE");
     return sb.toString();
   }
   sb.append("{");
   boolean moreThanOne = false;
   for(Cell kv : this.cells) {
     if(moreThanOne) {
       sb.append(", ");
     } else {
       moreThanOne = true;
     }
     sb.append(kv.toString());
   }
   sb.append("}");
   return sb.toString();
 }

 public Boolean getExists() {
   return exists;
 }

 public void setExists(Boolean exists) {
   checkReadonly();
   this.exists = exists;
 }

 /**
  * Whether or not the results are coming from possibly stale data. Stale results
  * might be returned if {@link Consistency} is not STRONG for the query.
  * @return Whether or not the results are coming from possibly stale data.
  */
 public boolean isStale() {
   return stale;
 }

 /**
  * For scanning large rows, the RS may choose to return the cells chunk by chunk to prevent OOM
  * or timeout. This flag is used to tell you if the current Result is the last one of the current
  * row. False means this Result is the last one. True means there MAY be more cells belonging to
  * the current row.
  * If you don't use {@link Scan#setAllowPartialResults(boolean)} or {@link Scan#setBatch(int)},
  * this method will always return false because the Result must contains all cells in one Row.
  */
 public boolean mayHaveMoreCellsInRow() {
   return mayHaveMoreCellsInRow;
 }

 /**
  * All methods modifying state of Result object must call this method
  * to ensure that special purpose immutable Results can't be accidentally modified.
  */
 private void checkReadonly() {
   if (readonly == true) {
     throw new UnsupportedOperationException("Attempting to modify readonly EMPTY_RESULT!");
   }
 }
}
