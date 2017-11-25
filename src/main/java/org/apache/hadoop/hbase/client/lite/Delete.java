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

//package org.apache.hadoop.hbase.client;
package org.apache.hadoop.hbase.client.lite;

import java.util.List;
import java.util.Map;

import org.apache.hadoop.hbase.client.lite.impl.Bytes;
import org.apache.hadoop.hbase.client.lite.impl.HConstants;
import org.apache.hadoop.hbase.client.lite.impl.KeyValue;

/**
* Used to perform Delete operations on a single row.
* <p>
* To delete an entire row, instantiate a Delete object with the row
* to delete.  To further define the scope of what to delete, perform
* additional methods as outlined below.
* <p>
* To delete specific families, execute {@link #addFamily(byte[]) deleteFamily}
* for each family to delete.
* <p>
* To delete multiple versions of specific columns, execute
* {@link #addColumns(byte[], byte[]) deleteColumns}
* for each column to delete.
* <p>
* To delete specific versions of specific columns, execute
* {@link #addColumn(byte[], byte[], long) deleteColumn}
* for each column version to delete.
* <p>
* Specifying timestamps, deleteFamily and deleteColumns will delete all
* versions with a timestamp less than or equal to that passed.  If no
* timestamp is specified, an entry is added with a timestamp of 'now'
* where 'now' is the servers's System.currentTimeMillis().
* Specifying a timestamp to the deleteColumn method will
* delete versions only with a timestamp equal to that specified.
* If no timestamp is passed to deleteColumn, internally, it figures the
* most recent cell's timestamp and adds a delete at that timestamp; i.e.
* it deletes the most recently added cell.
* <p>The timestamp passed to the constructor is used ONLY for delete of
* rows.  For anything less -- a deleteColumn, deleteColumns or
* deleteFamily -- then you need to use the method overrides that take a
* timestamp.  The constructor timestamp is not referenced.
*/
public class Delete extends Mutation {
 /**
  * Create a Delete operation for the specified row.
  * <p>
  * If no further operations are done, this will delete everything
  * associated with the specified row (all versions of all columns in all
  * families), with timestamp from current point in time to the past.
  * Cells defining timestamp for a future point in time
  * (timestamp > current time) will not be deleted.
  * @param row row key
  */
 public Delete(byte [] row) {
   this(row, HConstants.LATEST_TIMESTAMP);
 }

 /**
  * Create a Delete operation for the specified row and timestamp.<p>
  *
  * If no further operations are done, this will delete all columns in all
  * families of the specified row with a timestamp less than or equal to the
  * specified timestamp.<p>
  *
  * This timestamp is ONLY used for a delete row operation.  If specifying
  * families or columns, you must specify each timestamp individually.
  * @param row row key
  * @param timestamp maximum version timestamp (only for delete row)
  */
 public Delete(byte [] row, long timestamp) {
   this(row, 0, row.length, timestamp);
 }

 /**
  * Create a Delete operation for the specified row and timestamp.<p>
  *
  * If no further operations are done, this will delete all columns in all
  * families of the specified row with a timestamp less than or equal to the
  * specified timestamp.<p>
  *
  * This timestamp is ONLY used for a delete row operation.  If specifying
  * families or columns, you must specify each timestamp individually.
  * @param row We make a local copy of this passed in row.
  * @param rowOffset
  * @param rowLength
  */
 public Delete(final byte[] row, final int rowOffset, final int rowLength) {
   this(row, rowOffset, rowLength, HConstants.LATEST_TIMESTAMP);
 }

 /**
  * Create a Delete operation for the specified row and timestamp.<p>
  *
  * If no further operations are done, this will delete all columns in all
  * families of the specified row with a timestamp less than or equal to the
  * specified timestamp.<p>
  *
  * This timestamp is ONLY used for a delete row operation.  If specifying
  * families or columns, you must specify each timestamp individually.
  * @param row We make a local copy of this passed in row.
  * @param rowOffset
  * @param rowLength
  * @param timestamp maximum version timestamp (only for delete row)
  */
 public Delete(final byte[] row, final int rowOffset, final int rowLength, long timestamp) {
   checkRow(row, rowOffset, rowLength);
   this.row = Bytes.copy(row, rowOffset, rowLength);
   setTimestamp(timestamp);
 }

 /**
  * Delete all versions of all columns of the specified family.
  * <p>
  * Overrides previous calls to deleteColumn and deleteColumns for the
  * specified family.
  * @param family family name
  * @return this for invocation chaining
  */
 public Delete addFamily(final byte [] family) {
   this.addFamily(family, this.ts);
   return this;
 }

 /**
  * Delete all columns of the specified family with a timestamp less than
  * or equal to the specified timestamp.
  * <p>
  * Overrides previous calls to deleteColumn and deleteColumns for the
  * specified family.
  * @param family family name
  * @param timestamp maximum version timestamp
  * @return this for invocation chaining
  */
 public Delete addFamily(final byte [] family, final long timestamp) {
   if (timestamp < 0) {
     throw new IllegalArgumentException("Timestamp cannot be negative. ts=" + timestamp);
   }
   List<Cell> list = getCellList(family);
   if(!list.isEmpty()) {
     list.clear();
   }
   KeyValue kv = new KeyValue(row, family, null, timestamp, KeyValue.Type.DeleteFamily);
   list.add(kv);
   return this;
 }

 /**
  * Delete all columns of the specified family with a timestamp equal to
  * the specified timestamp.
  * @param family family name
  * @param timestamp version timestamp
  * @return this for invocation chaining
  */
 public Delete addFamilyVersion(final byte [] family, final long timestamp) {
   List<Cell> list = getCellList(family);
   list.add(new KeyValue(row, family, null, timestamp,
         KeyValue.Type.DeleteFamilyVersion));
   return this;
 }

 /**
  * Delete all versions of the specified column.
  * @param family family name
  * @param qualifier column qualifier
  * @return this for invocation chaining
  */
 public Delete addColumns(final byte [] family, final byte [] qualifier) {
   addColumns(family, qualifier, this.ts);
   return this;
 }

 /**
  * Delete all versions of the specified column with a timestamp less than
  * or equal to the specified timestamp.
  * @param family family name
  * @param qualifier column qualifier
  * @param timestamp maximum version timestamp
  * @return this for invocation chaining
  */
 public Delete addColumns(final byte [] family, final byte [] qualifier, final long timestamp) {
   if (timestamp < 0) {
     throw new IllegalArgumentException("Timestamp cannot be negative. ts=" + timestamp);
   }
   List<Cell> list = getCellList(family);
   list.add(new KeyValue(this.row, family, qualifier, timestamp,
       KeyValue.Type.DeleteColumn));
   return this;
 }

 /**
  * Delete the latest version of the specified column.
  * This is an expensive call in that on the server-side, it first does a
  * get to find the latest versions timestamp.  Then it adds a delete using
  * the fetched cells timestamp.
  * @param family family name
  * @param qualifier column qualifier
  * @return this for invocation chaining
  */
 public Delete addColumn(final byte [] family, final byte [] qualifier) {
   this.addColumn(family, qualifier, this.ts);
   return this;
 }

 /**
  * Delete the specified version of the specified column.
  * @param family family name
  * @param qualifier column qualifier
  * @param timestamp version timestamp
  * @return this for invocation chaining
  */
 public Delete addColumn(byte [] family, byte [] qualifier, long timestamp) {
   if (timestamp < 0) {
     throw new IllegalArgumentException("Timestamp cannot be negative. ts=" + timestamp);
   }
   List<Cell> list = getCellList(family);
   KeyValue kv = new KeyValue(this.row, family, qualifier, timestamp, KeyValue.Type.Delete);
   list.add(kv);
   return this;
 }

 /**
  * Set the timestamp of the delete.
  *
  * @param timestamp
  */
 public Delete setTimestamp(long timestamp) {
   if (timestamp < 0) {
     throw new IllegalArgumentException("Timestamp cannot be negative. ts=" + timestamp);
   }
   this.ts = timestamp;
   return this;
 }

 @Override
 public Map<String, Object> toMap(int maxCols) {
   // we start with the fingerprint map and build on top of it.
   Map<String, Object> map = super.toMap(maxCols);
   // why is put not doing this?
   map.put("ts", this.ts);
   return map;
 }
}
