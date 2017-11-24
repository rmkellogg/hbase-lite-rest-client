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
package org.apache.hadoop.hbase.client.lite;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hbase.client.lite.impl.Bytes;
import org.apache.hadoop.hbase.client.lite.impl.CellUtil;
import org.apache.hadoop.hbase.client.lite.impl.HConstants;
import org.apache.hadoop.hbase.client.lite.impl.KeyValue;

/**
* Used to perform Put operations for a single row.
* <p>
* To perform a Put, instantiate a Put object with the row to insert to, and
* for each column to be inserted, execute {@link #addColumn(byte[], byte[],
* byte[]) add} or {@link #addColumn(byte[], byte[], long, byte[]) add} if
* setting the timestamp.
*/
public class Put extends Mutation {
 /**
  * Create a Put operation for the specified row.
  * @param row row key
  */
 public Put(byte [] row) {
   this(row, HConstants.LATEST_TIMESTAMP);
 }

 /**
  * Create a Put operation for the specified row, using a given timestamp.
  *
  * @param row row key; we make a copy of what we are passed to keep local.
  * @param ts timestamp
  */
 public Put(byte[] row, long ts) {
   this(row, 0, row.length, ts);
 }

 /**
  * We make a copy of the passed in row key to keep local.
  * @param rowArray
  * @param rowOffset
  * @param rowLength
  * @param ts
  */
 public Put(byte [] rowArray, int rowOffset, int rowLength, long ts) {
   checkRow(rowArray, rowOffset, rowLength);
   this.row = Bytes.copy(rowArray, rowOffset, rowLength);
   this.ts = ts;
   if (ts < 0) {
     throw new IllegalArgumentException("Timestamp cannot be negative. ts=" + ts);
   }
 }

 /**
  * Add the specified column and value to this Put operation.
  * @param family family name
  * @param qualifier column qualifier
  * @param value column value
  * @return this
  */
 public Put addColumn(byte [] family, byte [] qualifier, byte [] value) {
   return addColumn(family, qualifier, this.ts, value);
 }

 /**
  * Add the specified column and value, with the specified timestamp as
  * its version to this Put operation.
  * @param family family name
  * @param qualifier column qualifier
  * @param ts version timestamp
  * @param value column value
  * @return this
  */
 public Put addColumn(byte [] family, byte [] qualifier, long ts, byte [] value) {
   if (ts < 0) {
     throw new IllegalArgumentException("Timestamp cannot be negative. ts=" + ts);
   }
   List<Cell> list = getCellList(family);
   KeyValue kv = createPutKeyValue(family, qualifier, ts, value);
   list.add(kv);
   return this;
 }

 /**
  * Add the specified KeyValue to this Put operation.  Operation assumes that
  * the passed KeyValue is immutable and its backing array will not be modified
  * for the duration of this Put.
  * @param kv individual KeyValue
  * @return this
  * @throws java.io.IOException e
  */
 public Put add(Cell kv) throws IOException{
   byte [] family = CellUtil.cloneFamily(kv);
   List<Cell> list = getCellList(family);
   //Checking that the row of the kv is the same as the put
   if (!CellUtil.matchingRows(kv, this.row)) {
//     throw new WrongRowIOException("The row in " + kv.toString() +
     throw new IOException("The row in " + kv.toString() +
       " doesn't match the original one " +  Bytes.toStringBinary(this.row));
   }
   list.add(kv);
   return this;
 }

 /**
  * Returns a list of all KeyValue objects with matching column family and qualifier.
  *
  * @param family column family
  * @param qualifier column qualifier
  * @return a list of KeyValue objects with the matching family and qualifier,
  * returns an empty list if one doesn't exist for the given family.
  */
 public List<Cell> get(byte[] family, byte[] qualifier) {
   List<Cell> filteredList = new ArrayList<>();
   for (Cell cell: getCellList(family)) {
     if (CellUtil.matchingQualifier(cell, qualifier)) {
       filteredList.add(cell);
     }
   }
   return filteredList;
 }
}
