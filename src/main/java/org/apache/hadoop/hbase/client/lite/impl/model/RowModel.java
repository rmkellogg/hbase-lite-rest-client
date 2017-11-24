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

package org.apache.hadoop.hbase.client.lite.impl.model;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.hadoop.hbase.client.lite.impl.ProtobufMessageHandler;

/**
* Representation of a row. A row is a related set of cells, grouped by common
* row key. RowModels do not appear in results by themselves. They are always
* encapsulated within CellSetModels.
*/
public class RowModel implements ProtobufMessageHandler {
 private byte[] key;

 private List<CellModel> cells = new ArrayList<>();


 /**
  * Default constructor
  */
 public RowModel() { }

 /**
  * Constructor
  * @param key the row key
  */
 public RowModel(final String key) {
   this(key.getBytes());
 }
 
 /**
  * Constructor
  * @param key the row key
  */
 public RowModel(final byte[] key) {
   this.key = key;
   cells = new ArrayList<>();
 }

 /**
  * Constructor
  * @param key the row key
  * @param cells the cells
  */
 public RowModel(final String key, final List<CellModel> cells) {
   this(key.getBytes(), cells);
 }
 
 /**
  * Constructor
  * @param key the row key
  * @param cells the cells
  */
 public RowModel(final byte[] key, final List<CellModel> cells) {
   this.key = key;
   this.cells = cells;
 }
 
 /**
  * Adds a cell to the list of cells for this row
  * @param cell the cell
  */
 public void addCell(CellModel cell) {
   cells.add(cell);
 }

 /**
  * @return the row key
  */
 public byte[] getKey() {
   return key;
 }

 /**
  * @param key the row key
  */
 public void setKey(byte[] key) {
   this.key = key;
 }

 /**
  * @return the cells
  */
 public List<CellModel> getCells() {
   return cells;
 }

 @Override
 public byte[] createProtobufOutput() {
   // there is no standalone row protobuf message
   throw new UnsupportedOperationException(
       "no protobuf equivalent to RowModel");
 }

 @Override
 public ProtobufMessageHandler getObjectFromMessage(byte[] message)
     throws IOException {
   // there is no standalone row protobuf message
   throw new UnsupportedOperationException(
       "no protobuf equivalent to RowModel");
 }

 @Override
 public boolean equals(Object obj) {
   if (obj == null) {
     return false;
   }
   if (obj == this) {
     return true;
   }
   if (obj.getClass() != getClass()) {
     return false;
   }
   RowModel rowModel = (RowModel) obj;
   return new EqualsBuilder().
       append(key, rowModel.key).
       append(cells, rowModel.cells).
       isEquals();
 }

 @Override
 public int hashCode() {
   return new HashCodeBuilder().
       append(key).
       append(cells).
       toHashCode();
 }

 @Override
 public String toString() {
   return new ToStringBuilder(this).
       append("key", key).
       append("cells", cells).
       toString();
 }
}
