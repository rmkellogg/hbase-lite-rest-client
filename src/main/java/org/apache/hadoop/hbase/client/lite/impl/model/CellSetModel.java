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

//package org.apache.hadoop.hbase.rest.model;
package org.apache.hadoop.hbase.client.lite.impl.model;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hbase.client.lite.impl.HConstants;
import org.apache.hadoop.hbase.client.lite.impl.ProtobufMessageHandler;
import org.apache.hadoop.hbase.client.lite.impl.ProtobufUtil;
import org.apache.hadoop.hbase.rest.protobuf.generated.CellMessage.Cell;
import org.apache.hadoop.hbase.rest.protobuf.generated.CellSetMessage.CellSet;

import com.google.protobuf.ByteString;

/**
* Representation of a grouping of cells. May contain cells from more than
* one row. Encapsulates RowModel and CellModel models.
*/
public class CellSetModel implements ProtobufMessageHandler {
 private List<RowModel> rows;

 /**
  * Constructor
  */
 public CellSetModel() {
   this.rows = new ArrayList<>();
 }

 /**
  * @param rows the rows
  */
 public CellSetModel(List<RowModel> rows) {
   super();
   this.rows = rows;
 }

 /**
  * Add a row to this cell set
  * @param row the row
  */
 public void addRow(RowModel row) {
   rows.add(row);
 }

 /**
  * @return the rows
  */
 public List<RowModel> getRows() {
   return rows;
 }

 @Override
 public byte[] createProtobufOutput() {
   CellSet.Builder builder = CellSet.newBuilder();
   for (RowModel row: getRows()) {
     CellSet.Row.Builder rowBuilder = CellSet.Row.newBuilder();
     rowBuilder.setKey(ByteString.copyFrom(row.getKey()));
     for (CellModel cell: row.getCells()) {
       Cell.Builder cellBuilder = Cell.newBuilder();
       cellBuilder.setColumn(ByteString.copyFrom(cell.getColumn()));
       cellBuilder.setData(ByteString.copyFrom(cell.getValue()));
       if (cell.hasUserTimestamp()) {
         cellBuilder.setTimestamp(cell.getTimestamp());
       }
       rowBuilder.addValues(cellBuilder);
     }
     builder.addRows(rowBuilder);
   }
   return builder.build().toByteArray();
 }

 @Override
 public ProtobufMessageHandler getObjectFromMessage(byte[] message)
     throws IOException {
   CellSet.Builder builder = CellSet.newBuilder();
   ProtobufUtil.mergeFrom(builder, message);
   for (CellSet.Row row: builder.getRowsList()) {
     RowModel rowModel = new RowModel(row.getKey().toByteArray());
     for (Cell cell: row.getValuesList()) {
       long timestamp = HConstants.LATEST_TIMESTAMP;
       if (cell.hasTimestamp()) {
         timestamp = cell.getTimestamp();
       }
       rowModel.addCell(
           new CellModel(cell.getColumn().toByteArray(), timestamp,
                 cell.getData().toByteArray()));
     }
     addRow(rowModel);
   }
   return this;
 }
}
