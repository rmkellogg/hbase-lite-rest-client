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
import java.util.Map;
import java.util.NavigableSet;

import org.apache.hadoop.hbase.client.lite.Scan;
import org.apache.hadoop.hbase.client.lite.impl.Bytes;
import org.apache.hadoop.hbase.client.lite.impl.HConstants;
import org.apache.hadoop.hbase.client.lite.impl.ProtobufMessageHandler;
import org.apache.hadoop.hbase.client.lite.impl.ProtobufUtil;
import org.apache.hadoop.hbase.rest.protobuf.generated.ScannerMessage.Scanner;

import com.google.protobuf.ByteString;

/**
* A representation of Scanner parameters.
*/
public class ScannerModel implements ProtobufMessageHandler {
 private byte[] startRow = HConstants.EMPTY_START_ROW;
 private byte[] endRow = HConstants.EMPTY_END_ROW;;
 private List<byte[]> columns = new ArrayList<>();
 private long startTime = 0;
 private long endTime = Long.MAX_VALUE;
 private int maxVersions = Integer.MAX_VALUE;

 private static final byte[] COLUMN_DIVIDER = Bytes.toBytes(":");

 /**
  * @param scan the scan specification
  * @throws Exception
  */
 public static ScannerModel fromScan(Scan scan) throws Exception {
   ScannerModel model = new ScannerModel();
   model.setStartRow(scan.getStartRow());
   model.setEndRow(scan.getStopRow());
   Map<byte [], NavigableSet<byte []>> families = scan.getFamilyMap();
   if (families != null) {
     for (Map.Entry<byte [], NavigableSet<byte []>> entry : families.entrySet()) {
       if (entry.getValue() != null) {
         for (byte[] qualifier: entry.getValue()) {
           model.addColumn(Bytes.add(entry.getKey(), COLUMN_DIVIDER, qualifier));
         }
       } else {
         model.addColumn(entry.getKey());
       }
     }
   }
   model.setStartTime(scan.getTimeRange().getMin());
   model.setEndTime(scan.getTimeRange().getMax());
   int maxVersions = scan.getMaxVersions();
   if (maxVersions > 0) {
     model.setMaxVersions(maxVersions);
   }
   return model;
 }

 /**
  * Default constructor
  */
 public ScannerModel() {}

 /**
  * Add a column to the column set
  * @param column the column name, as &lt;column&gt;(:&lt;qualifier&gt;)?
  */
 public void addColumn(byte[] column) {
   columns.add(column);
 }

 /**
  * @return true if a start row was specified
  */
 public boolean hasStartRow() {
   return !Bytes.equals(startRow, HConstants.EMPTY_START_ROW);
 }

 /**
  * @return start row
  */
 public byte[] getStartRow() {
   return startRow;
 }

 /**
  * @return true if an end row was specified
  */
 public boolean hasEndRow() {
   return !Bytes.equals(endRow, HConstants.EMPTY_END_ROW);
 }

 /**
  * @return end row
  */
 public byte[] getEndRow() {
   return endRow;
 }

 /**
  * @return list of columns of interest in column:qualifier format, or empty for all
  */
 public List<byte[]> getColumns() {
   return columns;
 }

 /**
  * @return the lower bound on timestamps of items of interest
  */
 public long getStartTime() {
   return startTime;
 }

 /**
  * @return the upper bound on timestamps of items of interest
  */
 public long getEndTime() {
   return endTime;
 }

 /**
  * @return maximum number of versions to return
  */
 public int getMaxVersions() {
   return maxVersions;
 }

 /**
  * @param startRow start row
  */
 public void setStartRow(byte[] startRow) {
   this.startRow = startRow;
 }

 /**
  * @param endRow end row
  */
 public void setEndRow(byte[] endRow) {
   this.endRow = endRow;
 }

 /**
  * @param columns list of columns of interest in column:qualifier format, or empty for all
  */
 public void setColumns(List<byte[]> columns) {
   this.columns = columns;
 }

 /**
  * @param maxVersions maximum number of versions to return
  */
 public void setMaxVersions(int maxVersions) {
   this.maxVersions = maxVersions;
 }

 /**
  * @param startTime the lower bound on timestamps of values of interest
  */
 public void setStartTime(long startTime) {
   this.startTime = startTime;
 }

 /**
  * @param endTime the upper bound on timestamps of values of interest
  */
 public void setEndTime(long endTime) {
   this.endTime = endTime;
 }

 @Override
 public byte[] createProtobufOutput() {
   Scanner.Builder builder = Scanner.newBuilder();
   if (!Bytes.equals(startRow, HConstants.EMPTY_START_ROW)) {
     builder.setStartRow(ByteString.copyFrom(startRow));
   }
   if (!Bytes.equals(endRow, HConstants.EMPTY_START_ROW)) {
     builder.setEndRow(ByteString.copyFrom(endRow));
   }
   for (byte[] column: columns) {
     builder.addColumns(ByteString.copyFrom(column));
   }
   if (startTime != 0) {
     builder.setStartTime(startTime);
   }
   if (endTime != 0) {
     builder.setEndTime(endTime);
   }
   builder.setMaxVersions(maxVersions);
   return builder.build().toByteArray();
 }

 @Override
 public ProtobufMessageHandler getObjectFromMessage(byte[] message)
     throws IOException {
   Scanner.Builder builder = Scanner.newBuilder();
   ProtobufUtil.mergeFrom(builder, message);
   if (builder.hasStartRow()) {
     startRow = builder.getStartRow().toByteArray();
   }
   if (builder.hasEndRow()) {
     endRow = builder.getEndRow().toByteArray();
   }
   for (ByteString column: builder.getColumnsList()) {
     addColumn(column.toByteArray());
   }
   if (builder.hasStartTime()) {
     startTime = builder.getStartTime();
   }
   if (builder.hasEndTime()) {
     endTime = builder.getEndTime();
   }
   if (builder.hasMaxVersions()) {
     maxVersions = builder.getMaxVersions();
   }
   return this;
 }
}
