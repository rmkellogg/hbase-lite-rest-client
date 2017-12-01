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

import org.apache.hadoop.hbase.client.lite.Cell;

public class CellUtil {
	  /**
	   * Colon character in UTF-8
	   */
	  public static final char COLUMN_FAMILY_DELIMITER = ':';

	  public static final byte[] COLUMN_FAMILY_DELIM_ARRAY =
	    new byte[]{COLUMN_FAMILY_DELIMITER};

	  /***************** get individual arrays for tests ************/

	  public static byte[] cloneRow(Cell cell) {
	    byte[] output = new byte[cell.getRowLength()];
	    copyRowTo(cell, output, 0);
	    return output;
	  }

	  public static byte[] cloneFamily(Cell cell) {
	    byte[] output = new byte[cell.getFamilyLength()];
	    copyFamilyTo(cell, output, 0);
	    return output;
	  }

	  public static byte[] cloneQualifier(Cell cell) {
	    byte[] output = new byte[cell.getQualifierLength()];
	    copyQualifierTo(cell, output, 0);
	    return output;
	  }

	  public static byte[] cloneValue(Cell cell) {
	    byte[] output = new byte[cell.getValueLength()];
	    copyValueTo(cell, output, 0);
	    return output;
	  }

	  /******************** copyTo **********************************/

	  /**
	   * Copies the row to the given byte[]
	   * @param cell the cell whose row has to be copied
	   * @param destination the destination byte[] to which the row has to be copied
	   * @param destinationOffset the offset in the destination byte[]
	   * @return the offset of the byte[] after the copy has happened
	   */
	  public static int copyRowTo(Cell cell, byte[] destination, int destinationOffset) {
	    short rowLen = cell.getRowLength();
	    System.arraycopy(cell.getRowArray(), cell.getRowOffset(), destination, destinationOffset, rowLen);
	    
	    return destinationOffset + rowLen;
	  }
	  
	  /**
	   * Copies the value to the given byte[]
	   * @param cell the cell whose value has to be copied
	   * @param destination the destination byte[] to which the value has to be copied
	   * @param destinationOffset the offset in the destination byte[]
	   * @return the offset of the byte[] after the copy has happened
	   */
	  public static int copyValueTo(Cell cell, byte[] destination, int destinationOffset) {
	    int vlen = cell.getValueLength();
	    System.arraycopy(cell.getValueArray(), cell.getValueOffset(), destination, destinationOffset, vlen);
	    
	    return destinationOffset + vlen;
	  }

	  /**
	   * Copies the family to the given byte[]
	   * @param cell the cell whose family has to be copied
	   * @param destination the destination byte[] to which the family has to be copied
	   * @param destinationOffset the offset in the destination byte[]
	   * @return the offset of the byte[] after the copy has happened
	   */
	  public static int copyFamilyTo(Cell cell, byte[] destination, int destinationOffset) {
	    byte fLen = cell.getFamilyLength();
	    System.arraycopy(cell.getFamilyArray(), cell.getFamilyOffset(), destination,
	        destinationOffset, fLen);
	    return destinationOffset + fLen;
	  }

	  /**
	   * Copies the qualifier to the given byte[]
	   * @param cell the cell whose qualifier has to be copied
	   * @param destination the destination byte[] to which the qualifier has to be copied
	   * @param destinationOffset the offset in the destination byte[]
	   * @return the offset of the byte[] after the copy has happened
	   */
	  public static int copyQualifierTo(Cell cell, byte[] destination, int destinationOffset) {
	    int qlen = cell.getQualifierLength();
	    System.arraycopy(cell.getQualifierArray(), cell.getQualifierOffset(), destination,
	        destinationOffset, qlen);
	    return destinationOffset + qlen;
	  }

	  public static boolean matchingColumn(final Cell left, final byte[] fam, final byte[] qual) {
		    if (!matchingFamily(left, fam)) return false;
		    return matchingQualifier(left, qual);
		  }

	  /**
	   * Finds if the qualifier part of the cell and the KV serialized byte[] are equal
	   * @param left
	   * @param buf the serialized keyvalue format byte[]
	   * @return true if the qualifier matches, false otherwise
	   */
	  public static boolean matchingQualifier(final Cell left, final byte[] buf) {
	    if (buf == null) {
	      return left.getQualifierLength() == 0;
	    }
	    return PrivateCellUtil.matchingQualifier(left, buf, 0, buf.length);
	  }
	  
	  /**
	   * Makes a column in family:qualifier form from separate byte arrays.
	   * <p>
	   * Not recommended for usage as this is old-style API.
	   * @param family
	   * @param qualifier
	   * @return family:qualifier
	   */
	  public static byte[] makeColumn(byte[] family, byte[] qualifier) {
	    return Bytes.add(family, COLUMN_FAMILY_DELIM_ARRAY, qualifier);
	  }
	  
	  /**
	   * Splits a column in {@code family:qualifier} form into separate byte arrays. An empty qualifier
	   * (ie, {@code fam:}) is parsed as <code>{ fam, EMPTY_BYTE_ARRAY }</code> while no delimiter (ie,
	   * {@code fam}) is parsed as an array of one element, <code>{ fam }</code>.
	   * <p>
	   * Don't forget, HBase DOES support empty qualifiers. (see HBASE-9549)
	   * </p>
	   * <p>
	   * Not recommend to be used as this is old-style API.
	   * </p>
	   * @param c The column.
	   * @return The parsed column.
	   */
	  public static byte[][] parseColumn(byte[] c) {
	    final int index = getDelimiter(c, 0, c.length, COLUMN_FAMILY_DELIMITER);
	    if (index == -1) {
	      // If no delimiter, return array of size 1
	      return new byte[][] { c };
	    } else if (index == c.length - 1) {
	      // family with empty qualifier, return array size 2
	      byte[] family = new byte[c.length - 1];
	      System.arraycopy(c, 0, family, 0, family.length);
	      return new byte[][] { family, HConstants.EMPTY_BYTE_ARRAY };
	    }
	    // Family and column, return array size 2
	    final byte[][] result = new byte[2][];
	    result[0] = new byte[index];
	    System.arraycopy(c, 0, result[0], 0, index);
	    final int len = c.length - (index + 1);
	    result[1] = new byte[len];
	    System.arraycopy(c, index + 1 /* Skip delimiter */, result[1], 0, len);
	    return result;
	  }
	  
	  /**
	   * @param b
	   * @param delimiter
	   * @return Index of delimiter having started from start of <code>b</code>
	   * moving rightward.
	   */
	  public static int getDelimiter(final byte [] b, int offset, final int length,
	      final int delimiter) {
	    if (b == null) {
	      throw new IllegalArgumentException("Passed buffer is null");
	    }
	    int result = -1;
	    for (int i = offset; i < length + offset; i++) {
	      if (b[i] == delimiter) {
	        result = i;
	        break;
	      }
	    }
	    return result;
	  }
	  
	  /**************** equals ****************************/

	  public static boolean equals(Cell a, Cell b) {
	    return matchingRows(a, b) && matchingFamily(a, b) && matchingQualifier(a, b)
	        && matchingTimestamp(a, b) && PrivateCellUtil.matchingType(a, b);
	  }

	  public static boolean matchingTimestamp(Cell a, Cell b) {
	    return CellComparatorImpl.COMPARATOR.compareTimestamps(a.getTimestamp(), b.getTimestamp()) == 0;
	  }

	  public static boolean matchingRows(final Cell left, final byte[] buf) {
	    if (buf == null) {
	      return left.getRowLength() == 0;
	    }
	    return PrivateCellUtil.matchingRows(left, buf, 0, buf.length);
	  }

	  /**
	   * Compares the row of two keyvalues for equality
	   * @param left
	   * @param right
	   * @return True if rows match.
	   */
	  public static boolean matchingRows(final Cell left, final Cell right) {
	    short lrowlength = left.getRowLength();
	    short rrowlength = right.getRowLength();
	    if (lrowlength != rrowlength) return false;
	    return Bytes.equals(left.getRowArray(), left.getRowOffset(), lrowlength, right.getRowArray(),
	      right.getRowOffset(), rrowlength);
	  }

	  public static boolean matchingFamily(final Cell left, final Cell right) {
		    byte lfamlength = left.getFamilyLength();
		    byte rfamlength = right.getFamilyLength();
		    return Bytes.equals(left.getFamilyArray(), left.getFamilyOffset(), lfamlength,
		      right.getFamilyArray(), right.getFamilyOffset(), rfamlength);
		  }

		  public static boolean matchingFamily(final Cell left, final byte[] buf) {
		    if (buf == null) {
		      return left.getFamilyLength() == 0;
		    }
		    return PrivateCellUtil.matchingFamily(left, buf, 0, buf.length);
		  }

	  public static boolean matchingQualifier(final Cell left, final Cell right) {
	    int lqlength = left.getQualifierLength();
	    int rqlength = right.getQualifierLength();
	    return Bytes.equals(left.getQualifierArray(), left.getQualifierOffset(), lqlength,
	      right.getQualifierArray(), right.getQualifierOffset(), rqlength);
	  }
}
