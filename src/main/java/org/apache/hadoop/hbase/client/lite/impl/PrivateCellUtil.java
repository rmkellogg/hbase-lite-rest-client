package org.apache.hadoop.hbase.client.lite.impl;

import org.apache.hadoop.hbase.client.lite.Cell;
import org.apache.hadoop.hbase.client.lite.impl.KeyValue.Type;

public class PrivateCellUtil {
	  public static Cell createFirstOnRow(final byte[] row, int roffset, short rlength,
		      final byte[] family, int foffset, byte flength, final byte[] col, int coffset, int clength) {
		    return new FirstOnRowColCell(row, roffset, rlength, family, foffset, flength, col, coffset,
		        clength);
		  }
	  public static boolean matchingType(Cell a, Cell b) {
		    return a.getTypeByte() == b.getTypeByte();
		  }

	  public static boolean matchingRows(final Cell left, final byte[] buf, final int offset,
		      final int length) {
		    return Bytes.equals(left.getRowArray(), left.getRowOffset(), left.getRowLength(), buf, offset,
		      length);
		  }

	  public static boolean matchingFamily(final Cell left, final byte[] buf, final int offset,
		      final int length) {
		    return Bytes.equals(left.getFamilyArray(), left.getFamilyOffset(), left.getFamilyLength(), buf,
		      offset, length);
	  }

	  /**
	   * Finds if the qualifier part of the cell and the KV serialized byte[] are equal
	   * @param left
	   * @param buf the serialized keyvalue format byte[]
	   * @param offset the offset of the qualifier in the byte[]
	   * @param length the length of the qualifier in the byte[]
	   * @return true if the qualifier matches, false otherwise
	   */
	  public static boolean matchingQualifier(final Cell left, final byte[] buf, final int offset,
	      final int length) {
	    if (buf == null) {
	      return left.getQualifierLength() == 0;
	    }
	    return Bytes.equals(left.getQualifierArray(), left.getQualifierOffset(),
	      left.getQualifierLength(), buf, offset, length);
	  }

	  /**
	   * These cells are used in reseeks/seeks to improve the read performance. They are not real cells
	   * that are returned back to the clients
	   */
	  private static abstract class EmptyCell implements Cell {
	    @Override
	    public byte[] getRowArray() {
	      return HConstants.EMPTY_BYTE_ARRAY;
	    }

	    @Override
	    public int getRowOffset() {
	      return 0;
	    }

	    @Override
	    public short getRowLength() {
	      return 0;
	    }

	    @Override
	    public byte[] getFamilyArray() {
	      return HConstants.EMPTY_BYTE_ARRAY;
	    }

	    @Override
	    public int getFamilyOffset() {
	      return 0;
	    }

	    @Override
	    public byte getFamilyLength() {
	      return 0;
	    }

	    @Override
	    public byte[] getQualifierArray() {
	      return HConstants.EMPTY_BYTE_ARRAY;
	    }

	    @Override
	    public int getQualifierOffset() {
	      return 0;
	    }

	    @Override
	    public int getQualifierLength() {
	      return 0;
	    }

	    @Override
	    public long getSequenceId() {
	      return 0;
	    }

	    @Override
	    public byte[] getValueArray() {
	      return HConstants.EMPTY_BYTE_ARRAY;
	    }

	    @Override
	    public int getValueOffset() {
	      return 0;
	    }

	    @Override
	    public int getValueLength() {
	      return 0;
	    }

	    @Override
	    public byte[] getTagsArray() {
	      return HConstants.EMPTY_BYTE_ARRAY;
	    }

	    @Override
	    public int getTagsOffset() {
	      return 0;
	    }

	    @Override
	    public int getTagsLength() {
	      return 0;
	    }
	  }

	  private static class FirstOnRowCell extends EmptyCell {
	    private final byte[] rowArray;
	    private final int roffset;
	    private final short rlength;

	    public FirstOnRowCell(final byte[] row, int roffset, short rlength) {
	      this.rowArray = row;
	      this.roffset = roffset;
	      this.rlength = rlength;
	    }

	    @Override
	    public byte[] getRowArray() {
	      return this.rowArray;
	    }

	    @Override
	    public int getRowOffset() {
	      return this.roffset;
	    }

	    @Override
	    public short getRowLength() {
	      return this.rlength;
	    }

	    @Override
	    public long getTimestamp() {
	      return HConstants.LATEST_TIMESTAMP;
	    }

	    @Override
	    public byte getTypeByte() {
	      return Type.Maximum.getCode();
	    }
	  }
	  
	  private static class FirstOnRowColCell extends FirstOnRowCell {
		    private final byte[] fArray;
		    private final int foffset;
		    private final byte flength;
		    private final byte[] qArray;
		    private final int qoffset;
		    private final int qlength;

		    public FirstOnRowColCell(byte[] rArray, int roffset, short rlength, byte[] fArray, int foffset,
		        byte flength, byte[] qArray, int qoffset, int qlength) {
		      super(rArray, roffset, rlength);
		      this.fArray = fArray;
		      this.foffset = foffset;
		      this.flength = flength;
		      this.qArray = qArray;
		      this.qoffset = qoffset;
		      this.qlength = qlength;
		    }

		    @Override
		    public byte[] getFamilyArray() {
		      return this.fArray;
		    }

		    @Override
		    public int getFamilyOffset() {
		      return this.foffset;
		    }

		    @Override
		    public byte getFamilyLength() {
		      return this.flength;
		    }

		    @Override
		    public byte[] getQualifierArray() {
		      return this.qArray;
		    }

		    @Override
		    public int getQualifierOffset() {
		      return this.qoffset;
		    }

		    @Override
		    public int getQualifierLength() {
		      return this.qlength;
		    }
		  }	  
}
