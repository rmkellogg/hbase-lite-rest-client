package org.apache.hadoop.hbase.client.lite;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;

/**
 * Minimal Table interface to remote tables accessed via REST gateway
 * 
 * Use RemoteTHableBuilder for construction
 */
public interface RemoteHTable extends AutoCloseable, Closeable
{
	  /**
	   * Gets the fully qualified table name instance of this table.
	   */
	String getName();
	
	  /**
	   * Test for the existence of columns in the table, as specified by the Get.
	   * <p>
	   *
	   * This will return true if the Get matches one or more keys, false if not.
	   * <p>
	   *
	   * This is a server-side call so it prevents any data from being transfered to
	   * the client.
	   *
	   * @param get the Get
	   * @return true if the specified Get matches one or more keys, false if not
	   * @throws IOException e
	   */
	boolean exists(Get paramGet) throws IOException;
	
	  /**
	   * Test for the existence of columns in the table, as specified by the Gets.
	   * <p>
	   *
	   * This will return an array of booleans. Each value will be true if the related Get matches
	   * one or more keys, false if not.
	   * <p>
	   *
	   * This is a server-side call so it prevents any data from being transferred to
	   * the client.
	   *
	   * @param gets the Gets
	   * @return Array of boolean.  True if the specified Get matches one or more keys, false if not.
	   * @throws IOException e
	   */
	boolean[] exists(List<Get> paramList) throws IOException;
	
	  /**
	   * Extracts certain cells from a given row.
	   * @param get The object that specifies what data to fetch and from which row.
	   * @return The data coming from the specified row, if it exists.  If the row
	   * specified doesn't exist, the {@link Result} instance returned won't
	   * contain any {@link org.apache.hadoop.hbase.KeyValue}, as indicated by {@link Result#isEmpty()}.
	   * @throws IOException if a remote or network exception occurs.
	   */
	Result get(Get paramGet) throws IOException;
	
	  /**
	   * Extracts specified cells from the given rows, as a batch.
	   *
	   * @param gets The objects that specify what data to fetch and from which rows.
	   * @return The data coming from the specified rows, if it exists.  If the row specified doesn't
	   * exist, the {@link Result} instance returned won't contain any {@link
	   * org.apache.hadoop.hbase.Cell}s, as indicated by {@link Result#isEmpty()}. If there are any
	   * failures even after retries, there will be a <code>null</code> in the results' array for those
	   * Gets, AND an exception will be thrown. The ordering of the Result array corresponds to the order
	   * of the list of passed in Gets.
	   * @throws IOException if a remote or network exception occurs.
	   * @apiNote {@link #put(List)} runs pre-flight validations on the input list on client.
	   * Currently {@link #get(List)} doesn't run any validations on the client-side, currently there
	   * is no need, but this may change in the future. An
	   * {@link IllegalArgumentException} will be thrown in this case.
	   */
	Result[] get(List<Get> paramList) throws IOException;

	  /**
	   * Returns a scanner on the current table as specified by the {@link Scan}
	   * object.
	   * Note that the passed {@link Scan}'s start row and caching properties
	   * maybe changed.
	   *
	   * @param scan A configured {@link Scan} object.
	   * @return A scanner.
	   * @throws IOException if a remote or network exception occurs.
	   */
		ResultScanner getScanner(Scan paramScan) throws IOException;

	  /**
	   * Gets a scanner on the current table for the given family.
	   *
	   * @param family The column family to scan.
	   * @return A scanner.
	   * @throws IOException if a remote or network exception occurs.
	   */
	  ResultScanner getScanner(byte[] family) throws IOException;

	  /**
	   * Gets a scanner on the current table for the given family and qualifier.
	   *
	   * @param family The column family to scan.
	   * @param qualifier The column qualifier to scan.
	   * @return A scanner.
	   * @throws IOException if a remote or network exception occurs.
	   */
	  ResultScanner getScanner(byte[] family, byte[] qualifier) throws IOException;

	  /**
	   * Puts some data in the table.
	   *
	   * @param put The data to put.
	   * @throws IOException if a remote or network exception occurs.
	   */
	  void put(Put put) throws IOException;

	  /**
	   * Batch puts the specified data into the table.
	   * <p>
	   * This can be used for group commit, or for submitting user defined batches. Before sending
	   * a batch of mutations to the server, the client runs a few validations on the input list. If an
	   * error is found, for example, a mutation was supplied but was missing it's column an
	   * {@link IllegalArgumentException} will be thrown and no mutations will be applied. If there
	   * are any failures even after retries, a {@link RetriesExhaustedWithDetailsException} will be
	   * thrown. RetriesExhaustedWithDetailsException contains lists of failed mutations and
	   * corresponding remote exceptions. The ordering of mutations and exceptions in the
	   * encapsulating exception corresponds to the order of the input list of Put requests.
	   *
	   * @param puts The list of mutations to apply.
	   * @throws IOException if a remote or network exception occurs.
	   */
	  void put(List<Put> puts) throws IOException;

	  /**
	   * Atomically checks if a row/family/qualifier value matches the expected
	   * value. If it does, it adds the put.  If the passed value is null, the check
	   * is for the lack of column (ie: non-existance)
	   *
	   * @param row to check
	   * @param family column family to check
	   * @param qualifier column qualifier to check
	   * @param value the expected value
	   * @param put data to put if check succeeds
	   * @throws IOException e
	   * @return true if the new put was executed, false otherwise
	   */
	  boolean checkAndPut(byte[] row, byte[] family, byte[] qualifier, byte[] value, Put put) throws IOException;
	  
	  /**
	   * Deletes the specified cells/row.
	   *
	   * @param delete The object that specifies what to delete.
	   * @throws IOException if a remote or network exception occurs.
	   */
	  void delete(Delete delete) throws IOException;

	  /**
	   * Batch Deletes the specified cells/rows from the table.
	   * <p>
	   * If a specified row does not exist, {@link Delete} will report as though sucessful
	   * delete; no exception will be thrown. If there are any failures even after retries,
	   * a * {@link RetriesExhaustedWithDetailsException} will be thrown.
	   * RetriesExhaustedWithDetailsException contains lists of failed {@link Delete}s and
	   * corresponding remote exceptions.
	   *
	   * @param deletes List of things to delete. The input list gets modified by this
	   * method. All successfully applied {@link Delete}s in the list are removed (in particular it
	   * gets re-ordered, so the order in which the elements are inserted in the list gives no
	   * guarantee as to the order in which the {@link Delete}s are executed).
	   * @throws IOException if a remote or network exception occurs. In that case
	   * the {@code deletes} argument will contain the {@link Delete} instances
	   * that have not be successfully applied.
	   * @apiNote In 3.0.0 version, the input list {@code deletes} will no longer be modified. Also,
	   * {@link #put(List)} runs pre-flight validations on the input list on client. Currently
	   * {@link #delete(List)} doesn't run validations on the client, there is no need currently,
	   * but this may change in the future. An * {@link IllegalArgumentException} will be thrown
	   * in this case.
	   */
	  void delete(List<Delete> deletes) throws IOException;

	  /**
	   * Atomically checks if a row/family/qualifier value matches the expected
	   * value. If it does, it adds the delete.  If the passed value is null, the
	   * check is for the lack of column (ie: non-existance)
	   *
	   * @param row to check
	   * @param family column family to check
	   * @param qualifier column qualifier to check
	   * @param value the expected value
	   * @param delete data to delete if check succeeds
	   * @throws IOException e
	   * @return true if the new delete was executed, false otherwise
	   */
	  boolean checkAndDelete(byte[] row, byte[] family, byte[] qualifier, byte[] value, Delete delete) throws IOException;
	  
	  /**
	   * Releases any resources held or pending changes in internal buffers.
	   *
	   * @throws IOException if a remote or network exception occurs.
	   */
	  void close() throws IOException;
}
