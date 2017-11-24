package org.apache.hadoop.hbase.client.lite;

import java.io.IOException;
import java.util.List;

/**
 * Access minimal Admin functionality for HBase REST Server
 * 
 * Use RemoteAdminBuilder for construction
 */
public interface RemoteAdmin {
	 /**
	  * @return string representing the rest api's version
	  * @throws IOException if the endpoint does not exist, there is a timeout, or some other general failure mode
	  */
	 String getRestVersion() throws IOException;

	 /**
	  * @param tableName name of table to check
	  * @return true if all regions of the table are available
	  * @throws IOException if a remote or network exception occurs
	  */
	 boolean isTableAvailable(String tableName) throws IOException;
	 
	 /**
	  * @return List of existing tables
	  * @throws IOException if the endpoint does not exist, there is a timeout, or some other general failure mode
	  */
	 List<String> getTableList() throws IOException;
}
