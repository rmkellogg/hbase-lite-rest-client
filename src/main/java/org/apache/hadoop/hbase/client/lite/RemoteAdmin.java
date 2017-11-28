/**
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
