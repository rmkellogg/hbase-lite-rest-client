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

import java.io.IOException;
import java.io.InterruptedIOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hbase.client.lite.RemoteAdmin;
import org.apache.hadoop.hbase.client.lite.impl.model.TableListModel;
import org.apache.hadoop.hbase.client.lite.impl.model.TableModel;
import org.apache.hadoop.hbase.client.lite.impl.model.VersionModel;

public class RemoteAdminImpl implements RemoteAdmin {

 final Client client;
 final String accessToken;
 final int maxRetries;
 final long sleepTime;

 /**
  * Constructor
  * @param client
  * @param conf
  * @param accessToken
  */
 public RemoteAdminImpl(Client client, String accessToken, int maxRetries, long sleepTime) {
   this.client = client;
   this.accessToken = accessToken;
   this.maxRetries = maxRetries;
   this.sleepTime = sleepTime;
 }

 /**
  * @param tableName name of table to check
  * @return true if all regions of the table are available
  * @throws IOException if a remote or network exception occurs
  */
 public boolean isTableAvailable(String tableName) throws IOException {
   return isTableAvailable(Bytes.toBytes(tableName));
 }

 /**
  * @return string representing the rest api's version
  * @throws IOException
  *           if the endpoint does not exist, there is a timeout, or some other
  *           general failure mode
  */
 public String getRestVersion() throws IOException {

   StringBuilder path = new StringBuilder();
   path.append('/');
   if (accessToken != null) {
     path.append(accessToken);
     path.append('/');
   }

   path.append("version/rest");

   int code = 0;
   for (int i = 0; i < maxRetries; i++) {
     Response response = client.get(path.toString(),
         Constants.MIMETYPE_PROTOBUF);
     code = response.getCode();
     switch (code) {
     case 200:

       VersionModel v = new VersionModel();
       v.getObjectFromMessage(response.getBody());
       return v.getRESTVersion();
     case 404:
       throw new IOException("REST version not found");
     case 509:
       try {
         Thread.sleep(sleepTime);
       } catch (InterruptedException e) {
         throw (InterruptedIOException)new InterruptedIOException().initCause(e);
       }
       break;
     default:
       throw new IOException("get request to " + path.toString()
           + " returned " + code);
     }
   }
   throw new IOException("get request to " + path.toString() + " timed out");
 }

 /**
  * @param tableName name of table to check
  * @return true if all regions of the table are available
  * @throws IOException if a remote or network exception occurs
  */
 public boolean isTableAvailable(byte[] tableName) throws IOException {
   StringBuilder path = new StringBuilder();
   path.append('/');
   if (accessToken != null) {
     path.append(accessToken);
     path.append('/');
   }
   path.append(Bytes.toStringBinary(tableName));
   path.append('/');
   path.append("exists");
   int code = 0;
   for (int i = 0; i < maxRetries; i++) {
     Response response = client.get(path.toString(), Constants.MIMETYPE_PROTOBUF);
     code = response.getCode();
     switch (code) {
     case 200:
       return true;
     case 404:
       return false;
     case 509:
       try {
         Thread.sleep(sleepTime);
       } catch (InterruptedException e) {
         throw (InterruptedIOException)new InterruptedIOException().initCause(e);
       }
       break;
     default:
       throw new IOException("get request to " + path.toString() + " returned " + code);
     }
   }
   throw new IOException("get request to " + path.toString() + " timed out");
 }

 /**
  * @return string representing the cluster's version
  * @throws IOException
  *           if the endpoint does not exist, there is a timeout, or some other
  *           general failure mode
  */
 public List<String> getTableList() throws IOException {

   StringBuilder path = new StringBuilder();
   path.append('/');
   if (accessToken != null) {
     path.append(accessToken);
     path.append('/');
   }

   int code = 0;
   for (int i = 0; i < maxRetries; i++) {
     Response response = client.get(path.toString(),
         Constants.MIMETYPE_PROTOBUF);
     code = response.getCode();
     switch (code) {
     case 200:
       TableListModel t = new TableListModel();
       //return (TableListModel) t.getObjectFromMessage(response.getBody());
       t.getObjectFromMessage(response.getBody());
       
       List<String> result = new ArrayList<String>();
       for(TableModel table : t.getTables())
       {
    	   result.add(table.getName());
       }
       
       return result;
     case 404:
       throw new IOException("Table list not found");
     case 509:
       try {
         Thread.sleep(sleepTime);
       } catch (InterruptedException e) {
         throw (InterruptedIOException)new InterruptedIOException().initCause(e);
       }
       break;
     default:
       throw new IOException("get request to " + path.toString()
           + " request returned " + code);
     }
   }
   throw new IOException("get request to " + path.toString()
       + " request timed out");
 }
}
