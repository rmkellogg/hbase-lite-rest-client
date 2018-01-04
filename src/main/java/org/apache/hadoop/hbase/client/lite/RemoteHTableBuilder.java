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

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hbase.client.lite.impl.Client;
import org.apache.hadoop.hbase.client.lite.impl.Cluster;
import org.apache.hadoop.hbase.client.lite.impl.RemoteHTableImpl;
import org.apache.http.client.HttpClient;

/**
 * Fluent API for construction of RemoteHTable.
 * 
 * <pre>
 * Example:
 * HttpClient httpClient = HttpClientBuilder.create().build();
 *				
 * RemoteHTable table = RemoteHTableBuilder.create("namespace:tablename")
 *							.addHost("hostname:8080")
 *							.withProtocol("https")
 *							.withMaxRetries(10)
 *							.withSleepTime(1000)
 *                          .withAllowSelfSignedCertificates(false)
 *							.withHttpClient(httpClient) // Normally not required
 *							.build();
 * </pre>
 */
public class RemoteHTableBuilder extends BaseHBaseBuilder
{
	/**
	 * Name of the table for operation execution
	 */
	private String tableName;
	
	private RemoteHTableBuilder(final String tableName)
	{
		this.tableName = tableName;
	}
	
	public static RemoteHTableBuilder create(final String tableName)
	{
		RemoteHTableBuilder builder = new RemoteHTableBuilder(tableName);
		
		return builder;
	}

	public RemoteHTable build()
	throws IOException
	{
		Cluster cluster = new Cluster();
		HttpClient tempHttpClient = httpClient;
		
		if (tempHttpClient == null)
		{
			tempHttpClient = buildHttpClient();
		}
		
		Client client = new Client(cluster, protocol, tempHttpClient, useKerberos, userPrincipal, keyTabLocation);
		
		if (hosts.isEmpty())
		{
			throw new IllegalArgumentException("At least one host required.");
		}
		else
		{
			for(String host : hosts)
			{
				cluster.add(host);
			}
		}
		
		if (!extraHeaders.isEmpty())
		{
			for(String header: extraHeaders.keySet())
			{
				client.addExtraHeader(header, extraHeaders.get(header));
			}
		}
		
		RemoteHTableImpl result = new RemoteHTableImpl(client, tableName, maxRetries, sleepTime);

		return result;
	}

	public RemoteHTableBuilder withProtocol(final String protocol)
	{
		this.protocol = StringUtils.trimToNull(protocol);
		
		return this;
	}
	
	public RemoteHTableBuilder withUserPrincipal(String userPrincipal)
	{
		this.userPrincipal = StringUtils.trimToNull(userPrincipal);
		
		return this;
	}
	
	public RemoteHTableBuilder withKeyTabLocation(String keyTabLocation)
	{
		this.keyTabLocation = StringUtils.trimToNull(keyTabLocation);
		
		return this;
	}

	public RemoteHTableBuilder withUseKerberos(boolean useKerberos)
	{
		this.useKerberos = useKerberos;
		
		return this;
	}

	public RemoteHTableBuilder withHttpClient(HttpClient httpClient)
	{
		this.httpClient = httpClient;
		
		return this;
	}
	
	public RemoteHTableBuilder withMaxRetries(int maxRetries)
	{
		this.maxRetries = maxRetries;
		
		return this;
	}
	
	public RemoteHTableBuilder withSleepTime(int sleepTime)
	{
		this.sleepTime = sleepTime;
		
		return this;
	}
	
	public RemoteHTableBuilder withConnectionTimeout(int connectionTimeout)
	{
		this.connectionTimeout = connectionTimeout;
		
		return this;
	}

	public RemoteHTableBuilder withAllowSelfSignedCertificates(boolean allowSelfSignedCertificates)
	{
		this.allowSelfSignedCerts = allowSelfSignedCertificates;
		
		return this;
	}

	public RemoteHTableBuilder addHost(final String hostName)
	{
		this.hosts.add(hostName);
		
		return this;
	}
	
	public RemoteHTableBuilder addExtraHeader(final String headerName, final String headerValue)
	{
		this.extraHeaders.put(headerName,  headerValue);
		
		return this;
	}
}
