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
import org.apache.hadoop.hbase.client.lite.impl.RemoteAdminImpl;
import org.apache.http.client.HttpClient;

/**
 * Fluent API for construction of RemoteAdmin.
 * 
 * <pre>
 * Example:
 * HttpClient httpClient = HttpClientBuilder.create().build();
 *				
 * RemoteAdminBuilder table = RemoteAdminBuilder.create("namespace:tablename")
 *							.addHost("hostname:8080")
 *							.withProtocol("https")
 *							.withMaxRetries(10)
 *							.withSleepTime(1000)
 *                          .withAllowSelfSignedCertificates(false)
 *                          
 *                          // Set these for use of Kerberos, Principal and Keytab
 *							//.withUseKerberos(true)
 *							//.withKeyTabLocation("/etc/security/keytabs/hbase.security.keytab")
 *  						//.withUserPrincipal("hbase/hostname@REALM.COM")
 *
 *                          // Set these for use of Kerberos and external kinit
 *							//.withUseKerberos(true)
 *  
 *                          // With explicit HttpClient but normally not required                            
 *                          //.withHttpClient(httpClient)
 *                           
 *							.build();
 * </pre>
 */
public class RemoteAdminBuilder extends BaseHBaseBuilder
{
	/**
	 * Access token
	 */
	private String accessToken;
	
	public static RemoteAdminBuilder create()
	{
		RemoteAdminBuilder builder = new RemoteAdminBuilder();
		
		return builder;
	}

	public RemoteAdmin build()
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
		
		RemoteAdminImpl result = new RemoteAdminImpl(client, accessToken, maxRetries, sleepTime);

		return result;
	}

	/**
	 * Protocol used in creation of URL, i.e. http or https
	 */
	public RemoteAdminBuilder withProtocol(final String protocol)
	{
		this.protocol = StringUtils.trimToNull(protocol);
		
		return this;
	}
	
	/**
	 * Kerberos User Principal (ignored if useKerberos is not true)
	 */
	public RemoteAdminBuilder withUserPrincipal(String userPrincipal)
	{
		this.userPrincipal = StringUtils.trimToNull(userPrincipal);
		
		return this;
	}
	
	/**
	 * Kerberos Keytab file location (ignored if useKerberos is not true)
	 */	
	public RemoteAdminBuilder withKeyTabLocation(String keyTabLocation)
	{
		this.keyTabLocation = StringUtils.trimToNull(keyTabLocation);
		
		return this;
	}
	
	/**
	 * Use Kerberos context during Http Request
	 */	
	public RemoteAdminBuilder withUseKerberos(boolean useKerberos)
	{
		this.useKerberos = useKerberos;
		
		return this;
	}
	
	/**
	 * Externally configured Apache HttpClient
	 */
	public RemoteAdminBuilder withHttpClient(HttpClient httpClient)
	{
		this.httpClient = httpClient;
		
		return this;
	}
	
	public RemoteAdminBuilder withAccessToken(String accessToken)
	{
		this.accessToken = StringUtils.trimToNull(accessToken);
		
		return this;
	}
	
	/**
	 * Number of times to attempt request
	 */
	public RemoteAdminBuilder withMaxRetries(int maxRetries)
	{
		this.maxRetries = maxRetries;
		
		return this;
	}
	
	/**
	 * Sleet time between requests on connection failure
	 */
	public RemoteAdminBuilder withSleepTime(int sleepTime)
	{
		this.sleepTime = sleepTime;
		
		return this;
	}
	
	/**
	 * Connection timeout in milliseconds
	 */
	public RemoteAdminBuilder withConnectionTimeout(int connectionTimeout)
	{
		this.connectionTimeout = connectionTimeout;
		
		return this;
	}
	
	/**
	 * Allow use of self-signed SSL certificates
	 */	
	public RemoteAdminBuilder withAllowSelfSignedCertificates(boolean allowSelfSignedCertificates)
	{
		this.allowSelfSignedCerts = allowSelfSignedCertificates;
		
		return this;
	}
	
	/**
	 * Host name and port, i.e. hostname1:8080
	 */	
	public RemoteAdminBuilder addHost(final String hostName)
	{
		this.hosts.add(hostName);
		
		return this;
	}
	
	/**
	 * Extra headers added to the request
	 */
	public RemoteAdminBuilder addExtraHeader(final String headerName, final String headerValue)
	{
		this.extraHeaders.put(headerName,  headerValue);
		
		return this;
	}
}
