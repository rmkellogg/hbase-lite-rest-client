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

import org.apache.commons.codec.binary.Base64;
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
 *                          // Set these for use of Kerberos with Principal and Keytab
 *							//.withUseKerberos("hbase/hostname@REALM.COM","/etc/security/keytabs/hbase.security.keytab")
 *  
 *                          // Set these for use of Kerberos with external kinit
 *							//.withUseKerberos()
 *
 *                          // Set these for use of Kerberos with external JAAS configuration
 *							//.withUseJAAS("Client")
 *
 *                          // Set these for use of Preemptive Basic Authentication
 *							//.withUsePreemptiveBasicAuthentication("hbase-user","hbase-password")
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
		
		Client client = new Client(cluster, protocol, tempHttpClient, useKerberos, jaasEntryName, userPrincipal, keyTabLocation);
		
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
	 * Protocol used in creation of URL
	 * 
	 * @param protocol Protocol (required) either http or https
	 * 
  	 * @return RemoteAdminBuilder
	 */
	public RemoteAdminBuilder withProtocol(final String protocol)
	{
		this.protocol = StringUtils.trimToNull(protocol);
		
		return this;
	}
	
	/**
	 * Use Kerberos context during Http Request with external kinit
	 * 
  	 * @return RemoteAdminBuilder
	 */	
	public RemoteAdminBuilder withUseKerberos()
	{
		this.useKerberos = true;
		
		return this;
	}

	/**
	 * Use Kerberos context during Http Request with user supplied principal and keytab location.
	 * 
	 * @param userPrincipal User Principal (required) (hbase/hostname@REALM.COM)
	 * @param keyTabLocation Kerberos Keytab Location (required) (/etc/security/keytabs/hbase.security.keytab)
	 * 
  	 * @return RemoteAdminBuilder
	 */	
	public RemoteAdminBuilder withUseKerberos(String userPrincipal, String keyTabLocation)
	{
		if ((userPrincipal != null) && (keyTabLocation != null))
		{
			this.useKerberos = true;
			this.userPrincipal = StringUtils.trimToNull(userPrincipal);
			this.keyTabLocation = StringUtils.trimToNull(keyTabLocation);
		}
		
		return this;
	}

	/**
	 * Use Preemptive Basic Authentication 
	 * 
	 * @param userName User Name (required)
	 * @param password Password (required)
	 * 
	 * @return RemoteAdminBuilder
	 */
	public RemoteAdminBuilder withUsePreemptiveBasicAuthentication(String userName, String password)
	{
		if ((userName != null) && (password != null))
		{
			String authHeader = Base64.encodeBase64String((userName + ":" + password).getBytes());

			addExtraHeader("Authorization", "Basic " + authHeader);
		}
		
		return this;
	}
	
	/**
	 * Use external JAAS configuration for Kerberos configuration
	 * 
	 * @param jaasEntry Name of JAAS Entry to use for login
	 * 
  	 * @return RemoteAdminBuilder
	 */	
	public RemoteAdminBuilder withUseJAAS(String jaasEntryName)
	{
		this.jaasEntryName = StringUtils.trimToNull(jaasEntryName);
		
		return this;
	}
	
	/**
	 * Externally configured Apache HttpClient
	 * 
  	 * @return RemoteAdminBuilder
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
	 * 
  	 * @return RemoteAdminBuilder
	 */
	public RemoteAdminBuilder withMaxRetries(int maxRetries)
	{
		this.maxRetries = maxRetries;
		
		return this;
	}
	
	/**
	 * Sleep time between requests on connection failure
	 * 
  	 * @return RemoteAdminBuilder
	 */
	public RemoteAdminBuilder withSleepTime(int sleepTime)
	{
		this.sleepTime = sleepTime;
		
		return this;
	}
	
	/**
	 * Connection timeout in milliseconds
	 * 
  	 * @return RemoteAdminBuilder
	 */
	public RemoteAdminBuilder withConnectionTimeout(int connectionTimeout)
	{
		this.connectionTimeout = connectionTimeout;
		
		return this;
	}
	
	/**
	 * Allow use of self-signed SSL certificates
	 * 
  	 * @return RemoteAdminBuilder
	 */	
	public RemoteAdminBuilder withAllowSelfSignedCertificates(boolean allowSelfSignedCertificates)
	{
		this.allowSelfSignedCerts = allowSelfSignedCertificates;
		
		return this;
	}
	
	/**
	 * Host name and port 
	 * 
	 * @param hostName Hostname and port (required) (hostname1:8080)
	 * 
  	 * @return RemoteAdminBuilder
	 */	
	public RemoteAdminBuilder addHost(final String hostName)
	{
		this.hosts.add(hostName);
		
		return this;
	}
	
	/**
	 * Extra headers added to the request
	 * 
	 * @param headerName Header Name (required)
	 * @param HeaderValue Header Value (required)
	 * 
  	 * @return RemoteAdminBuilder
	 */
	public RemoteAdminBuilder addExtraHeader(final String headerName, final String headerValue)
	{
		this.extraHeaders.put(headerName,  headerValue);
		
		return this;
	}
}
