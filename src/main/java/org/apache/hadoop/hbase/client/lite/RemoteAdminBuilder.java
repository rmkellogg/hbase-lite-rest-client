package org.apache.hadoop.hbase.client.lite;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.apache.hadoop.hbase.client.lite.impl.Client;
import org.apache.hadoop.hbase.client.lite.impl.Cluster;
import org.apache.hadoop.hbase.client.lite.impl.RemoteAdminImpl;
import org.apache.http.client.HttpClient;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.impl.client.HttpClientBuilder;

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
 *							.withHttpClient(httpClient)
 *							.build();
 * </pre>
 */
public class RemoteAdminBuilder 
{
	public static final int DEFAULT_MAX_RETRIES = 10;
	public static final long DEFAULT_SLEEP_TIME = 1000;
	public static final int DEFAULT_CONNECTION_TIMEOUT = 1000;
	
	/**
	 * Number of times to attempt request
	 */
	private int maxRetries = DEFAULT_MAX_RETRIES;
	/**
	 * Sleet time between requests on connection failure
	 */
	private long sleepTime = DEFAULT_SLEEP_TIME;
	/**
	 * Access token
	 */
	private String accessToken;
	/**
	 * Protocol used in creation of URL, i.e. http or https
	 */
	private String protocol = "http";
	/**
	 * Connection timeout in milliseconds
	 */
	private int connectionTimeout = DEFAULT_CONNECTION_TIMEOUT; 
	/**
	 * List of host names and port, i.e. hostname1:8080
	 */
	private List<String> hosts = new ArrayList<String>();
	/**
	 * Extra headers added to the request
	 */
	private Map<String, String> extraHeaders = new TreeMap<String, String>();
	/*
	 * Apache HttpClient
	 */
	private HttpClient httpClient;
	
	public static RemoteAdminBuilder create()
	{
		RemoteAdminBuilder builder = new RemoteAdminBuilder();
		
		return builder;
	}

	public RemoteAdmin build()
	{
		Cluster cluster = new Cluster();
		HttpClient tempHttpClient = httpClient;
		
		if (tempHttpClient == null)
		{
			// Establish timeout configuration
			RequestConfig config = RequestConfig.custom()
					.setConnectTimeout(connectionTimeout)
					.setConnectionRequestTimeout(connectionTimeout)
					.setSocketTimeout(connectionTimeout).build();
			
			tempHttpClient = HttpClientBuilder.create()
									.setDefaultRequestConfig(config)
									.build();
		}
		
		Client client = new Client(cluster, protocol, tempHttpClient);
		
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

	public RemoteAdminBuilder withProtocol(final String protocol)
	{
		this.protocol = protocol;
		
		return this;
	}
	
	public RemoteAdminBuilder withHttpClient(HttpClient httpClient)
	{
		this.httpClient = httpClient;
		
		return this;
	}
	
	public RemoteAdminBuilder withAccessToken(String accessToken)
	{
		this.accessToken = accessToken;
		
		return this;
	}
	
	public RemoteAdminBuilder withMaxRetries(int maxRetries)
	{
		this.maxRetries = maxRetries;
		
		return this;
	}
	
	public RemoteAdminBuilder withSleepTime(int sleepTime)
	{
		this.sleepTime = sleepTime;
		
		return this;
	}
	
	public RemoteAdminBuilder withConnectionTimeout(int connectionTimeout)
	{
		this.connectionTimeout = connectionTimeout;
		
		return this;
	}
	
	public RemoteAdminBuilder addHost(final String hostName)
	{
		this.hosts.add(hostName);
		
		return this;
	}
	
	public RemoteAdminBuilder addExtraHeader(final String headerName, final String headerValue)
	{
		this.extraHeaders.put(headerName,  headerValue);
		
		return this;
	}
}
