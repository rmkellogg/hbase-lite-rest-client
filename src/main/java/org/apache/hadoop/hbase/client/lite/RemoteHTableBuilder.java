package org.apache.hadoop.hbase.client.lite;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.apache.hadoop.hbase.client.lite.impl.Client;
import org.apache.hadoop.hbase.client.lite.impl.Cluster;
import org.apache.hadoop.hbase.client.lite.impl.RemoteHTableImpl;
import org.apache.http.client.HttpClient;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.impl.client.HttpClientBuilder;

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
 *							.withHttpClient(httpClient)
 *							.build();
 * </pre>
 */
public class RemoteHTableBuilder 
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
	 * Name of the table for operation execution
	 */
	private String tableName;
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
		
		RemoteHTableImpl result = new RemoteHTableImpl(client, tableName, maxRetries, sleepTime);

		return result;
	}

	public RemoteHTableBuilder withProtocol(final String protocol)
	{
		this.protocol = protocol;
		
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