package org.apache.hadoop.hbase.client.lite;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.apache.http.client.HttpClient;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.impl.client.HttpClientBuilder;

public abstract class BaseHBaseBuilder 
{
	public static final int DEFAULT_MAX_RETRIES = 10;
	public static final long DEFAULT_SLEEP_TIME = 1000;
	public static final int DEFAULT_CONNECTION_TIMEOUT = 1000;
	
	/**
	 * Number of times to attempt request
	 */
	protected int maxRetries = DEFAULT_MAX_RETRIES;
	/**
	 * Sleet time between requests on connection failure
	 */
	protected long sleepTime = DEFAULT_SLEEP_TIME;
	/**
	 * Protocol used in creation of URL, i.e. http or https
	 */
	protected String protocol = "http";
	/**
	 * Connection timeout in milliseconds
	 */
	protected int connectionTimeout = DEFAULT_CONNECTION_TIMEOUT; 
	/**
	 * List of host names and port, i.e. hostname1:8080
	 */
	protected List<String> hosts = new ArrayList<String>();
	/**
	 * Extra headers added to the request
	 */
	protected Map<String, String> extraHeaders = new TreeMap<String, String>();
	/*
	 * Apache HttpClient
	 */
	protected HttpClient httpClient;

	protected HttpClient buildHttpClient()
	{
		// Establish timeout configuration
		RequestConfig config = RequestConfig.custom()
				.setConnectTimeout(connectionTimeout)
				.setConnectionRequestTimeout(connectionTimeout)
				.setSocketTimeout(connectionTimeout).build();
		
		return HttpClientBuilder.create()
				  				.setDefaultRequestConfig(config)
								.build();
	}
}
