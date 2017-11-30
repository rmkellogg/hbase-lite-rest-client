package org.apache.hadoop.hbase.client.lite;

import java.security.Principal;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.apache.http.auth.AuthSchemeProvider;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.Credentials;
import org.apache.http.client.HttpClient;
import org.apache.http.client.config.AuthSchemes;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.config.Lookup;
import org.apache.http.config.RegistryBuilder;
import org.apache.http.impl.auth.SPNegoSchemeFactory;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.client.HttpClientBuilder;

public abstract class BaseHBaseBuilder 
{
	private static final Credentials CREDENTIALS = new NullCredentials();
	
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
	/**
	 * Apache HttpClient
	 */
	protected HttpClient httpClient;
	/**
	 * Use Kerberos context during Http Request
	 */
	protected boolean useKerberos;
	/**
	 * Kerberos Keytab file location (ignored if useKerberos is not true)
	 */
	protected String keyTabLocation;
	/**
	 * Kerberos User Principal (ignored if useKerberos is not true)
	 */
	protected String userPrincipal;
	
	protected HttpClient buildHttpClient()
	{
		// Establish timeout configuration
		RequestConfig config = RequestConfig.custom()
				.setConnectTimeout(connectionTimeout)
				.setConnectionRequestTimeout(connectionTimeout)
				.setSocketTimeout(connectionTimeout).build();
		
		HttpClientBuilder builder = HttpClientBuilder.create();
		
		builder.setDefaultRequestConfig(config);
		
		// Enable Kerberos authentication
		if (useKerberos)
		{
			Lookup<AuthSchemeProvider> authSchemeRegistry = RegistryBuilder.<AuthSchemeProvider> create()
																.register(AuthSchemes.SPNEGO, new SPNegoSchemeFactory(true))
																.build();
			builder.setDefaultAuthSchemeRegistry(authSchemeRegistry);
			BasicCredentialsProvider credentialsProvider = new BasicCredentialsProvider();
			credentialsProvider.setCredentials(new AuthScope(null, -1, null), CREDENTIALS);
			builder.setDefaultCredentialsProvider(credentialsProvider);
		}
		
		return builder.build();
	}
	
	private static class NullCredentials implements Credentials {
		@Override
		public Principal getUserPrincipal() {
			return null;
		}

		@Override
		public String getPassword() {
			return null;
		}
	}
}
