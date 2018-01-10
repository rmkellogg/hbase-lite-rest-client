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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.security.Principal;
import java.security.PrivilegedExceptionAction;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import javax.security.auth.Subject;
import javax.security.auth.kerberos.KerberosPrincipal;
import javax.security.auth.login.AppConfigurationEntry;
import javax.security.auth.login.Configuration;
import javax.security.auth.login.LoginContext;
import javax.security.auth.login.LoginException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.http.Header;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpDelete;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpHead;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.http.entity.InputStreamEntity;
import org.apache.http.message.BasicHeader;
import org.apache.http.util.EntityUtils;

/**
* A wrapper around HttpClient which provides some useful function and
* semantics for interacting with the REST gateway.
*/
public class Client {
 public static final Header[] EMPTY_HEADER_ARRAY = new Header[0];

 private static final Log LOG = LogFactory.getLog(Client.class);
 
 private HttpClient httpClient;
 private Cluster cluster;
 private HttpResponse resp;
 private HttpGet httpGet = null;
 private String protocol;
 /**
  * Kerberos Keytab file location (optional)
  */
 private String keyTabLocation;
 /**
  * Kerberos User Principal (optional)
  */
 private String userPrincipal;
 /**
  * Execute Http Request using Kerberos context
  */
 private boolean useKerberos = false;
 /**
  * Name of entry in external JAAS configuration for Kerberos configuration?
  */
 private String jaasEntryName;
 
 private Map<String, String> extraHeaders = new ConcurrentHashMap<>();

 public Client(Cluster cluster, String protocol, HttpClient httpClient, boolean useKerberos, String jaasEntryName, String userPrincipal, String keyTabLocation) {
	 this.cluster = cluster;
	 this.protocol = protocol;
	 this.httpClient = httpClient;
	 this.jaasEntryName = jaasEntryName;
	 this.useKerberos = useKerberos;
	 this.userPrincipal = userPrincipal;
	 this.keyTabLocation = keyTabLocation;
 }
 
 /**
  * Shut down the client. Close any open persistent connections.
  */
 public void shutdown() {
 }

 /**
  * @return the wrapped HttpClient
  */
 public HttpClient getHttpClient() {
   return httpClient;
 }

 /**
  * Add extra headers.  These extra headers will be applied to all http
  * methods before they are removed. If any header is not used any more,
  * client needs to remove it explicitly.
  */
 public void addExtraHeader(final String name, final String value) {
   extraHeaders.put(name, value);
 }

 /**
  * Execute a transaction method given only the path. Will select at random
  * one of the members of the supplied cluster definition and iterate through
  * the list until a transaction can be successfully completed. The
  * definition of success here is a complete HTTP transaction, irrespective
  * of result code.
  * @param cluster the cluster definition
  * @param method the transaction method
  * @param headers HTTP header values to send
  * @param path the properly urlencoded path
  * @return the HTTP response code
  * @throws IOException
  */
 public HttpResponse executePathOnly(Cluster cluster, HttpUriRequest method,
     Header[] headers, String path) throws IOException {
   IOException lastException;
   if (cluster.nodes.size() < 1) {
     throw new IOException("Cluster is empty");
   }
   int start = (int)Math.round((cluster.nodes.size() - 1) * Math.random());
   int i = start;
   do {
     cluster.lastHost = cluster.nodes.get(i);
     try {
       StringBuilder sb = new StringBuilder();
       sb.append(protocol);
       sb.append("://");
       sb.append(cluster.lastHost);
       sb.append(path);
       URI uri = new URI(sb.toString());
       if (method instanceof HttpPut) {
         HttpPut put = new HttpPut(uri);
         put.setEntity(((HttpPut) method).getEntity());
         put.setHeaders(method.getAllHeaders());
         method = put;
       } else if (method instanceof HttpGet) {
         method = new HttpGet(uri);
       } else if (method instanceof HttpHead) {
         method = new HttpHead(uri);
       } else if (method instanceof HttpDelete) {
         method = new HttpDelete(uri);
       } else if (method instanceof HttpPost) {
         HttpPost post = new HttpPost(uri);
         post.setEntity(((HttpPost) method).getEntity());
         post.setHeaders(method.getAllHeaders());
         method = post;
       }
       return executeURI(method, headers, uri.toString());
     } catch (IOException e) {
       lastException = e;
     } catch (URISyntaxException use) {
       lastException = new IOException(use);
     }
   } while (++i != start && i < cluster.nodes.size());
   throw lastException;
 }

 /**
  * Execute a transaction method given a complete URI.
  * @param method the transaction method
  * @param headers HTTP header values to send
  * @param uri a properly urlencoded URI
  * @return the HTTP response code
  * @throws IOException
  */
 public HttpResponse executeURI(HttpUriRequest method, Header[] headers, String uri)
     throws IOException {
   for (Map.Entry<String, String> e: extraHeaders.entrySet()) {
     method.addHeader(e.getKey(), e.getValue());
   }
   if (headers != null) {
     for (Header header: headers) {
       method.addHeader(header);
     }
   }
   long startTime = System.currentTimeMillis();
   if (resp != null) EntityUtils.consumeQuietly(resp.getEntity());
   
   boolean useJAAS = (jaasEntryName != null);
   
   if (useKerberos || useJAAS) {
	   // Execute HTTP Operation within Kerberos Security Context
	   try {
	   	 LoginContext lc = null;
	   	 
	   	 // Use external JAAS configuration for location of principal and keytab
		 if (useJAAS) {
			lc = new LoginContext("Client");
		 } else {
			 // Either use explicit principal/keytab or previously set kinit principal
		     ClientLoginConfig loginConfig = new ClientLoginConfig(keyTabLocation, userPrincipal, null);
			 Set<Principal> princ = new HashSet<Principal>(1);
			 if (userPrincipal != null) {
				 princ.add(new KerberosPrincipal(userPrincipal));
			 }
			 
			 Subject sub = new Subject(false, princ, new HashSet<Object>(), new HashSet<Object>());
			 lc = new LoginContext("", sub, null, loginConfig);
		 }
		 
		 lc.login();
		 Subject serviceSubject = lc.getSubject();
		 return Subject.doAs(serviceSubject, new PrivilegedExceptionAction<HttpResponse>() {
				@Override
				public HttpResponse run() throws IOException {
					return httpClient.execute(method);
				}
			});
	   }
	   catch (LoginException ex)
	   {
		   if (useJAAS) { 
			   if  (System.getProperty("java.security.auth.login.config",null) == null) {
				   LOG.error("Missing JAAS Configuration, i.e. -Djava.security.auth.login.config=/etc/config/client_jaas.conf");
			   } else {
				   LOG.error("Using JAAS Config file: " + System.getProperty("java.security.auth.login.config",null));
			   }
		   } else if (useKerberos && userPrincipal == null) {
			   LOG.error("UserPrincipal not specified.  Remember to kinit prior to execution.");
		   }
 
		   throw new IOException(ex.getMessage(),ex);
	   }
	   catch (Exception ex) {
		   throw new IOException(ex.getMessage(),ex);
	   }
   } else {
     resp = httpClient.execute(method);
   }

   long endTime = System.currentTimeMillis();
   if (LOG.isTraceEnabled()) {
     LOG.trace(method.getMethod() + " " + uri + " " + resp.getStatusLine().getStatusCode() + " " +
         resp.getStatusLine().getReasonPhrase() + " in " + (endTime - startTime) + " ms");
   }
   return resp;
 }

 /**
  * Execute a transaction method. Will call either <tt>executePathOnly</tt>
  * or <tt>executeURI</tt> depending on whether a path only is supplied in
  * 'path', or if a complete URI is passed instead, respectively.
  * @param cluster the cluster definition
  * @param method the HTTP method
  * @param headers HTTP header values to send
  * @param path the properly urlencoded path or URI
  * @return the HTTP response code
  * @throws IOException
  */
 public HttpResponse execute(Cluster cluster, HttpUriRequest method, Header[] headers,
     String path) throws IOException {
   if (path.startsWith("/")) {
     return executePathOnly(cluster, method, headers, path);
   }
   return executeURI(method, headers, path);
 }

 /**
  * Send a HEAD request
  * @param path the path or URI
  * @return a Response object with response detail
  * @throws IOException
  */
 public Response head(String path) throws IOException {
   return head(cluster, path, null);
 }

 /**
  * Send a HEAD request
  * @param cluster the cluster definition
  * @param path the path or URI
  * @param headers the HTTP headers to include in the request
  * @return a Response object with response detail
  * @throws IOException
  */
 public Response head(Cluster cluster, String path, Header[] headers)
     throws IOException {
   HttpHead method = new HttpHead(path);
   try {
     HttpResponse resp = execute(cluster, method, null, path);
     return new Response(resp.getStatusLine().getStatusCode(), resp.getAllHeaders(), null);
   } finally {
     method.releaseConnection();
   }
 }

 /**
  * Send a GET request
  * @param path the path or URI
  * @return a Response object with response detail
  * @throws IOException
  */
 public Response get(String path) throws IOException {
   return get(cluster, path);
 }

 /**
  * Send a GET request
  * @param cluster the cluster definition
  * @param path the path or URI
  * @return a Response object with response detail
  * @throws IOException
  */
 public Response get(Cluster cluster, String path) throws IOException {
   return get(cluster, path, EMPTY_HEADER_ARRAY);
 }

 /**
  * Send a GET request
  * @param path the path or URI
  * @param accept Accept header value
  * @return a Response object with response detail
  * @throws IOException
  */
 public Response get(String path, String accept) throws IOException {
   return get(cluster, path, accept);
 }

 /**
  * Send a GET request
  * @param cluster the cluster definition
  * @param path the path or URI
  * @param accept Accept header value
  * @return a Response object with response detail
  * @throws IOException
  */
 public Response get(Cluster cluster, String path, String accept)
     throws IOException {
   Header[] headers = new Header[1];
   headers[0] = new BasicHeader("Accept", accept);
   return get(cluster, path, headers);
 }

 /**
  * Send a GET request
  * @param path the path or URI
  * @param headers the HTTP headers to include in the request,
  * <tt>Accept</tt> must be supplied
  * @return a Response object with response detail
  * @throws IOException
  */
 public Response get(String path, Header[] headers) throws IOException {
   return get(cluster, path, headers);
 }

 /**
  * Returns the response body of the HTTPResponse, if any, as an array of bytes.
  * If response body is not available or cannot be read, returns <tt>null</tt>
  *
  * Note: This will cause the entire response body to be buffered in memory. A
  * malicious server may easily exhaust all the VM memory. It is strongly
  * recommended, to use getResponseAsStream if the content length of the response
  * is unknown or reasonably large.
  *
  * @param resp HttpResponse
  * @return The response body, null if body is empty
  * @throws IOException If an I/O (transport) problem occurs while obtaining the
  * response body.
  */
 public static byte[] getResponseBody(HttpResponse resp) throws IOException {
   if (resp.getEntity() == null) return null;
   try (InputStream instream = resp.getEntity().getContent()) {
     if (instream != null) {
       long contentLength = resp.getEntity().getContentLength();
       if (contentLength > Integer.MAX_VALUE) {
         //guard integer cast from overflow
         throw new IOException("Content too large to be buffered: " + contentLength +" bytes");
       }
       ByteArrayOutputStream outstream = new ByteArrayOutputStream(
           contentLength > 0 ? (int) contentLength : 4*1024);
       byte[] buffer = new byte[4096];
       int len;
       while ((len = instream.read(buffer)) > 0) {
         outstream.write(buffer, 0, len);
       }
       outstream.close();
       return outstream.toByteArray();
     }
     return null;
   }
 }

 /**
  * Send a GET request
  * @param c the cluster definition
  * @param path the path or URI
  * @param headers the HTTP headers to include in the request
  * @return a Response object with response detail
  * @throws IOException
  */
 public Response get(Cluster c, String path, Header[] headers)
     throws IOException {
   if (httpGet != null) {
     httpGet.releaseConnection();
   }
   httpGet = new HttpGet(path);
   HttpResponse resp = execute(c, httpGet, headers, path);
   return new Response(resp.getStatusLine().getStatusCode(), resp.getAllHeaders(),
       resp, resp.getEntity() == null ? null : resp.getEntity().getContent());
 }

 /**
  * Send a PUT request
  * @param path the path or URI
  * @param contentType the content MIME type
  * @param content the content bytes
  * @return a Response object with response detail
  * @throws IOException
  */
 public Response put(String path, String contentType, byte[] content)
     throws IOException {
   return put(cluster, path, contentType, content);
 }

 /**
  * Send a PUT request
  * @param path the path or URI
  * @param contentType the content MIME type
  * @param content the content bytes
  * @param extraHdr extra Header to send
  * @return a Response object with response detail
  * @throws IOException
  */
 public Response put(String path, String contentType, byte[] content, Header extraHdr)
     throws IOException {
   return put(cluster, path, contentType, content, extraHdr);
 }

 /**
  * Send a PUT request
  * @param cluster the cluster definition
  * @param path the path or URI
  * @param contentType the content MIME type
  * @param content the content bytes
  * @return a Response object with response detail
  * @throws IOException for error
  */
 public Response put(Cluster cluster, String path, String contentType,
     byte[] content) throws IOException {
   Header[] headers = new Header[1];
   headers[0] = new BasicHeader("Content-Type", contentType);
   return put(cluster, path, headers, content);
 }

 /**
  * Send a PUT request
  * @param cluster the cluster definition
  * @param path the path or URI
  * @param contentType the content MIME type
  * @param content the content bytes
  * @param extraHdr additional Header to send
  * @return a Response object with response detail
  * @throws IOException for error
  */
 public Response put(Cluster cluster, String path, String contentType,
     byte[] content, Header extraHdr) throws IOException {
   int cnt = extraHdr == null ? 1 : 2;
   Header[] headers = new Header[cnt];
   headers[0] = new BasicHeader("Content-Type", contentType);
   if (extraHdr != null) {
     headers[1] = extraHdr;
   }
   return put(cluster, path, headers, content);
 }

 /**
  * Send a PUT request
  * @param path the path or URI
  * @param headers the HTTP headers to include, <tt>Content-Type</tt> must be
  * supplied
  * @param content the content bytes
  * @return a Response object with response detail
  * @throws IOException
  */
 public Response put(String path, Header[] headers, byte[] content)
     throws IOException {
   return put(cluster, path, headers, content);
 }

 /**
  * Send a PUT request
  * @param cluster the cluster definition
  * @param path the path or URI
  * @param headers the HTTP headers to include, <tt>Content-Type</tt> must be
  * supplied
  * @param content the content bytes
  * @return a Response object with response detail
  * @throws IOException
  */
 public Response put(Cluster cluster, String path, Header[] headers,
     byte[] content) throws IOException {
   HttpPut method = new HttpPut(path);
   try {
     method.setEntity(new InputStreamEntity(new ByteArrayInputStream(content), content.length));
     HttpResponse resp = execute(cluster, method, headers, path);
     headers = resp.getAllHeaders();
     content = getResponseBody(resp);
     return new Response(resp.getStatusLine().getStatusCode(), headers, content);
   } finally {
     method.releaseConnection();
   }
 }

 /**
  * Send a POST request
  * @param path the path or URI
  * @param contentType the content MIME type
  * @param content the content bytes
  * @return a Response object with response detail
  * @throws IOException
  */
 public Response post(String path, String contentType, byte[] content)
     throws IOException {
   return post(cluster, path, contentType, content);
 }

 /**
  * Send a POST request
  * @param path the path or URI
  * @param contentType the content MIME type
  * @param content the content bytes
  * @param extraHdr additional Header to send
  * @return a Response object with response detail
  * @throws IOException
  */
 public Response post(String path, String contentType, byte[] content, Header extraHdr)
     throws IOException {
   return post(cluster, path, contentType, content, extraHdr);
 }

 /**
  * Send a POST request
  * @param cluster the cluster definition
  * @param path the path or URI
  * @param contentType the content MIME type
  * @param content the content bytes
  * @return a Response object with response detail
  * @throws IOException for error
  */
 public Response post(Cluster cluster, String path, String contentType,
     byte[] content) throws IOException {
   Header[] headers = new Header[1];
   headers[0] = new BasicHeader("Content-Type", contentType);
   return post(cluster, path, headers, content);
 }

 /**
  * Send a POST request
  * @param cluster the cluster definition
  * @param path the path or URI
  * @param contentType the content MIME type
  * @param content the content bytes
  * @param extraHdr additional Header to send
  * @return a Response object with response detail
  * @throws IOException for error
  */
 public Response post(Cluster cluster, String path, String contentType,
     byte[] content, Header extraHdr) throws IOException {
   int cnt = extraHdr == null ? 1 : 2;
   Header[] headers = new Header[cnt];
   headers[0] = new BasicHeader("Content-Type", contentType);
   if (extraHdr != null) {
     headers[1] = extraHdr;
   }
   return post(cluster, path, headers, content);
 }

 /**
  * Send a POST request
  * @param path the path or URI
  * @param headers the HTTP headers to include, <tt>Content-Type</tt> must be
  * supplied
  * @param content the content bytes
  * @return a Response object with response detail
  * @throws IOException
  */
 public Response post(String path, Header[] headers, byte[] content)
     throws IOException {
   return post(cluster, path, headers, content);
 }

 /**
  * Send a POST request
  * @param cluster the cluster definition
  * @param path the path or URI
  * @param headers the HTTP headers to include, <tt>Content-Type</tt> must be
  * supplied
  * @param content the content bytes
  * @return a Response object with response detail
  * @throws IOException
  */
 public Response post(Cluster cluster, String path, Header[] headers,
     byte[] content) throws IOException {
   HttpPost method = new HttpPost(path);
   try {
     method.setEntity(new InputStreamEntity(new ByteArrayInputStream(content), content.length));
     HttpResponse resp = execute(cluster, method, headers, path);
     headers = resp.getAllHeaders();
     content = getResponseBody(resp);
     return new Response(resp.getStatusLine().getStatusCode(), headers, content);
   } finally {
     method.releaseConnection();
   }
 }

 /**
  * Send a DELETE request
  * @param path the path or URI
  * @return a Response object with response detail
  * @throws IOException
  */
 public Response delete(String path) throws IOException {
   return delete(cluster, path);
 }

 /**
  * Send a DELETE request
  * @param path the path or URI
  * @param extraHdr additional Header to send
  * @return a Response object with response detail
  * @throws IOException
  */
 public Response delete(String path, Header extraHdr) throws IOException {
   return delete(cluster, path, extraHdr);
 }

 /**
  * Send a DELETE request
  * @param cluster the cluster definition
  * @param path the path or URI
  * @return a Response object with response detail
  * @throws IOException for error
  */
 public Response delete(Cluster cluster, String path) throws IOException {
   HttpDelete method = new HttpDelete(path);
   try {
     HttpResponse resp = execute(cluster, method, null, path);
     Header[] headers = resp.getAllHeaders();
     byte[] content = getResponseBody(resp);
     return new Response(resp.getStatusLine().getStatusCode(), headers, content);
   } finally {
     method.releaseConnection();
   }
 }

 /**
  * Send a DELETE request
  * @param cluster the cluster definition
  * @param path the path or URI
  * @return a Response object with response detail
  * @throws IOException for error
  */
 public Response delete(Cluster cluster, String path, Header extraHdr) throws IOException {
   HttpDelete method = new HttpDelete(path);
   try {
     Header[] headers = { extraHdr };
     HttpResponse resp = execute(cluster, method, headers, path);
     headers = resp.getAllHeaders();
     byte[] content = getResponseBody(resp);
     return new Response(resp.getStatusLine().getStatusCode(), headers, content);
   } finally {
     method.releaseConnection();
   }
 }

 /**
  * Used for Kerberos configuration
  */
 private static class ClientLoginConfig extends Configuration {
   private final String keyTabLocation;
   private final String userPrincipal;
   private final Map<String, Object> loginOptions;

   public ClientLoginConfig(String keyTabLocation, String userPrincipal, Map<String, Object> loginOptions) {
	   super();
	   this.keyTabLocation = keyTabLocation;
	   this.userPrincipal = userPrincipal;
	   this.loginOptions = loginOptions;
	}

	@Override
	public AppConfigurationEntry[] getAppConfigurationEntry(String name) {
		Map<String, Object> options = new HashMap<String, Object>();

		// if we don't have keytab or principal only option is to rely on
		// credentials cache.
		if ((keyTabLocation == null) || userPrincipal == null) {
			// cache
			options.put("useTicketCache", "true");
		} else {
			// keytab
			options.put("useKeyTab", "true");
			options.put("keyTab", this.keyTabLocation);
			options.put("principal", this.userPrincipal);
			options.put("storeKey", "true");
		}
		options.put("doNotPrompt", "true");
		options.put("isInitiator", "true");

		if (loginOptions != null) {
			options.putAll(loginOptions);
		}

		return new AppConfigurationEntry[] { new AppConfigurationEntry(
				"com.sun.security.auth.module.Krb5LoginModule",
				AppConfigurationEntry.LoginModuleControlFlag.REQUIRED, options) };
	}
  }
}
