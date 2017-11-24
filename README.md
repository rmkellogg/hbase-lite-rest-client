# hbase-lite-rest-client
Apache HBase Lite REST Client 

Provides a REST Client to Apache HBase 1.x/2.x using a minimal set of external dependencies.

External Dependencies: Apache Commons Logging, Apache Commons Lang3, Apache Commons Codec, Apache HttpClient & Google Protocol Buffers

By comparison the HBase REST Client included with HBase 2.x even after careful exclusions has 21 dependencies.
 
Care has been taken to allow for use against existing clients of the RemoteHTable interface by replacement of the package name alone.  

Improvments:

   * Fluent API for construction of RemoteHTable and RemoteAdmin.
   * Access to underlying Apache HttpClient for unique client needs
   * Reduced footprint by stripping away rarely used or methods not supported by the HBase REST Server.
   * RemoteHTable and RemoteAdmin are now interfaces.

Note: This REST Client was based on Apache HBase 2.0 Alpha 4.
  
RemoteHTable Example:

```
HttpClient httpClient = HttpClientBuilder.create().build();
 			
RemoteHTable table = RemoteHTableBuilder.create("namespace:tablename")
	 						.addHost("hostname:8080")
 							.withProtocol("http")
 							.withMaxRetries(10)
							.withSleepTime(1000)
							.withHttpClient(httpClient)
							.build();

Get get = new Get("KEYA".getBytes());
		
Result result = table.get(get);
dumpResult(result);

Scan scan = new Scan("KEYA".getBytes(), "KEYB".getBytes());
    	
ResultScanner scanner = table.getScanner(scan);
    	
for(Result row : scanner)
{
	dumpResult(row);
}

...
	private static void dumpResult(Result result)
	{
		System.out.println(result.isEmpty());

    	NavigableMap<byte[], NavigableMap<byte[], byte[]>> noVersionMap = result.getNoVersionMap();
    	
    	if (noVersionMap.isEmpty())
		{
    		System.out.println("No Map items");
		}
    	else
    	{
    		NavigableMap<byte[], byte[]> keyAndValues = noVersionMap.get("CF".getBytes());
    		
    		if (keyAndValues != null)
    		{
    			for(byte[] key : keyAndValues.keySet())
    			{
    				String keyString = new String(key);
    				byte[] value = keyAndValues.get(key);
    				String valueString = new String(value);
    				
    				System.out.println(keyString + "/" + valueString);
    			}
    		}
    	}
	}
```

RemoteAdmin Example:

```
 HttpClient httpClient = HttpClientBuilder.create().build();
 			
 RemoteAdminBuilder admin = RemoteAdminBuilder.create("namespace:tablename")
 						.addHost("hostname:8080")
 						.withProtocol("https")
 						.withMaxRetries(10)
 						.withSleepTime(1000)
 						.withHttpClient(httpClient)
 						.build();

 System.out.println(admin.getRestVersion());
 System.out.println(admin.getTableList()); 						
```
