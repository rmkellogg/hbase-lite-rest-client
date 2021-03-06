# hbase-lite-rest-client
Apache HBase Lite REST Client 

Provides a REST Client to Apache HBase 1.x/2.x using a minimal set of external dependencies.

External Dependencies: Apache Commons Logging, Apache Commons Lang3, Apache Commons Codec, Apache HttpClient & Google Protocol Buffers

By comparison the HBase REST Client included with HBase 2.x even after careful exclusions has 21 dependencies.
 
Care has been taken to allow for use against existing clients of the RemoteHTable interface by replacement of the package name alone.  

Improvements:

   * Fluent API for construction of RemoteHTable and RemoteAdmin.
   * API improvements using Strings and other primitives in addition to byte arrays on: Result, Get, Put
   * Easy use of Kerberos and preemptive basic authentication
   * Easy configuration support for self-signed SSL certificates
   * Reduced footprint by stripping away rarely used or methods not supported by the HBase REST Server.
   * RemoteHTable and RemoteAdmin are now interfaces.
   * Support for Kerberos authentication via keytab and user principal
   * Access to underlying Apache HttpClient for unique client needs

Note: This REST Client was based on Apache HBase 2.0 Alpha 4.

For use with Kerberos, you have three options: specify the User Principal and Keytab explicitly, do a kinit prior to execution or use an external JAAS configuration file.

RemoteHTable Construction:

```
HttpClient httpClient = HttpClientBuilder.create().build();
            
RemoteHTable table = RemoteHTableBuilder.create("namespace:tablename")
                            .addHost("hostname:8080")
                            .withProtocol("http")
                            .withMaxRetries(10)
                            .withSleepTime(1000)
                            //.withAllowSelfSignedCertificates(false)
                            
                            // Set these for use of Kerberos with Principal and Keytab
                            //.withUseKerberos("hbase/hostname@REALM.COM","/etc/security/keytabs/hbase.security.keytab")
  
                            // Set these for use of Kerberos with external kinit
                            //.withUseKerberos()
                        
                            // Set these for use of Kerberos with external JAAS configuration
                            //.withUseJAAS("Client")
                        
                            // Set these for use of Preemptive Basic Authentication
                            //.withUsePreemptiveBasicAuthentication("hbase-user","hbase-password")
                            
                        .build();
```
  
Legacy RemoteHTable examples using byte arrays:

```
Get get = new Get("KEYA".getBytes(StandardCharsets.UTF_8));
get.addFamily("Family".getBytes(StandardCharsets.UTF_8));
get.addColumn("Family".getBytes(StandardCharsets.UTF_8),"ColB".getBytes(StandardCharsets.UTF_8));
  
Result result = table.get(get);
dumpResult(result);

Scan scan = new Scan("KEYA".getBytes(StandardCharsets.UTF_8), "KEYB".getBytes(StandardCharsets.UTF_8));
        
ResultScanner scanner = table.getScanner(scan);
        
for(Result row : scanner)
{
    dumpResult(row);
}

Put put = new Put("KEYA".getBytes(StandardCharsets.UTF_8));
put.addColumn("COLA".getBytes(StandardCharsets.UTF_8), "VALUE1".getBytes(StandardCharsets.UTF_8));
table.put(put);
```

Modern API RemoteHTable examples using native data types:

```
Get get = new Get("KEYA");
get.addFamily("Family");
get.addColumn("Family","ColB");
        
Result result = table.get(get);
dumpResult(result);

// Use Strings directly without conversion to byte arrays explicitly
if (result.containsColumn("Family","ColA")) 
{
   System.out.println(result.getIntValue("Family","ColA",0));
}

Put put = new Put("KEYA");
put.addColumn("COLA", "VALUE1");
put.addColumn("COLB", 37);
table.put(put);
```

Helper Methods:

```
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
                        //.withAllowSelfSignedCertificates(false)
                        // With explicit HttpClient but normally not required
                        //.withHttpClient(httpClient)
                        .build();

 System.out.println(admin.getRestVersion());
 System.out.println(admin.getTableList());                      
```

Example Kerberos JAAS Configuration:

```
Client {
com.sun.security.auth.module.Krb5LoginModule required
useKeyTab=true
storeKey=true
useTicketCache=false
keyTab="/etc/security/keytabs/hbase.service.keytab"
principal="hbase/hostname@REALM.COM";
}                       
```
