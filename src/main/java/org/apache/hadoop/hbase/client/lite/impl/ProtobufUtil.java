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
package org.apache.hadoop.hbase.client.lite.impl;

import java.io.IOException;

import com.google.protobuf.CodedInputStream;
import com.google.protobuf.Message;

/**
 * Protobufs utility.
 */
public class ProtobufUtil {
	  /**
	   * This version of protobuf's mergeFrom avoids the hard-coded 64MB limit for decoding
	   * buffers when working with byte arrays
	   * @param builder current message builder
	   * @param b byte array
	   * @throws IOException
	   */
	  public static void mergeFrom(Message.Builder builder, byte[] b) throws IOException {
	    final CodedInputStream codedInput = CodedInputStream.newInstance(b);
	    codedInput.setSizeLimit(b.length);
	    builder.mergeFrom(codedInput);
	    codedInput.checkLastTagWas(0);
	  }
}
