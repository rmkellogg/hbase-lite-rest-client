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

/**
 * Namespace POJO class. Used to represent and define namespaces.
 *
 * Descriptors will be persisted in an hbase table.
 * This works since namespaces are essentially metadata of a group of tables
 * as opposed to a more tangible container.
 */
public class NamespaceDescriptor {

  /** System namespace name. */
  public static final byte [] SYSTEM_NAMESPACE_NAME = Bytes.toBytes("hbase");
  public static final String SYSTEM_NAMESPACE_NAME_STR =
      Bytes.toString(SYSTEM_NAMESPACE_NAME);
  /** Default namespace name. */
  public static final byte [] DEFAULT_NAMESPACE_NAME = Bytes.toBytes("default");
  public static final String DEFAULT_NAMESPACE_NAME_STR =
      Bytes.toString(DEFAULT_NAMESPACE_NAME);
}
