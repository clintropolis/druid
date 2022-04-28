/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.druid.segment.serde;

import org.apache.druid.segment.column.ColumnIndexSupplier;

import javax.annotation.Nullable;

/**
 * Default implementation of {@link ColumnIndexSupplier} for columns which do not
 * have any indexes.
 */
public class NoIndexesColumnIndexSupplier implements ColumnIndexSupplier
{
  private static final NoIndexesColumnIndexSupplier INSTANCE = new NoIndexesColumnIndexSupplier();

  public static NoIndexesColumnIndexSupplier getInstance()
  {
    return INSTANCE;
  }

  @Nullable
  @Override
  public <T> T getIndex(Class<T> clazz)
  {
    return null;
  }
}
