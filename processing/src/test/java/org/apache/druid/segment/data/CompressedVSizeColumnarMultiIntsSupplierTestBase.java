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

package org.apache.druid.segment.data;

import com.google.common.base.Supplier;
import org.apache.druid.java.util.common.io.Closer;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.List;

public abstract class CompressedVSizeColumnarMultiIntsSupplierTestBase
{

  @Before
  public abstract void setUpSimple();

  @After
  public abstract void teardown() throws IOException;

  public abstract List<int[]> getValsUsed();

  public abstract Supplier<ColumnarMultiInts> getColumnarMultiIntsSupplier();

  public abstract Supplier<ColumnarMultiInts> fromByteBuffer(ByteBuffer buf);

  @Test
  public void testSanity()
  {
    assertSame(getValsUsed(), getColumnarMultiIntsSupplier().get());
  }


  @Test(expected = IllegalArgumentException.class)
  public void testGetInvalidElementInRow()
  {
    getColumnarMultiIntsSupplier().get().get(3).get(15);
  }

  @Test
  public void testIterators()
  {
    final Supplier<ColumnarMultiInts> columnarMultiIntsSupplier = getColumnarMultiIntsSupplier();
    List<int[]> vals = getValsUsed();

    Iterator<IndexedInts> iterator = columnarMultiIntsSupplier.get().iterator();
    int row = 0;
    while (iterator.hasNext()) {
      final int[] ints = vals.get(row);
      final IndexedInts vSizeIndexedInts = iterator.next();

      Assert.assertEquals(ints.length, vSizeIndexedInts.size());
      for (int i = 0, size = vSizeIndexedInts.size(); i < size; i++) {
        Assert.assertEquals(ints[i], vSizeIndexedInts.get(i));
      }
      row++;
    }
  }

  private void assertSame(List<int[]> someInts, ColumnarMultiInts columnarMultiInts)
  {
    Assert.assertEquals(someInts.size(), columnarMultiInts.size());
    for (int i = 0; i < columnarMultiInts.size(); ++i) {
      final int[] ints = someInts.get(i);
      final IndexedInts vSizeIndexedInts = columnarMultiInts.get(i);

      Assert.assertEquals(ints.length, vSizeIndexedInts.size());
      for (int j = 0; j < ints.length; j++) {
        Assert.assertEquals(ints[j], vSizeIndexedInts.get(j));
      }
    }
  }

  public static <T extends Closeable> Supplier<T> wrapSupplier(
      Supplier<T> supplier,
      Closer closer
  )
  {
    return () -> {
      // We must register the actual column with the closer as well because the resources taken by the
      // column are not part of what the Supplier's closer manages
      return closer.register(supplier.get());
    };
  }
}
