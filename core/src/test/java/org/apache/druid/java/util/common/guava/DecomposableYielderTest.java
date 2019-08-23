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

package org.apache.druid.java.util.common.guava;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Ordering;
import org.apache.druid.common.guava.CombiningSequence;
import org.apache.druid.java.util.common.Pair;
import org.junit.Assert;
import org.junit.Test;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.function.BinaryOperator;
import java.util.stream.Collectors;

public class DecomposableYielderTest
{

  @Test
  public void testarossa() throws Exception
  {
    List<IntPair> pairs1 = Arrays.asList(
        new IntPair(0, 6),
        new IntPair(1, 1),
        new IntPair(2, 1),
        new IntPair(5, 11),
        new IntPair(6, 1)
    );

    List<IntPair> pairs2 = Arrays.asList(
        new IntPair(0, 1),
        new IntPair(1, 13),
        new IntPair(4, 1),
        new IntPair(6, 2),
        new IntPair(10, 2)
    );

    List<IntPair> pairs3 = Arrays.asList(
        new IntPair(4, 5),
        new IntPair(10, 3)
    );

    List<IntPair> pairs4 = Arrays.asList(
        new IntPair(0, 1),
        new IntPair(1, 13),
        new IntPair(4, 1),
        new IntPair(6, 2),
        new IntPair(10, 2)
    );

    List<IntPair> pairs5 = Arrays.asList(
        new IntPair(0, 6),
        new IntPair(1, 1),
        new IntPair(2, 1),
        new IntPair(5, 11),
        new IntPair(6, 1)
    );

    List<Sequence<IntPair>> input = new ArrayList<>();
    input.add(Sequences.simple(pairs1));
    input.add(Sequences.simple(pairs2));
    input.add(Sequences.simple(pairs3));
    input.add(Sequences.simple(pairs4));
    input.add(Sequences.simple(pairs5));

    assertResult(input);
  }

  private void assertResult(List<Sequence<IntPair>> sequences) throws InterruptedException
  {
    final Ordering<IntPair> ordering = Ordering.natural().onResultOf(p -> p.lhs);
    final BinaryOperator<IntPair> mergeFn = (lhs, rhs) -> {
      if (lhs == null) {
        return rhs;
      }

      if (rhs == null) {
        return lhs;
      }

      return new IntPair(lhs.lhs, lhs.rhs + rhs.rhs);
    };

    final CombiningSequence<IntPair> combiningSequence = CombiningSequence.create(
        new MergeSequence<>(ordering, Sequences.simple(sequences)),
        ordering,
        mergeFn
    );

    Yielder<IntPair> combiningYielder = Yielders.each(combiningSequence);
    List<IntPair> results1 = new ArrayList<>();

    IntPair prev = null;

    while (!combiningYielder.isDone()) {
      Assert.assertNotEquals(combiningYielder.get(), prev);
      prev = combiningYielder.get();
      results1.add(prev);
      combiningYielder = combiningYielder.next(combiningYielder.get());
    }

    Assert.assertTrue(combiningYielder.isDone());


    List<IntPair> results2 = new ArrayList<>();
    int yieldAfter = 3;
    YieldingAccumulator<IntPair, IntPair> acc = new YieldingAccumulator<IntPair, IntPair>()
    {
      int ctr = 0;
      @Override
      public IntPair accumulate(IntPair accumulated, IntPair in)
      {
        ctr++;
        if (ctr % yieldAfter == 0) {
          yield();
        }
        results2.add(in);
        return in;
      }
    };
    Yielder<IntPair> partialYielder = combiningSequence.toYielder(null, acc);

    // yield once, then decompose that shit
    partialYielder.get();
    DecomposableYielder<IntPair, IntPair> decomposableYielder = (DecomposableYielder<IntPair, IntPair>) partialYielder;
    DecomposableYielder<IntPair, IntPair> mergingYielder =
        (DecomposableYielder<IntPair, IntPair>) decomposableYielder.decompose().iterator().next();
    Collection<Yielder<IntPair>> baseYielders = mergingYielder.decompose();

    // make sequences out of all the decomposed yielders
    List<Sequence<IntPair>> partiallyProcessedSequences = baseYielders.stream()
                                                                      .map(y -> makeSequenceForYielder(y))
                                                                      .collect(Collectors.toList());

    // we lose the next value of the merging yielder since it got pulled out from the pqueue, wrap it in a sequence
    // and add it to the list
    partiallyProcessedSequences.add(Sequences.simple(ImmutableList.of(mergingYielder.get())));

    final CombiningSequence<IntPair> restCombiningSequence = CombiningSequence.create(
        new MergeSequence<>(ordering, Sequences.simple(partiallyProcessedSequences)),
        ordering,
        mergeFn
    );
    Yielder<IntPair> restCombiningYielder = Yielders.each(restCombiningSequence);

    prev = null;

    while (!restCombiningYielder.isDone()) {
      Assert.assertNotEquals(restCombiningYielder.get(), prev);
      prev = restCombiningYielder.get();
      results2.add(prev);
      restCombiningYielder = restCombiningYielder.next(restCombiningYielder.get());
    }

    Assert.assertEquals(results1, results2);
  }

  private <T> Sequence<T> makeSequenceForYielder(Yielder<T> yielder)
  {
    return new BaseSequence<>(
        new BaseSequence.IteratorMaker<T, Iterator<T>>()
        {
          private Yielder<T> next = yielder;
          @Override
          public Iterator<T> make()
          {
            return new Iterator<T>()
            {
              private T nextVal = yielder.get();

              @Override
              public boolean hasNext()
              {
                return !next.isDone();
              }

              @Override
              public T next()
              {
                nextVal = next.get();
                next = next.next(nextVal);
                return nextVal;
              }
            };
          }

          @Override
          public void cleanup(Iterator<T> iterFromMake)
          {
            try {
              next.close();
            }
            catch (IOException e) {
              e.printStackTrace();
              throw new RuntimeException("failed to close");
            }
          }
        }
    );
  }

  static class IntPair extends Pair<Integer, Integer>
  {
    IntPair(@Nullable Integer lhs, @Nullable Integer rhs)
    {
      super(lhs, rhs);
    }
  }
}
