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
import com.google.common.collect.Lists;
import com.google.common.collect.Ordering;
import com.google.common.primitives.Ints;
import org.apache.druid.common.guava.CombiningSequence;
import org.apache.druid.java.util.common.logger.Logger;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.RecursiveAction;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.BinaryOperator;
import java.util.stream.Collectors;

public class ParallelMergeCombiningSequence<T> extends YieldingSequenceBase<T>
{
  private static final Logger LOG = new Logger(ParallelMergeCombiningSequence.class);

  private final ForkJoinPool workerPool;
  private final List<? extends Sequence<? extends T>> baseSequences;
  private final Ordering<T> orderingFn;
  private final BinaryOperator<T> combineFn;
  private final int queueSize;
  private final boolean hasTimeout;
  private final long timeoutAt;
  private final int queryPriority; // :(
  private final boolean possiblyParallel;
  private final int parallelism;

  public ParallelMergeCombiningSequence(
      ForkJoinPool workerPool,
      List<? extends Sequence<? extends T>> baseSequences,
      Ordering<T> orderingFn,
      BinaryOperator<T> combineFn,
      int queueSize,
      boolean hasTimeout,
      long timeout,
      int queryPriority,
      boolean possiblyParallel,
      int parallelism
  )
  {
    this.workerPool = workerPool;
    this.baseSequences = baseSequences;
    this.orderingFn = orderingFn;
    this.combineFn = combineFn;
    this.queueSize = queueSize;
    this.hasTimeout = hasTimeout;
    this.timeoutAt = System.currentTimeMillis() + timeout;
    this.queryPriority = queryPriority;
    this.possiblyParallel = possiblyParallel;
    this.parallelism = parallelism;
  }

  @Override
  public <OutType> Yielder<OutType> toYielder(OutType initValue, YieldingAccumulator<OutType, T> accumulator)
  {
    // todo: access to the fjp should be controlled instead of a free for all scheduling of stuff...

    final BlockingQueue<ValueHolder<T>> finalQueue = new ArrayBlockingQueue<>(queueSize);

    // 2 layer parallel merge done entirely in fjp, either
    final RecursiveAction mergeAction;
    if (possiblyParallel) {
      mergeAction = new OpportunisticParallelMergeCombineAction<>(
          baseSequences,
          orderingFn,
          combineFn,
          finalQueue,
          queueSize,
          hasTimeout,
          timeoutAt,
          parallelism
      );
    } else {
      mergeAction = new ParallelMergeCombineAction<>(
          baseSequences,
          orderingFn,
          combineFn,
          finalQueue,
          queueSize,
          hasTimeout,
          timeoutAt,
          parallelism
      );
    }
    workerPool.invoke(mergeAction);
    Sequence<T> finalOutSequence = makeOutputSequenceForQueue(mergeAction, finalQueue, hasTimeout, timeoutAt);
    return finalOutSequence.toYielder(initValue, accumulator);
  }

  /**
   * Create an output {@link Sequence} that wraps the output {@link BlockingQueue} of a {@link RecursiveAction} task
   */
  private static <T> Sequence<T> makeOutputSequenceForQueue(
      RecursiveAction task,
      BlockingQueue<ValueHolder<T>> queue,
      boolean hasTimeout,
      long timeoutAt
  )
  {
    final QueueTaker<ValueHolder<T>> taker = new QueueTaker<>(
        queue,
        hasTimeout,
        timeoutAt
    );
    final Sequence<T> backgroundCombineSequence = new BaseSequence<>(
        new BaseSequence.IteratorMaker<T, Iterator<T>>()
        {
          @Override
          public Iterator<T> make()
          {
            return new Iterator<T>()
            {
              private T nextVal;

              @Override
              public boolean hasNext()
              {
                try {
                  final ValueHolder<T> holder;
                  ForkJoinPool.managedBlock(taker);
                  holder = taker.getItem();

                  if (holder == null) {
                    throw new RuntimeException(new TimeoutException());
                  }
                  if (holder.isTerminal) {
                    return false;
                  }
                  nextVal = holder.val;
                  return true;
                }
                catch (InterruptedException e) {
                  throw new RuntimeException(e);
                }
              }

              @Override
              public T next()
              {
                if (nextVal == null) {
                  throw new NoSuchElementException();
                }
                return nextVal;
              }
            };
          }

          @Override
          public void cleanup(Iterator<T> iterFromMake)
          {
            try {
              if (!task.isDone()) {
                task.cancel(true);
              }
            }
            catch (Exception e) {
              throw new RuntimeException(e);
            }
          }
        }
    );

    return backgroundCombineSequence;
  }

  /**
   * This fork join task is the initial merge combine task of the parallel merge combine process, and doesn't operate in
   * parallel at all. It's mission is to perform a traditional sequential merge on all of the sequences to calculate how
   * many results to yield per fork join task to each run for around ~10ms. If the merge completes, sweet, we didn't
   * need to parallelize anyway. If not, it will fan out into some number of {@link MergeCombineAction} tasks into a 2
   * layer tree to perform the remainder of the work. where the sequences will be divided into groups for the first
   * layer, and a single task at the 2nd layer which consumes the output of the parallel layer.
   *
   * Access to starting these tasks should be gated somehow to prevent pool overloading, and this should be computed
   * based on current utilization of the fjp, but currently is fixed to always use 3 tasks for testing purposes
   * (2 for sequence processing, 1 for final merge)
   */
  private static class OpportunisticParallelMergeCombineAction<T> extends ParallelMergeCombineAction<T>
  {
    private OpportunisticParallelMergeCombineAction(
        List<? extends Sequence<? extends T>> sequences,
        Ordering<T> orderingFn,
        BinaryOperator<T> combineFn,
        BlockingQueue<ValueHolder<T>> out,
        int queueSize,
        boolean hasTimeout,
        long timeoutAt,
        int parallelism
    )
    {
      super(sequences, orderingFn, combineFn, out, queueSize, hasTimeout, timeoutAt, parallelism);
    }

    @Override
    protected void compute()
    {
      final CombiningSequence<T> combiningSequence = CombiningSequence.create(
          new MergeSequence<>(orderingFn, Sequences.simple(baseSequences)),
          orderingFn,
          combineFn
      );

      final int sampleSize = 1000;
      QueuePusher<ValueHolder<T>> pusher = new QueuePusher<>(out, hasTimeout, timeoutAt);

      // creating a yielder for the sequence yield the first yield amount, this first yield will be a single result,
      // this is to burn the time period for _all_ sequences to have results
      Yielder<T> serialYielder = MergeCombineAction.makeSequenceYielder(combiningSequence, sampleSize, pusher);

      // but the next will measure sampleSize, so time it to compute how many to yield per MergeCombineAction
      long start = System.nanoTime();
      serialYielder = serialYielder.next(null);

      if (serialYielder.isDone()) {
        // sweet, we did it already, nothing left to do. close out the final queue
        pusher.addItem(new ValueHolder<>());
        try {
          ForkJoinPool.managedBlock(pusher);
          serialYielder.close();
        }
        catch (InterruptedException | IOException e) {
          throw new RuntimeException("parallel merge combine task failed", e);
        }
      } else {
        long elapsedMillis = Math.max(TimeUnit.MILLISECONDS.convert(System.nanoTime() - start, TimeUnit.NANOSECONDS), 1L);
        int yieldAfter = Math.max(Ints.checkedCast((10 * sampleSize) / elapsedMillis), 1);


        DecomposableYielder<T, T> combineYielder = (DecomposableYielder<T, T>) serialYielder;
        DecomposableYielder<T, T> mergeYielder = (DecomposableYielder<T, T>) combineYielder.decompose().iterator().next();
        List<Sequence<T>> partiallyProcessedSequences =
            mergeYielder.decompose().stream()
                        .map(OpportunisticParallelMergeCombineAction::makeSequenceForYielder)
                        .collect(Collectors.toList());

        // we lose the next value of the merging yielder since it got pulled out from the pqueue, wrap it in a sequence
        // and add it to the list
        partiallyProcessedSequences.add(Sequences.simple(ImmutableList.of(mergeYielder.get())));

        LOG.info(
            "starting %s parallel merge tasks, initial %s yielded results ran for %s millis, yielding every %s operations",
            computeNumTasks(partiallyProcessedSequences),
            sampleSize,
            elapsedMillis,
            yieldAfter
        );
        spawnParallelTasks(partiallyProcessedSequences, yieldAfter);
      }
    }



    private static <T> Sequence<T> makeSequenceForYielder(Yielder<T> yielder)
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
                throw new RuntimeException("failed to close");
              }
            }
          }
      );
    }
  }

  /**
   * This task is primarily designed for testing raw 2 layer parallel merge performance, and currently always does a
   * fixed 3 task, 2 layer merge/combine of the result sets.
   */
  private static class ParallelMergeCombineAction<T> extends RecursiveAction
  {
    final List<? extends Sequence<? extends T>> baseSequences;
    final Ordering<T> orderingFn;
    final BinaryOperator<T> combineFn;
    final BlockingQueue<ValueHolder<T>> out;
    final int queueSize;
    final boolean hasTimeout;
    final long timeoutAt;
    final int parallelism;

    private ParallelMergeCombineAction(
        List<? extends Sequence<? extends T>> sequences,
        Ordering<T> orderingFn,
        BinaryOperator<T> combineFn,
        BlockingQueue<ValueHolder<T>> out,
        int queueSize,
        boolean hasTimeout,
        long timeoutAt,
        int parallelism
    )
    {
      this.baseSequences = sequences;
      this.combineFn = combineFn;
      this.orderingFn = orderingFn;
      this.out = out;
      this.queueSize = queueSize;
      this.hasTimeout = hasTimeout;
      this.timeoutAt = timeoutAt;
      this.parallelism = parallelism;
    }

    @Override
    protected void compute()
    {
      final int yieldEvery = 1000;

      spawnParallelTasks(baseSequences, yieldEvery);
    }

    int computeNumTasks(List<? extends Sequence<? extends T>> sequences)
    {
      final int parallelTasks = Math.max(2, parallelism - 1);
      return Math.min((int) Math.floor((double) sequences.size() / 2.0), parallelTasks);
    }

    void spawnParallelTasks(List<? extends Sequence<? extends T>> sequences, int yieldAfter)
    {
      final int parallelMergeTasks = computeNumTasks(sequences);

      List<RecursiveAction> tasks = new ArrayList<>();
      List<Sequence<T>> outputSequences = new ArrayList<>();

      List<? extends List<? extends Sequence<? extends T>>> partitions =
          Lists.partition(sequences, sequences.size() / parallelMergeTasks);


      for (List<? extends Sequence<? extends T>> partition : partitions) {

        final BlockingQueue<ValueHolder<T>> queue = new ArrayBlockingQueue<>(queueSize);
        MergeCombineAction<T> mergeAction = new MergeCombineAction<>(
            partition,
            orderingFn,
            combineFn,
            queue,
            hasTimeout,
            timeoutAt,
            yieldAfter
        );
        Sequence<T> outSequence = makeOutputSequenceForQueue(mergeAction, queue, hasTimeout, timeoutAt);

        tasks.add(mergeAction);
        outputSequences.add(outSequence);
      }

      invokeAll(tasks);

      MergeCombineAction<T> finalMergeAction = new MergeCombineAction<>(
          outputSequences,
          orderingFn,
          combineFn,
          out,
          hasTimeout,
          timeoutAt,
          yieldAfter
      );

      getPool().execute(finalMergeAction);
    }
  }

  /**
   * This fork join task is the initial task that performs merge combine of a sub group of {@link Sequence},
   * executed from {@link ParallelMergeCombineAction}. If the sequence is not fully yielded by {@link #compute()} then a
   * {@link MergeCombineContinuationAction} task will be created to continue yielding the merged and combined
   * output {@link Sequence} to {@link #outputQueue}
   */
  private static class MergeCombineAction<T> extends RecursiveAction
  {
    private final List<? extends Sequence<? extends T>> baseSequences;
    private final Ordering<T> orderingFn;
    private final BinaryOperator<T> combineFn;
    private final BlockingQueue<ValueHolder<T>> outputQueue;
    private final boolean hasTimeout;
    private final long timeoutAt;
    private final int yieldEvery;

    private MergeCombineAction(
        List<? extends Sequence<? extends T>> sequences,
        Ordering<T> orderingFn,
        BinaryOperator<T> combineFn,
        BlockingQueue<ValueHolder<T>> outputQueue,
        boolean hasTimeout,
        long timeoutAt,
        int yieldEvery
    )
    {
      this.baseSequences = sequences;
      this.combineFn = combineFn;
      this.orderingFn = orderingFn;
      this.outputQueue = outputQueue;
      this.hasTimeout = hasTimeout;
      this.timeoutAt = timeoutAt;
      this.yieldEvery = yieldEvery;
    }

    @Override
    protected void compute()
    {
      final Sequence<? extends Sequence<? extends T>> sequences = Sequences.simple(baseSequences);
      MergeSequence<T> mergeAllSequence = new MergeSequence<T>(orderingFn, sequences);
      final CombiningSequence<T> combiningSequence = CombiningSequence.create(
          mergeAllSequence,
          orderingFn,
          combineFn
      );
      QueuePusher<ValueHolder<T>> pusher = new QueuePusher<ValueHolder<T>>(outputQueue, hasTimeout, timeoutAt);
      Yielder<T> yielder = makeSequenceYielder(combiningSequence, yieldEvery, pusher);

      getPool().execute(new MergeCombineContinuationAction<>(yielder, pusher));
    }

    static <T> Yielder<T> makeSequenceYielder(
        Sequence<T> sequence,
        final int yieldEvery,
        QueuePusher<ValueHolder<T>> pusher
    )
    {
      return sequence.toYielder(
          null,
          new YieldingAccumulator<T, T>()
          {
            int count = 0;
            boolean firstYield = true;
            @Override
            public T accumulate(T accumulated, T in)
            {
              if (firstYield) {
                firstYield = false;
                yield();
              } else {
                count++;
                if (count % yieldEvery == 0) {
                  yield();
                }
              }
              pusher.addItem(new ValueHolder<>(in));
              try {
                ForkJoinPool.managedBlock(pusher);
              }
              catch (InterruptedException e) {
                throw new RuntimeException("parallel merge combine task failed", e);
              }
              return in;
            }
          }
      );
    }
  }

  /**
   * Continuation task to yield additional results of a {@link Yielder} of a merged and combined {@link Sequence} to
   * {@link #outputQueue}
   */
  private static class MergeCombineContinuationAction<T> extends RecursiveAction
  {
    private final Yielder<T> yielder;
    private final QueuePusher<ValueHolder<T>> outputQueue;

    private MergeCombineContinuationAction(
        Yielder<T> yielder,
        QueuePusher<ValueHolder<T>> outputQueue
    )
    {
      this.yielder = yielder;
      this.outputQueue = outputQueue;
    }

    @Override
    protected void compute()
    {
      if (!yielder.isDone()) {
        T init = yielder.get();
        Yielder<T> nextYielder = yielder.next(init);
        getPool().execute(new MergeCombineContinuationAction<>(nextYielder, outputQueue));
        return;
      }
      // yielder is done, so are we, write terminator
      outputQueue.addItem(new ValueHolder<>());
      try {
        ForkJoinPool.managedBlock(outputQueue);
        yielder.close();
      }
      catch (InterruptedException | IOException e) {
        throw new RuntimeException("parallel merge combine task failed", e);
      }
    }
  }

  private static class ValueHolder<T>
  {
    @Nullable
    private final T val;
    private final boolean isTerminal;

    private ValueHolder(@Nullable T val)
    {
      this.val = val;
      this.isTerminal = false;
    }

    private ValueHolder()
    {
      this.val = null;
      this.isTerminal = true;
    }
  }

  static class QueueTaker<E> implements ForkJoinPool.ManagedBlocker
  {
    final boolean hasTimeout;
    final long timeoutAt;
    final BlockingQueue<E> queue;
    volatile E item = null;

    volatile boolean isComplete;

    QueueTaker(BlockingQueue<E> q, boolean hasTimeout, long timeoutAt)
    {
      this.queue = q;
      this.hasTimeout = hasTimeout;
      this.timeoutAt = timeoutAt;
    }

    @Override
    public boolean block() throws InterruptedException
    {
      if (item == null) {
        if (hasTimeout) {
          final int thisTimeout = Ints.checkedCast(timeoutAt - System.currentTimeMillis());
          item = queue.poll(thisTimeout, TimeUnit.MILLISECONDS);
        } else {
          item = queue.take();
        }
        if (item == null) {
          isComplete = true;
        }
      }
      return true;
    }

    @Override
    public boolean isReleasable()
    {
      return isComplete || item != null || (item = queue.poll()) != null;
    }
    public E getItem()
    {
      final E item = this.item;
      this.item = null;
      return item;
    }
  }

  static class QueuePusher<E> implements ForkJoinPool.ManagedBlocker
  {
    final boolean hasTimeout;
    final long timeoutAt;
    final BlockingQueue<E> queue;
    volatile E item = null;

    QueuePusher(BlockingQueue<E> q, boolean hasTimeout, long timeoutAt)
    {
      this.queue = q;
      this.hasTimeout = hasTimeout;
      this.timeoutAt = timeoutAt;
    }

    @Override
    public boolean block() throws InterruptedException
    {
      if (item != null) {
        if (hasTimeout) {
          final int thisTimeout = Ints.checkedCast(timeoutAt - System.currentTimeMillis());
          queue.offer(item, thisTimeout, TimeUnit.MILLISECONDS);
        } else {
          queue.offer(item);
        }
        item = null;
      }
      return true;
    }

    @Override
    public boolean isReleasable()
    {
      return item == null;
    }

    public void addItem(E item)
    {
      this.item = item;
    }
  }
}
