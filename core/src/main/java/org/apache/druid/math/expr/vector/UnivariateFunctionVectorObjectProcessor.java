package org.apache.druid.math.expr.vector;

import org.apache.druid.math.expr.Expr;

public abstract class UnivariateFunctionVectorObjectProcessor<TInput, TOutput> implements VectorExprProcessor<TOutput>
{
  final VectorExprProcessor<TInput> processor;
  final int maxVectorSize;
  final boolean[] outNulls;
  final TOutput outValues;

  public UnivariateFunctionVectorObjectProcessor(
      VectorExprProcessor<TInput> processor,
      int maxVectorSize,
      TOutput outValues
  )
  {
    this.processor = processor;
    this.maxVectorSize = maxVectorSize;
    this.outNulls = new boolean[maxVectorSize];
    this.outValues = outValues;
  }

  @Override
  public VectorExprEval<TOutput> evalVector(Expr.VectorBinding bindings)
  {
    final VectorExprEval<TInput> lhs = processor.evalVector(bindings);

    final int currentSize = bindings.getCurrentVectorSize();

    final TInput input = lhs.values();

    for (int i = 0; i < currentSize; i++) {
      processIndex(input, outValues, outNulls, i);
    }
    return asEval();
  }

  public abstract void processIndex(TInput input, TOutput output, boolean[] outputNulls, int i);

  public abstract VectorExprEval<TOutput> asEval();
}
