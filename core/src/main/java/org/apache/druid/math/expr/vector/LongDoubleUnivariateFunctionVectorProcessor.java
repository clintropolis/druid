package org.apache.druid.math.expr.vector;

import org.apache.druid.math.expr.ExprType;

public abstract class LongDoubleUnivariateFunctionVectorProcessor extends UnivariateFunctionVectorProcessor<long[], double[]>
{
  public LongDoubleUnivariateFunctionVectorProcessor(VectorExprProcessor<long[]> processor, int maxVectorSize)
  {
    super(processor, maxVectorSize, new double[maxVectorSize]);
  }

  public abstract double apply(long input);

  @Override
  public ExprType getOutputType()
  {
    return ExprType.DOUBLE;
  }

  @Override
  final void processIndex(long[] longs, int i)
  {
    outValues[i] = apply(longs[i]);
  }

  @Override
  final VectorExprEval<double[]> asEval()
  {
    return new DoubleVectorExprEval(outValues, outNulls);
  }
}