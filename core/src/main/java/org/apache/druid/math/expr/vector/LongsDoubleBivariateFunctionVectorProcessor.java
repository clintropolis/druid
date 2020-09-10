package org.apache.druid.math.expr.vector;

import org.apache.druid.math.expr.ExprType;

public abstract class LongsDoubleBivariateFunctionVectorProcessor extends BivariateFunctionVectorProcessor<double[], long[], long[]>
{
  public LongsDoubleBivariateFunctionVectorProcessor(
      VectorExprProcessor<long[]> left,
      VectorExprProcessor<long[]> right,
      int maxVectorSize
  )
  {
    super(left, right, maxVectorSize, new double[maxVectorSize]);
  }

  public abstract double apply(long left, long right);

  @Override
  public ExprType getOutputType()
  {
    return ExprType.LONG;
  }

  @Override
  final void processIndex(long[] leftInput, long[] rightInput, int i)
  {
    outValues[i] = apply(leftInput[i], rightInput[i]);
  }

  @Override
  final VectorExprEval<double[]> asEval()
  {
    return new DoubleVectorExprEval(outValues, outNulls);
  }
}
