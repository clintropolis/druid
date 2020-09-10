package org.apache.druid.math.expr.vector;

import org.apache.druid.math.expr.ExprType;

public abstract class StringLongUnivariateFunctionVectorProcessor extends UnivariateFunctionVectorObjectProcessor<String[], long[]>
{
  public StringLongUnivariateFunctionVectorProcessor(VectorExprProcessor<String[]> processor, int maxVectorSize)
  {
    super(processor, maxVectorSize, new long[maxVectorSize]);
  }

  @Override
  public ExprType getOutputType()
  {
    return ExprType.LONG;
  }

  @Override
  public final VectorExprEval<long[]> asEval()
  {
    return new LongVectorExprEval(outValues, outNulls);
  }
}
