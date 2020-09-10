package org.apache.druid.segment.virtual;

import org.apache.druid.math.expr.Expr;
import org.apache.druid.math.expr.vector.VectorExprProcessor;
import org.apache.druid.segment.IdLookup;
import org.apache.druid.segment.vector.SingleValueDimensionVectorSelector;

import javax.annotation.Nullable;

public class ExpressionSingleValueDimensionVectorSelector implements SingleValueDimensionVectorSelector
{
  final Expr.VectorBinding bindings;
  final VectorExprProcessor<?> processor;

  public ExpressionSingleValueDimensionVectorSelector(
      Expr.VectorBinding bindings,
      VectorExprProcessor<?> processor
  )
  {
    this.bindings = bindings;
    this.processor = processor;
  }

  @Override
  public int[] getRowVector()
  {
    return new int[0];
  }

  @Override
  public int getValueCardinality()
  {
    return CARDINALITY_UNKNOWN;
  }

  @Nullable
  @Override
  public String lookupName(int id)
  {
    return null;
  }

  @Override
  public boolean nameLookupPossibleInAdvance()
  {
    return false;
  }

  @Nullable
  @Override
  public IdLookup idLookup()
  {
    return null;
  }

  @Override
  public int getMaxVectorSize()
  {
    return bindings.getMaxVectorSize();
  }

  @Override
  public int getCurrentVectorSize()
  {
    return bindings.getCurrentVectorSize();
  }
}
