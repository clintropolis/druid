package org.apache.druid.segment.virtual;

import com.google.common.base.Preconditions;
import org.apache.druid.math.expr.Expr;
import org.apache.druid.math.expr.vector.VectorExprProcessor;
import org.apache.druid.segment.vector.VectorValueSelector;

import javax.annotation.Nullable;

public class ExpressionVectorValueSelector implements VectorValueSelector
{
  final Expr.VectorBinding bindings;
  final VectorExprProcessor<?> processor;
  final float[] floats;

  public ExpressionVectorValueSelector(VectorExprProcessor<?> processor, Expr.VectorBinding bindings)
  {
    this.processor = Preconditions.checkNotNull(processor, "processor");
    this.bindings = Preconditions.checkNotNull(bindings, "bindings");
    this.floats = new float[bindings.getMaxVectorSize()];
  }

  @Override
  public long[] getLongVector()
  {
    return processor.evalVector(bindings).asLongs();
  }

  @Override
  public float[] getFloatVector()
  {
    final double[] doubles = processor.evalVector(bindings).asDoubles();
    for (int i = 0 ; i < bindings.getCurrentVectorSize(); i++)
    {
      floats[i] = (float) doubles[i];
    }
    return floats;
  }

  @Override
  public double[] getDoubleVector()
  {
    return processor.evalVector(bindings).asDoubles();
  }

  @Nullable
  @Override
  public boolean[] getNullVector()
  {
    return processor.evalVector(bindings).nulls();
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
