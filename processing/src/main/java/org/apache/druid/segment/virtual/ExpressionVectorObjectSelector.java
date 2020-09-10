package org.apache.druid.segment.virtual;

import com.google.common.base.Preconditions;
import org.apache.druid.math.expr.Expr;
import org.apache.druid.math.expr.vector.VectorExprProcessor;
import org.apache.druid.segment.vector.VectorObjectSelector;

public class ExpressionVectorObjectSelector implements VectorObjectSelector
{
  final Expr.VectorBinding bindings;
  final VectorExprProcessor<?> processor;

  public ExpressionVectorObjectSelector(VectorExprProcessor<?> processor, Expr.VectorBinding bindings)
  {
    this.processor = Preconditions.checkNotNull(processor, "processor");
    this.bindings = Preconditions.checkNotNull(bindings, "bindings");
  }

  @Override
  public Object[] getObjectVector()
  {
    return processor.evalVector(bindings).asObjects();
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
