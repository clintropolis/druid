package org.apache.druid.math.expr.vector;

import org.apache.druid.math.expr.Expr;
import org.apache.druid.math.expr.ExprType;

import javax.annotation.Nullable;

public class FunctionVectorProcessorFactory
{

  @Nullable
  public static <TO, TL, TR> BivariateFunctionVectorProcessor<TO, TL, TR> buildPrimitiveMathProcessor(
      Expr.VectorInputBindingTypes inputTypes,
      Expr left,
      Expr right,
      final BivariateMathEvals evalFunctions
  )
  {
    final ExprType leftType = left.getOutputType(inputTypes);
    final ExprType rightType = right.getOutputType(inputTypes);

    final int maxVectorSize = inputTypes.getMaxVectorSize();
    BivariateFunctionVectorProcessor<?, ?, ?> processor = null;
    if (ExprType.LONG.equals(leftType)) {
      if (ExprType.LONG.equals(rightType)) {
        processor = new LongsBivariateFunctionVectorProcessor(
            left.buildVectorized(inputTypes),
            right.buildVectorized(inputTypes),
            maxVectorSize
        )
        {
          @Override
          public final long apply(long left, long right)
          {
            return evalFunctions.eval(left, right);
          }
        };
      } else if (ExprType.DOUBLE.equals(rightType)) {
        processor = new DoubleLongDoubleBivariateFunctionVectorProcessor(
            left.buildVectorized(inputTypes),
            right.buildVectorized(inputTypes),
            maxVectorSize
        )
        {
          @Override
          public final double apply(long left, double right)
          {
            return evalFunctions.eval(left, right);
          }
        };
      }
    } else if (ExprType.DOUBLE.equals(leftType)) {
      if (ExprType.LONG.equals(rightType)) {
        processor = new DoubleDoubleLongBivariateFunctionVectorProcessor(
            left.buildVectorized(inputTypes),
            right.buildVectorized(inputTypes),
            maxVectorSize
        )
        {
          @Override
          public final double apply(double left, long right)
          {
            return evalFunctions.eval(left, right);
          }
        };
      } else if (ExprType.DOUBLE.equals(rightType)) {
        processor = new DoublesBivariateFunctionVectorProcessor(
            left.buildVectorized(inputTypes),
            right.buildVectorized(inputTypes),
            maxVectorSize
        )
        {
          @Override
          public final double apply(double left, double right)
          {
            return evalFunctions.eval(left, right);
          }
        };
      }
    }
    return (BivariateFunctionVectorProcessor<TO, TL, TR>) processor;
  }

  public interface BivariateMathEvals
  {
    long eval(long left, long right);

    double eval(double left, double right);

    default double eval(long left, double right)
    {
      return eval((double) left, right);
    }

    default double eval(double left, long right)
    {
      return eval(left, (double) right);
    }
  }

  public interface BivariateDoubleMathEvals
  {
    double eval(long left, long right);

    double eval(double left, double right);

    default double eval(long left, double right)
    {
      return eval((double) left, right);
    }

    default double eval(double left, long right)
    {
      return eval(left, (double) right);
    }
  }
}
