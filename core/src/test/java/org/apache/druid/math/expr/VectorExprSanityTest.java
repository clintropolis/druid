package org.apache.druid.math.expr;

import com.google.common.collect.ImmutableMap;
import org.apache.druid.java.util.common.NonnullPair;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.math.expr.vector.VectorExprEval;
import org.apache.druid.testing.InitializedNullHandlingTest;
import org.junit.Assert;
import org.junit.Test;

import javax.annotation.Nullable;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;

/**
 * randomize inputs to various vector expressions and make sure the results match nonvectorized expressions
 *
 * this is not a replacement for correctness tests, but will ensure that vectorized and non-vectorized expression
 * evaluation is at least self consistent...
 */
public class VectorExprSanityTest extends InitializedNullHandlingTest
{
  private static final int NUM_ITERATIONS = 100;
  private static final int VECTOR_SIZE = 512;

  final Map<String, ExprType> types = ImmutableMap.<String, ExprType>builder()
      .put("l1", ExprType.LONG)
      .put("l2", ExprType.LONG)
      .put("d1", ExprType.DOUBLE)
      .put("d2", ExprType.DOUBLE)
      .put("s1", ExprType.STRING)
      .put("s2", ExprType.STRING)
      .build();

  @Test
  public void testUnaryOperators()
  {
    final String[] functions = new String[]{"-"};
    final String[] templates = new String[]{"%sd1", "%sl1"};

    testFunctions(types, templates, functions);
  }

  @Test
  public void testBinaryOperators()
  {
    final String[] functions = new String[]{"+", "-", "*", "/", "^", "%", ">", ">=", "<", "<=", "==", "!="};
    final String[] templates = new String[]{
        "d1 %s d2",
        "d1 %s l1",
        "l1 %s d1",
        "l1 %s l2",
        "1 %s l1",
        "1.1 %s d1",
        "l1 %s 1",
        "d1 %s 1.1"
    };

    testFunctions(types, templates, functions);
  }

  @Test
  public void testUnivariateFunctions()
  {
    final String[] functions = new String[]{"parse_long"};
    final String[] templates = new String[]{"%s(s1)", "%s(l1)", "%s(d1)"};
    testFunctions(types, templates, functions);
  }

  @Test
  public void testUnivariateMathFunctions()
  {
    final String[] functions = new String[]{"atan", "cos", "cosh", "cot", "sin", "sinh", "tan", "tanh"};
    final String[] templates = new String[]{"%s(l1)", "%s(d1)"};
    testFunctions(types, templates, functions);
  }

  @Test
  public void testBivariateMathFunctions()
  {
    final String[] functions = new String[]{"max", "min"};
    final String[] templates = new String[]{"%s(d1, d2)", "%s(d1, l1)", "%s(l1, d1)", "%s(l1, l2)"};
    testFunctions(types, templates, functions);
  }

  static void testFunctions(Map<String, ExprType> types, String[] templates, String[] functions)
  {
    for (String template : templates) {
      for (String function : functions) {
        String expr = StringUtils.format(template, function);
        testExpression(expr, types);
      }
    }
  }

  static void testExpression(String expr, Map<String, ExprType> types)
  {
    Expr parsed = Parser.parse(expr, ExprMacroTable.nil());

    for (int iterations = 0; iterations < NUM_ITERATIONS; iterations++) {
      NonnullPair<Expr.ObjectBinding[], Expr.VectorBinding> bindings = makeRandomizedBindings(VECTOR_SIZE, types);
      Assert.assertTrue(parsed.canVectorize(bindings.rhs));
      VectorExprEval<?> vectorEval = parsed.buildVectorized(bindings.rhs).evalVector(bindings.rhs);
      for (int i = 0; i < VECTOR_SIZE; i++) {
        ExprEval<?> eval = parsed.eval(bindings.lhs[i]);
        Assert.assertEquals(StringUtils.format("Values do not match for row %s for expression %s", i, expr), eval.value(), vectorEval.get(i));
      }
    }
  }

  static NonnullPair<Expr.ObjectBinding[], Expr.VectorBinding> makeRandomizedBindings(int vectorSize, Map<String, ExprType> types)
  {
    SettableVectorBinding vectorBinding = new SettableVectorBinding(vectorSize);
    SettableObjectBinding[] objectBindings = new SettableObjectBinding[vectorSize];

    for (Map.Entry<String, ExprType> entry : types.entrySet())
    {
      switch (entry.getValue()) {
        case LONG:
          long[] longs = new long[vectorSize];
          for (int i = 0; i < vectorSize; i++) {
            longs[i] = ThreadLocalRandom.current().nextLong(Integer.MAX_VALUE - 1);
            if (objectBindings[i] == null) {
              objectBindings[i] = new SettableObjectBinding();
            }
            objectBindings[i].withBinding(entry.getKey(), longs[i]);
          }
          vectorBinding.addLong(entry.getKey(), longs);
          break;
        case DOUBLE:
          double[] doubles = new double[vectorSize];
          for (int i = 0; i < vectorSize; i++) {
            doubles[i] = ThreadLocalRandom.current().nextDouble();
            if (objectBindings[i] == null) {
              objectBindings[i] = new SettableObjectBinding();
            }
            objectBindings[i].withBinding(entry.getKey(), doubles[i]);
          }
          vectorBinding.addDouble(entry.getKey(), doubles);
          break;
        case STRING:
          String[] strings = new String[vectorSize];
          for (int i = 0; i < vectorSize; i++) {
            strings[i] = String.valueOf(ThreadLocalRandom.current().nextLong(Integer.MAX_VALUE - 1));
            if (objectBindings[i] == null) {
              objectBindings[i] = new SettableObjectBinding();
            }
            objectBindings[i].withBinding(entry.getKey(), strings[i]);
          }
          vectorBinding.addString(entry.getKey(), strings);
          break;
      }
    }

    return new NonnullPair<>(objectBindings, vectorBinding);
  }

  static class SettableObjectBinding implements Expr.ObjectBinding
  {
    private final Map<String, Object> bindings;

    SettableObjectBinding()
    {
      this.bindings = new HashMap<>();
    }

    @Nullable
    @Override
    public Object get(String name)
    {
      return bindings.get(name);
    }

    public SettableObjectBinding withBinding(String name, Object value)
    {
      bindings.put(name, value);
      return this;
    }
  }

  static class SettableVectorBinding implements Expr.VectorBinding
  {
    private final Map<String, boolean[]> nulls;
    private final Map<String, long[]> longs;
    private final Map<String, double[]> doubles;
    private final Map<String, Object[]> objects;
    private final Map<String, ExprType> types;

    private final int vectorSize;

    SettableVectorBinding(int vectorSize)
    {
      this.nulls = new HashMap<>();
      this.longs = new HashMap<>();
      this.doubles = new HashMap<>();
      this.objects = new HashMap<>();
      this.types = new HashMap<>();
      this.vectorSize = vectorSize;
    }

    public SettableVectorBinding addBinding(String name, ExprType type, boolean[] nulls)
    {
      this.nulls.put(name, nulls);
      this.types.put(name, type);
      return this;
    }
    public SettableVectorBinding addLong(String name, long[] longs)
    {
      return addLong(name, longs, new boolean[longs.length]);
    }

    public SettableVectorBinding addLong(String name, long[] longs, boolean[] nulls)
    {
      assert longs.length == vectorSize;
      this.longs.put(name, longs);
      return addBinding(name, ExprType.LONG, nulls);
    }

    public SettableVectorBinding addDouble(String name, double[] doubles)
    {
      return addDouble(name, doubles, new boolean[doubles.length]);
    }

    public SettableVectorBinding addDouble(String name, double[] doubles, boolean[] nulls)
    {
      assert doubles.length == vectorSize;
      this.doubles.put(name, doubles);
      return addBinding(name, ExprType.DOUBLE, nulls);
    }

    public SettableVectorBinding addString(String name, String[] strings)
    {
      assert strings.length == vectorSize;
      this.objects.put(name, strings);
      return addBinding(name, ExprType.STRING, new boolean[strings.length]);
    }

    @Override
    public <T> T[] getObjects(String name)
    {
      return (T[]) objects.get(name);
    }

    @Override
    public ExprType getType(String name)
    {
      return types.get(name);
    }

    @Override
    public long[] getLongs(String name)
    {
      return longs.get(name);
    }

    @Override
    public double[] getDoubles(String name)
    {
      return doubles.get(name);
    }

    @Override
    public boolean[] getNulls(String name)
    {
      return nulls.get(name);
    }

    @Override
    public int getMaxVectorSize()
    {
      return vectorSize;
    }

    @Override
    public int getCurrentVectorSize()
    {
      return vectorSize;
    }
  }
}
