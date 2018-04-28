package org.apache.druid.processing.codecs;

import org.apache.druid.processing.codecs.FastPFor.NativeFastPForCodec;
import org.apache.druid.processing.codecs.FastPFor.NativeFastPForCodecs;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.List;
import java.util.Random;
import java.util.stream.Collectors;

@RunWith(Parameterized.class)
public class NativeFastPForCodecTest
{
  @Parameterized.Parameters(name = "{index}: {0}")
  public static List<Object[]> constructorFeeder()
  {
    return NativeFastPForCodecs.getNames()
                 .stream()
                 .map(c -> new Object[]{c})
                 .collect(Collectors.toList());
  }


  private static int buffSize = 1 << 16;
  private static ByteBuffer initialValues = ByteBuffer.allocateDirect(buffSize).order(ByteOrder.nativeOrder());
  private static ByteBuffer encodedValues = ByteBuffer.allocateDirect(buffSize + 1024).order(ByteOrder.nativeOrder());
  private static ByteBuffer decodedValues = ByteBuffer.allocateDirect(buffSize).order(ByteOrder.nativeOrder());

  private static void resetBuffers()
  {
    initialValues.clear();
    encodedValues.clear();
    decodedValues.clear();
  }

  private String codecName;
  private NativeFastPForCodec codec;

  public NativeFastPForCodecTest(String codecName)
  {
    this.codecName = codecName;
  }

  @Before
  public void setup()
  {

    this.codec = new NativeFastPForCodec(NativeFastPForCodecs.fromString(codecName));
  }

  @After
  public void tearDown() throws Exception
  {
    this.codec.dispose(); // this is racy and can coredump if we stop while native cleanup in in progress
    Thread.sleep(250);
  }

  @Test
  public void testFastPForCodecSequentialData()
  {
    resetBuffers();

    int numValues = 1 << 14;
    int maxNumValues = buffSize >> 2;

    initialValues.position(0);
    for (int i = 0; i < numValues; i++) {
      initialValues.putInt(i);
    }

    int size = codec.encode(initialValues, numValues, encodedValues, maxNumValues);

    int decodedSize = codec.decode(encodedValues, 0, size, decodedValues, numValues);

    Assert.assertEquals(numValues, decodedSize);
    for (int i = 0, pos = 0; i < numValues; i++, pos += Integer.BYTES) {
      Assert.assertEquals(initialValues.getInt(pos), decodedValues.getInt(pos));
    }
  }


  @Test
  public void testFastPForCodecRandomData()
  {
    resetBuffers();
    Random r = new Random();

    int numValues = 1 << 14;
    int maxNumValues = buffSize >> 2;

    initialValues.position(0);
    for (int i = 0; i < numValues; i++) {
      // not all codecs work with full range of int values... some of them start breaking after here
      initialValues.putInt(r.nextInt(1 << 16));
    }

    int size = codec.encode(initialValues, numValues, encodedValues, maxNumValues);

    int decodedSize = codec.decode(encodedValues, 0, size, decodedValues, numValues);

    Assert.assertEquals(numValues, decodedSize);
    for (int i = 0, pos = 0; i < numValues; i++, pos += Integer.BYTES) {
      Assert.assertEquals(initialValues.getInt(pos), decodedValues.getInt(pos));
    }
  }
}
