package org.apache.druid.processing.codecs;

import com.google.common.io.Files;
import me.lemire.integercompression.BinaryPacking;
import org.apache.druid.processing.codecs.FastPFor.NativeFastPForCodec;
import org.apache.druid.processing.codecs.FastPFor.NativeFastPForCodecs;
import me.lemire.integercompression.FastPFOR;
import me.lemire.integercompression.IntWrapper;
import me.lemire.integercompression.SkippableComposition;
import me.lemire.integercompression.SkippableIntegerCODEC;
import me.lemire.integercompression.VariableByte;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.concurrent.ThreadLocalRandom;

public class NativeFastPForJavaFastPForCompatibilityTest
{

  int buffSize = 1 << 16;

  int numValues = 256;
  int upperBound = Integer.MAX_VALUE;

  SkippableIntegerCODEC codec = new SkippableComposition(new FastPFOR(), new VariableByte());
  NativeFastPForCodec nativeCodec = new NativeFastPForCodec(NativeFastPForCodecs.SIMD_FASTPFOR_256);
//  SkippableIntegerCODEC codec = new VariableByte();
//  NativeFastPForCodec nativeCodec = new NativeFastPForCodec(NativeFastPForCodecs.VARINT);
//  SkippableIntegerCODEC codec = new SkippableComposition(new BinaryPacking(), new VariableByte());
//  NativeFastPForCodec nativeCodec = new NativeFastPForCodec(NativeFastPForCodecs.FAST_BINARY_PACKING_32);

  @Test
  public void javaToJavaTest()
  {
    // this works
    int[] initialValueArray = new int[numValues];
    int[] encodedValueArray = new int[buffSize >> 2];
    int[] decodedValueArray = new int[numValues];


    for (int i = 0; i < numValues; i++) {
      initialValueArray[i] = ThreadLocalRandom.current().nextInt(0, upperBound);
//      initialValueArray[i] = i;
    }

    IntWrapper outPos = new IntWrapper(0);
    codec.headlessCompress(initialValueArray, new IntWrapper(0), numValues, encodedValueArray, outPos);

    IntWrapper outPos2 = new IntWrapper(0);
    codec.headlessUncompress(encodedValueArray, new IntWrapper(0), outPos.get(), decodedValueArray, outPos2, numValues);

    Assert.assertEquals(numValues, outPos2.get());

  }

  @Test
  public void nativeToNativeTest()
  {
    // this works
    ByteBuffer initialValues = ByteBuffer.allocateDirect(buffSize).order(ByteOrder.nativeOrder());
    ByteBuffer encodedValues = ByteBuffer.allocateDirect(buffSize + 1024).order(ByteOrder.nativeOrder());
    ByteBuffer decodedValues = ByteBuffer.allocateDirect(buffSize).order(ByteOrder.nativeOrder());

//    int[] initialValueArray = new int[numValues];
//    int[] encodedValueArray = new int[numValues];
//    int[] decodedValueArray = new int[numValues];

    initialValues.position(0);
    for (int i = 0; i < numValues; i++) {
      initialValues.putInt(ThreadLocalRandom.current().nextInt(0, upperBound));
//      initialValues.putInt(i+1);
    }

//    for (int i = 0, pos = 0; i < numValues; i++, pos +=Integer.BYTES) {
//      initialValueArray[i] = initialValues.getInt(pos);
//    }

    // encode native
    int size = nativeCodec.encode(initialValues, numValues, encodedValues, buffSize << 2);
    encodedValues.position(0);
    encodedValues.limit(size * Integer.BYTES);

//    byte[] bytes = new byte[size * Integer.BYTES];
//    for (int i = 0, pos = 0; i < size; i++, pos += Integer.BYTES) {
//      encodedValueArray[i] = encodedValues.getInt(pos);
//      bytes[i] = encodedValues.get(i);
//    }

    int decodedSize = nativeCodec.decode(encodedValues, 0, size, decodedValues, numValues);
//    for (int i = 0, pos = 0; i < numValues; i++, pos += Integer.BYTES) {
//      decodedValueArray[i] = decodedValues.getInt(pos);
//    }
    Assert.assertEquals(decodedSize, numValues);
  }

  @Test
  public void nativeToJavaTest()
  {
    // this throws a java exception
    ByteBuffer initialValues = ByteBuffer.allocateDirect(buffSize).order(ByteOrder.nativeOrder());
    ByteBuffer encodedValues = ByteBuffer.allocateDirect(buffSize + 1024).order(ByteOrder.nativeOrder());

    int[] valueArray = new int[numValues];

    initialValues.position(0);
    for (int i = 0; i < numValues; i++) {
      initialValues.putInt(ThreadLocalRandom.current().nextInt(0, upperBound));
//      initialValues.putInt(i);
    }

    // encode native
    int size = nativeCodec.encode(initialValues, numValues, encodedValues, buffSize << 2);
    encodedValues.position(0);
    encodedValues.limit(size * Integer.BYTES);

    int[] encodedValueArray = new int[size];

    // copy encoded buffer to int array
    for (int i = 0; i < size; i++) {
      encodedValueArray[i] = encodedValues.getInt();
    }

    // decode with java

    IntWrapper outPos = new IntWrapper(0);
    codec.headlessUncompress(encodedValueArray, new IntWrapper(0), size, valueArray, outPos, numValues);

    Assert.assertEquals(numValues, outPos.get());
  }

  @Test
  public void javaToNativeTest()
  {
    // this segfaults
    ByteBuffer encodedValues = ByteBuffer.allocateDirect(buffSize + 1024).order(ByteOrder.nativeOrder());
    ByteBuffer decodedValues = ByteBuffer.allocateDirect(buffSize).order(ByteOrder.nativeOrder());

    int[] initialValueArray = new int[numValues];
    int[] encodedValueArray = new int[buffSize >> 2];

    for (int i = 0; i < numValues; i++) {
//      initialValueArray[i] = i;
      initialValueArray[i] = ThreadLocalRandom.current().nextInt(0, upperBound);
    }

    IntWrapper outPos = new IntWrapper(0);
    codec.headlessCompress(initialValueArray, new IntWrapper(0), numValues, encodedValueArray, outPos);

    encodedValues.position(0);
    for(int i = 0; i < outPos.get(); i++) {
      encodedValues.putInt(encodedValueArray[i]);
    }
    encodedValues.flip();

    int decodedSize = nativeCodec.decode(encodedValues, 0, outPos.get(), decodedValues, numValues);

    Assert.assertEquals(decodedSize, numValues);
  }

  @Test
  public void sideBySideTest()
  {
    int[] initialValueArray = new int[numValues];
    int[] encodedValueArray = new int[buffSize >> 2];
    int[] decodedValueArray = new int[numValues];

    ByteBuffer initialValues = ByteBuffer.allocateDirect(buffSize).order(ByteOrder.nativeOrder());
    ByteBuffer encodedValues = ByteBuffer.allocateDirect(buffSize + 1024).order(ByteOrder.nativeOrder());
    ByteBuffer decodedValues = ByteBuffer.allocateDirect(buffSize).order(ByteOrder.nativeOrder());

    initialValues.position(0);
    for (int i = 0; i < numValues; i++) {
      final int nextInt = ThreadLocalRandom.current().nextInt(0, upperBound);
      initialValues.putInt(nextInt);
      initialValueArray[i] = nextInt;
    }

    // encode native
    int size = nativeCodec.encode(initialValues, numValues, encodedValues, buffSize << 2);
    encodedValues.position(0);
    encodedValues.limit(size * Integer.BYTES);

    IntWrapper outPos = new IntWrapper(0);
    codec.headlessCompress(initialValueArray, new IntWrapper(0), numValues, encodedValueArray, outPos);


    byte[] bytes = new byte[size * Integer.BYTES];
    int[] encodedBufferAsInts = new int[size];
    for (int i = 0, pos = 0; i < size; i++, pos += Integer.BYTES) {
      encodedBufferAsInts[i] = encodedValues.getInt(pos);
    }

    for (int i = 0; i < size * Integer.BYTES; i++) {
      bytes[i] = encodedValues.get(i);
    }

    byte[] bytes2 = new byte[outPos.get() * Integer.BYTES];
    for (int i = 0, pos = 0; i < outPos.get(); i++, pos += Integer.BYTES) {
      bytes2[pos] = (byte) encodedValueArray[i];
      bytes2[pos + 1] = (byte) (encodedValueArray[i] >> 8);
      bytes2[pos + 2] = (byte) (encodedValueArray[i] >> 16);
      bytes2[pos + 3] = (byte) (encodedValueArray[i] >> 24);
    }

    for (int i = 0; i < outPos.get(); i++) {
      Assert.assertEquals("values not equal at " + i, encodedValueArray[i], encodedBufferAsInts[i]);
    }
    Assert.assertEquals(outPos.get(), encodedBufferAsInts.length);
  }

//  @Test
//  public void nativeExternalFileToJavaTest() throws IOException
//  {
//    // this explodes
//    int encodedSize = 2214;
//
//    ByteBuffer encodedValues = Files.map(new File("numbers.bin"));
//
//    int[] valueArray = new int[numValues];
//    int[] encodedValueArray = new int[encodedSize];
//
//    // copy encoded buffer to int array
//    for (int i = 0; i < encodedSize; i++) {
//      encodedValueArray[i] = encodedValues.getInt();
//    }
//
//    // decode with java
//
//    IntWrapper outPos = new IntWrapper(0);
//    codec.headlessUncompress(encodedValueArray, new IntWrapper(0), encodedSize, valueArray, outPos, numValues);
//
//    Assert.assertEquals(numValues, outPos.get());
//  }
//
//  @Test
//  public void nativeExternalFileToNativeTest() throws IOException
//  {
//    // this works
//    ByteBuffer decodedValues = ByteBuffer.allocateDirect(buffSize).order(ByteOrder.nativeOrder());
//
//    int encodedSize = 2214;
//
//    ByteBuffer encodedValues =Files.map(new File("numbers.bin"));
//
//    int decodedSize = nativeCodec.decode(encodedValues, 0, encodedSize, decodedValues, numValues);
//
//    Assert.assertEquals(decodedSize, numValues);
//  }
}
