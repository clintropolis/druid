/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */


#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <iostream>
#include <stdint.h>
#include <stdlib.h>

#include "codecfactory.h"
#include "org_apache_druid_processing_codecs_FastPFor_NativeFastPForCodec.h"
#include "NativeFastPForCodec.h"

using namespace FastPForLib;

/**
 * Creates IntegerCODEC object and allocates 16 byte aligned 64kb buffer for decodes against unaligned memory,
 * stuffs them in fields of counterpart 'NativeFastPForCodec' java object
 */
void Java_org_apache_druid_processing_codecs_FastPFor_NativeFastPForCodec_loadCodec(JNIEnv * env, jobject obj, jstring name)
{
  const char *cstr = env->GetStringUTFChars(name, NULL);
  IntegerCODEC &codec = *CODECFactory::getFromName(cstr);

  uint32_t *tmp;
  posix_memalign((void**)&tmp, 16, 1 << 16);

  setCodec(env, obj, &codec);
  setAlignedBuffer(env, obj, tmp);
}

/**
 * encode integers contained in 'decodedData' direct buffer to 'encodedData' direct buffer
 */
jint Java_org_apache_druid_processing_codecs_FastPFor_NativeFastPForCodec_encode(
  JNIEnv * env,
  jobject obj,
  jobject decodedData,
  jint decodedSize,
  jobject encodedData,
  jint encodedSize
)
{
  uint8_t *inBuff = (uint8_t*) env->GetDirectBufferAddress(decodedData);
  uint32_t *outBuff = (uint32_t*) env->GetDirectBufferAddress(encodedData);

  IntegerCODEC *codec = getCodec(env, obj);

  size_t size = encodedSize;
  codec->encodeArray((uint32_t*)inBuff, decodedSize, outBuff, size);

  return size;
}

/**
 * decode data from 'encodedData' direct buffer into 'decodedData' direct buffer
 */
jint Java_org_apache_druid_processing_codecs_FastPFor_NativeFastPForCodec_decode(
  JNIEnv * env,
  jobject obj,
  jobject encodedData,
  jint encodedOffset,
  jint encodedSize,
  jobject decodedData,
  jint decodedSize
)
{
  uint8_t *inBuff = (uint8_t*) env->GetDirectBufferAddress(encodedData);
  uint32_t *outBuff = (uint32_t*) env->GetDirectBufferAddress(decodedData);

  IntegerCODEC *codec = getCodec(env, obj);
  size_t recoveredSize = decodedSize;

  uint32_t *chunk = (uint32_t*)(inBuff + encodedOffset);
  long addr = (long)chunk;

  // simd codecs explode if not aligned to 16 bytes (may or may not be a bug that lib doesn't auto handle this)
  // regardless, copy to aligned location if not already aligned because working instead of exploding is totally rad
  if ((addr & 0xF) == 0) {
    codec->decodeArray(chunk, encodedSize, outBuff, recoveredSize);
  } else {
    uint32_t *tmp = getAlignedBuffer(env, obj);
    std::copy(chunk, chunk + encodedSize, tmp);
    codec->decodeArray(tmp, encodedSize, outBuff, recoveredSize);
  }

  return recoveredSize;
}

/**
 * clean up after ourselves
 */
void Java_org_apache_druid_processing_codecs_FastPFor_NativeFastPForCodec_dispose(JNIEnv * env, jobject obj)
{
  uint32_t *tmp = getAlignedBuffer(env, obj);
  setAlignedBuffer(env, obj, NULL);
  free(tmp);

  IntegerCODEC *codec = getCodec(env, obj);
  setCodec(env, obj, NULL);
  delete &codec;
}