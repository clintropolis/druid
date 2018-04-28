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

#ifndef _NATIVE_FASTPFOR_CODEC_H_INCLUDED_
#define _NATIVE_FASTPFOR_CODEC_H_INCLUDED_

// some stuff to get and set things on java 'NativeFastPForCodec' object to re-use between function calls
#include <cstdint>
#include "codecfactory.h"
using namespace FastPForLib;

#define _CODEC_ADDRESS_FIELD "codecAddress"
#define _ALIGNED_BUFFER_ADDRESS_FIELD "alignedBufferAddress"

jfieldID getAddressFieldID(JNIEnv *env, jobject obj, const char* name)
{
    return env->GetFieldID(env->GetObjectClass(obj), name, "J"); // 'J' for long, obviously
}

template <typename T>
T *getAddressField(JNIEnv *env, jobject obj, jfieldID field)
{
    jlong handle = env->GetLongField(obj, field);
    return reinterpret_cast<T *>(handle);
}

template <typename T>
void setAddressField(JNIEnv *env, jobject obj, jfieldID field, T *t)
{
    jlong handle = reinterpret_cast<jlong>(t);
    env->SetLongField(obj, field, handle);
}

IntegerCODEC *getCodec(JNIEnv *env, jobject obj)
{
    return getAddressField<IntegerCODEC>(env, obj, getAddressFieldID(env, obj, _CODEC_ADDRESS_FIELD));
}

void setCodec(JNIEnv *env, jobject obj, IntegerCODEC *c)
{
    setAddressField(env, obj, getAddressFieldID(env, obj, _CODEC_ADDRESS_FIELD), c);
}

uint32_t *getAlignedBuffer(JNIEnv *env, jobject obj)
{
    return getAddressField<uint32_t>(env, obj, getAddressFieldID(env, obj, _ALIGNED_BUFFER_ADDRESS_FIELD));
}

void setAlignedBuffer(JNIEnv *env, jobject obj, uint32_t *buffer)
{
    setAddressField(env, obj, getAddressFieldID(env, obj, _ALIGNED_BUFFER_ADDRESS_FIELD), buffer);
}

#endif