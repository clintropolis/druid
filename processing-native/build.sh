#!/bin/bash

# todo: this is pretty lame, find something better

# build fastpfor

git submodule init
git submodule update

echo "building native dependencies"

pushd -n $(pwd)

cd src/lib/FastPFor/

cmake -DCMAKE_POSITION_INDEPENDENT_CODE=ON .

make FastPFor VERBOSE=1


popd

# build java part 1
echo "building java classes and generating jni header"

mvn clean compile -DskipTests -Dforbiddenapis.skip=true -Dcheckstyle.skip=true -Dpmd.skip=true

pushd -n $(pwd)
cd target/classes/

# generate jni header
echo "javah -d ../../src/main/native/FastPFor/ -jni org.apache.druid.processing.codecs.FastPFor.NativeFastPForCodec"
javah -d ../../src/main/native/FastPFor/ -jni org.apache.druid.processing.codecs.FastPFor.NativeFastPForCodec

popd

# build native lib
echo "building libDruidProcessingNative"

jniArch="linux"
nativeLib="linux/amd64/libDruidProcessingNative.so"

if [[ "$OSTYPE" =~ ^darwin ]]; then
  jniArch="darwin"
  nativeLib="darwin/x86_64/libDruidProcessingNative.dylib"
fi

echo "Building $nativeLib"
echo "c++ -std=c++11 -fPIC -march=native -O3 -I$JAVA_HOME/include -I$JAVA_HOME/include/$jniArch -I./src/lib/FastPFor/headers -shared -o src/main/resources/lib/$nativeLib src/main/native/FastPFor/NativeFastPForCodec.cpp -L./src/lib/FastPFor/ -lFastPFor"
c++ -std=c++11 -fPIC -march=native -O3 -I$JAVA_HOME/include -I$JAVA_HOME/include/$jniArch -I./src/lib/FastPFor/headers -shared -o src/main/resources/lib/$nativeLib src/main/native/FastPFor/NativeFastPForCodec.cpp -L./src/lib/FastPFor/ -lFastPFor

# build test file
c++ -std=c++11 -fPIC -march=native -O3 -I$JAVA_HOME/include -I$JAVA_HOME/include/$jniArch -I./src/lib/FastPFor/headers -shared -o src/test/native/nativeWriteEncodedFile src/test/native/nativeWriteEncodedFile.cpp -L./src/lib/FastPFor/ -lFastPFor

