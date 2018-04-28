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

package org.apache.druid.processing.codecs;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;

// stolen from lz4-java and slightly modified
public class NativeLoader
{
  private static String[] nativeLibraries = new String[]{
      "DruidProcessingNative"
  };

  private enum OS
  {
    LINUX("linux", "so"),
    MAC("darwin", "dylib");

    public final String name, libExtension;

    OS(String name, String libExtension)
    {
      this.name = name;
      this.libExtension = libExtension;
    }
  }

  private static String arch()
  {
    return System.getProperty("os.arch");
  }

  private static OS os()
  {
    String osName = System.getProperty("os.name");
    if (osName.contains("Linux")) {
      return OS.LINUX;
    } else if (osName.contains("Mac")) {
      return OS.MAC;
    } else {
      throw new UnsupportedOperationException("Unsupported operating system: "
                                              + osName);
    }
  }

  private static String resourceName(String resource)
  {
    OS os = os();
    return "/lib/" + os.name + "/" + arch() + "/" + resource + "." + os.libExtension;
  }

  private static boolean loaded = false;

  private static void loadLibrary(String libraryName)
  {
    String libResource = "lib" + libraryName;

    // Try to load library (i.e. lib{libraryName}.so on Linux) from the java.library.path.
    try {
      System.loadLibrary(libraryName);
      loaded = true;
      return;
    }
    catch (UnsatisfiedLinkError ignored) {
      // Doesn't exist, so proceed to loading bundled library.
    }

    String resourceName = resourceName(libResource);
    InputStream is = NativeLoader.class.getResourceAsStream(resourceName);
    if (is == null) {
      throw new UnsupportedOperationException("Unsupported OS/arch, cannot find "
                                              + resourceName
                                              + ". Please try building from source.");
    }
    File tempLib;
    try {
      tempLib = File.createTempFile(libResource, "." + os().libExtension);
      // copy to tempLib
      FileOutputStream out = new FileOutputStream(tempLib);
      try {
        byte[] buf = new byte[4096];
        while (true) {
          int read = is.read(buf);
          if (read == -1) {
            break;
          }
          out.write(buf, 0, read);
        }
        try {
          out.close();
          out = null;
        }
        catch (IOException ignored) {
          // ignore
        }
        System.load(tempLib.getAbsolutePath());
        loaded = true;
      }
      finally {
        try {
          if (out != null) {
            out.close();
          }
        }
        catch (IOException ignored) {
          // ignore
        }
        if (tempLib.exists()) {
          if (!loaded) {
            tempLib.delete();
          } else {
            // try to delete on exit, does it work on Windows?
            tempLib.deleteOnExit();
          }
        }
      }
    }
    catch (IOException e) {
      throw new ExceptionInInitializerError("Cannot unpack " + libResource);
    }
  }

  public static synchronized boolean isLoaded()
  {
    return loaded;
  }

  public static synchronized void load()
  {
    if (loaded) {
      return;
    }


    for (String lib : nativeLibraries) {
      loadLibrary(lib);
    }
  }
}
