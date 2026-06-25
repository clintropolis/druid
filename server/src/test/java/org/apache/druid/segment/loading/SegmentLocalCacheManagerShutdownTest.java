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

package org.apache.druid.segment.loading;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.druid.java.util.emitter.EmittingLogger;
import org.apache.druid.segment.TestHelper;
import org.apache.druid.segment.column.ColumnConfig;
import org.apache.druid.server.metrics.NoopServiceEmitter;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import javax.annotation.Nullable;
import java.io.File;
import java.util.List;

class SegmentLocalCacheManagerShutdownTest
{
  @TempDir
  File cacheRoot;

  private ObjectMapper jsonMapper;

  @BeforeAll
  static void setUpClass()
  {
    EmittingLogger.registerEmitter(new NoopServiceEmitter());
  }

  @BeforeEach
  void setUp()
  {
    jsonMapper = TestHelper.makeJsonMapper();
  }

  @Test
  void testShutdownStopsSelfProvisionedPool()
  {
    // Null pool => the manager creates and owns its loading pool, so shutdown() must stop it.
    final SegmentLocalCacheManager manager = makeManager(null);
    final StorageLoadingThreadPool pool = manager.getLoadingThreadPool();
    Assertions.assertTrue(pool.isAvailable());
    Assertions.assertFalse(pool.getExecutorService().isShutdown());

    manager.shutdown();

    Assertions.assertTrue(pool.getExecutorService().isShutdown());
  }

  @Test
  void testShutdownLeavesExternallyOwnedPoolRunning()
  {
    // A non-null (externally owned) pool must NOT be stopped by the manager; its owner (e.g. the node lifecycle) is.
    final StorageLoadingThreadPool externalPool = StorageLoadingThreadPool.createFromConfig(virtualStorageConfig());
    try {
      final SegmentLocalCacheManager manager = makeManager(externalPool);
      Assertions.assertSame(externalPool, manager.getLoadingThreadPool());
      Assertions.assertFalse(externalPool.getExecutorService().isShutdown());

      manager.shutdown();

      Assertions.assertFalse(externalPool.getExecutorService().isShutdown());
    }
    finally {
      externalPool.stop();
    }
  }

  private SegmentLoaderConfig virtualStorageConfig()
  {
    return new SegmentLoaderConfig()
        .setLocations(List.of(new StorageLocationConfig(cacheRoot, 1024L * 1024L * 1024L, null)))
        .setVirtualStorage(true);
  }

  private SegmentLocalCacheManager makeManager(@Nullable StorageLoadingThreadPool loadingThreadPool)
  {
    final SegmentLoaderConfig config = virtualStorageConfig();
    final List<StorageLocation> storageLocations = config.toStorageLocations();
    return new SegmentLocalCacheManager(
        storageLocations,
        config,
        loadingThreadPool,
        new LeastBytesUsedStorageLocationSelectorStrategy(storageLocations),
        TestHelper.getTestIndexIO(jsonMapper, ColumnConfig.DEFAULT),
        jsonMapper
    );
  }
}
