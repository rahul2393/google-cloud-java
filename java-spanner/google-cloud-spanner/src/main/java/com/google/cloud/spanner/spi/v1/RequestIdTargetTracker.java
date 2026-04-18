/*
 * Copyright 2026 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.google.cloud.spanner.spi.v1;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nullable;

final class RequestIdTargetTracker {

  private static final Cache<String, String> TARGETS =
      CacheBuilder.newBuilder().maximumSize(100_000L).expireAfterWrite(10, TimeUnit.MINUTES).build();

  private RequestIdTargetTracker() {}

  static void record(String requestId, String targetEndpoint) {
    if (requestId == null || requestId.isEmpty() || targetEndpoint == null || targetEndpoint.isEmpty()) {
      return;
    }
    TARGETS.put(requestId, targetEndpoint);
  }

  @Nullable
  static String get(String requestId) {
    if (requestId == null || requestId.isEmpty()) {
      return null;
    }
    return TARGETS.getIfPresent(requestId);
  }

  static void remove(String requestId) {
    if (requestId == null || requestId.isEmpty()) {
      return;
    }
    TARGETS.invalidate(requestId);
  }

  @VisibleForTesting
  static void clear() {
    TARGETS.invalidateAll();
  }
}
