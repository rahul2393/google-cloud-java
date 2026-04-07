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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import org.junit.After;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class EndpointLifecycleManagerTest {

  private EndpointLifecycleManager manager;

  @After
  public void tearDown() {
    if (manager != null) {
      manager.shutdown();
    }
  }

  @Test
  public void endpointCreationStartsProbing() throws Exception {
    KeyRangeCacheTest.FakeEndpointCache cache = new KeyRangeCacheTest.FakeEndpointCache();
    manager =
        new EndpointLifecycleManager(
            cache, /* probeIntervalSeconds= */ 1, Duration.ofMinutes(30), Clock.systemUTC());

    manager.ensureEndpointExists("server1");

    // Wait for background creation.
    Thread.sleep(500);

    // Endpoint should be created in the cache.
    assertNotNull(cache.getIfPresent("server1"));

    // Should be managed.
    assertTrue(manager.isManaged("server1"));
    assertNotNull(manager.getEndpointState("server1"));
    assertEquals(1, manager.managedEndpointCount());
  }

  @Test
  public void duplicateEnsureEndpointExistsIsNoop() throws Exception {
    KeyRangeCacheTest.FakeEndpointCache cache = new KeyRangeCacheTest.FakeEndpointCache();
    manager =
        new EndpointLifecycleManager(
            cache, /* probeIntervalSeconds= */ 60, Duration.ofMinutes(30), Clock.systemUTC());

    manager.ensureEndpointExists("server1");
    manager.ensureEndpointExists("server1");
    manager.ensureEndpointExists("server1");

    Thread.sleep(300);

    assertEquals(1, manager.managedEndpointCount());
  }

  @Test
  public void defaultEndpointIsNotManaged() {
    KeyRangeCacheTest.FakeEndpointCache cache = new KeyRangeCacheTest.FakeEndpointCache();
    manager =
        new EndpointLifecycleManager(
            cache, /* probeIntervalSeconds= */ 60, Duration.ofMinutes(30), Clock.systemUTC());

    manager.ensureEndpointExists("default");

    assertFalse(manager.isManaged("default"));
    assertEquals(0, manager.managedEndpointCount());
  }

  @Test
  public void probeTrafficDoesNotUpdateLastRealTrafficAt() throws Exception {
    KeyRangeCacheTest.FakeEndpointCache cache = new KeyRangeCacheTest.FakeEndpointCache();
    TestClock clock = new TestClock(Instant.now());
    manager =
        new EndpointLifecycleManager(
            cache, /* probeIntervalSeconds= */ 1, Duration.ofMinutes(30), clock);

    Instant creationTime = clock.instant();
    manager.ensureEndpointExists("server1");
    Thread.sleep(300);

    // Probe traffic should not change lastRealTrafficAt.
    EndpointLifecycleManager.EndpointState state = manager.getEndpointState("server1");
    assertNotNull(state);
    assertEquals(creationTime, state.lastRealTrafficAt);
  }

  @Test
  public void realRoutedTrafficUpdatesLastRealTrafficAt() throws Exception {
    KeyRangeCacheTest.FakeEndpointCache cache = new KeyRangeCacheTest.FakeEndpointCache();
    TestClock clock = new TestClock(Instant.now());
    manager =
        new EndpointLifecycleManager(
            cache, /* probeIntervalSeconds= */ 60, Duration.ofMinutes(30), clock);

    manager.ensureEndpointExists("server1");
    Thread.sleep(300);

    Instant before = clock.instant();
    clock.advance(Duration.ofMinutes(5));
    manager.recordRealTraffic("server1");

    EndpointLifecycleManager.EndpointState state = manager.getEndpointState("server1");
    assertNotNull(state);
    assertTrue(state.lastRealTrafficAt.isAfter(before));
  }

  @Test
  public void endpointWithOnlyProbeTrafficIsEvictedAfterIdleDuration() throws Exception {
    KeyRangeCacheTest.FakeEndpointCache cache = new KeyRangeCacheTest.FakeEndpointCache();
    TestClock clock = new TestClock(Instant.now());
    Duration idleDuration = Duration.ofMinutes(30);
    manager =
        new EndpointLifecycleManager(cache, /* probeIntervalSeconds= */ 60, idleDuration, clock);

    manager.ensureEndpointExists("server1");
    Thread.sleep(300);

    assertTrue(manager.isManaged("server1"));

    // Advance past idle threshold.
    clock.advance(Duration.ofMinutes(31));

    // Trigger eviction check manually.
    manager.checkIdleEviction();

    // Endpoint should be evicted.
    assertFalse(manager.isManaged("server1"));
    assertNull(cache.getIfPresent("server1"));
    assertEquals(0, manager.managedEndpointCount());
  }

  @Test
  public void endpointWithRecentRealTrafficIsNotEvicted() throws Exception {
    KeyRangeCacheTest.FakeEndpointCache cache = new KeyRangeCacheTest.FakeEndpointCache();
    TestClock clock = new TestClock(Instant.now());
    Duration idleDuration = Duration.ofMinutes(30);
    manager =
        new EndpointLifecycleManager(cache, /* probeIntervalSeconds= */ 60, idleDuration, clock);

    manager.ensureEndpointExists("server1");
    Thread.sleep(300);

    // Record real traffic at 20 minutes.
    clock.advance(Duration.ofMinutes(20));
    manager.recordRealTraffic("server1");

    // Advance to 31 minutes (only 11 minutes since last real traffic).
    clock.advance(Duration.ofMinutes(11));
    manager.checkIdleEviction();

    // Should NOT be evicted because last real traffic was 11 minutes ago.
    assertTrue(manager.isManaged("server1"));
  }

  @Test
  public void evictedEndpointIsRecreatedOnDemand() throws Exception {
    KeyRangeCacheTest.FakeEndpointCache cache = new KeyRangeCacheTest.FakeEndpointCache();
    TestClock clock = new TestClock(Instant.now());
    Duration idleDuration = Duration.ofMinutes(30);
    manager =
        new EndpointLifecycleManager(cache, /* probeIntervalSeconds= */ 60, idleDuration, clock);

    manager.ensureEndpointExists("server1");
    Thread.sleep(300);

    // Evict.
    clock.advance(Duration.ofMinutes(31));
    manager.checkIdleEviction();
    assertFalse(manager.isManaged("server1"));

    // Recreate.
    manager.requestEndpointRecreation("server1");
    Thread.sleep(500);

    assertTrue(manager.isManaged("server1"));
    assertNotNull(cache.getIfPresent("server1"));
  }

  @Test
  public void shutdownStopsAllProbing() throws Exception {
    KeyRangeCacheTest.FakeEndpointCache cache = new KeyRangeCacheTest.FakeEndpointCache();
    manager =
        new EndpointLifecycleManager(
            cache, /* probeIntervalSeconds= */ 1, Duration.ofMinutes(30), Clock.systemUTC());

    manager.ensureEndpointExists("server1");
    manager.ensureEndpointExists("server2");
    Thread.sleep(300);

    assertEquals(2, manager.managedEndpointCount());

    manager.shutdown();

    assertEquals(0, manager.managedEndpointCount());
  }

  @Test
  public void emptyOrNullAddressIsIgnored() {
    KeyRangeCacheTest.FakeEndpointCache cache = new KeyRangeCacheTest.FakeEndpointCache();
    manager =
        new EndpointLifecycleManager(
            cache, /* probeIntervalSeconds= */ 60, Duration.ofMinutes(30), Clock.systemUTC());

    manager.ensureEndpointExists(null);
    manager.ensureEndpointExists("");

    assertEquals(0, manager.managedEndpointCount());
  }

  @Test
  public void recordRealTrafficForDefaultEndpointIsIgnored() {
    KeyRangeCacheTest.FakeEndpointCache cache = new KeyRangeCacheTest.FakeEndpointCache();
    manager =
        new EndpointLifecycleManager(
            cache, /* probeIntervalSeconds= */ 60, Duration.ofMinutes(30), Clock.systemUTC());

    // Should not throw or create state.
    manager.recordRealTraffic("default");
    manager.recordRealTraffic(null);
    assertEquals(0, manager.managedEndpointCount());
  }

  /** Test clock that can be advanced manually. */
  private static final class TestClock extends Clock {
    private Instant now;

    TestClock(Instant now) {
      this.now = now;
    }

    void advance(Duration duration) {
      now = now.plus(duration);
    }

    @Override
    public Instant instant() {
      return now;
    }

    @Override
    public ZoneId getZone() {
      return ZoneId.of("UTC");
    }

    @Override
    public Clock withZone(ZoneId zone) {
      return this;
    }
  }
}
