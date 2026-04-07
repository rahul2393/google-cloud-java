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

import com.google.api.core.InternalApi;
import com.google.common.annotations.VisibleForTesting;
import io.grpc.ConnectivityState;
import io.grpc.ManagedChannel;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Manages the lifecycle of location-aware routing endpoints including background probing, traffic
 * tracking, and idle eviction.
 *
 * <p>This manager is the only component that proactively creates routed replica endpoints. It:
 *
 * <ul>
 *   <li>Creates endpoints in the background when new server addresses appear in cache updates.
 *   <li>Periodically checks channel state and uses {@code getState(true)} to warm up IDLE channels
 *       without sending application RPCs.
 *   <li>Tracks real traffic per endpoint.
 *   <li>Evicts endpoints that have had no real traffic for the configured idle duration, or that
 *       are in TRANSIENT_FAILURE state.
 *   <li>Recreates and reprobes endpoints when they are needed again after eviction.
 * </ul>
 */
@InternalApi
class EndpointLifecycleManager {

  private static final Logger logger = Logger.getLogger(EndpointLifecycleManager.class.getName());

  /** Default probe interval: 60 seconds. Keeps channels from drifting into IDLE. */
  @VisibleForTesting static final long DEFAULT_PROBE_INTERVAL_SECONDS = 60;

  /** Default idle eviction threshold: 30 minutes without real traffic. */
  @VisibleForTesting static final Duration DEFAULT_IDLE_EVICTION_DURATION = Duration.ofMinutes(30);

  /** Interval for checking idle eviction: every 5 minutes. */
  private static final long EVICTION_CHECK_INTERVAL_SECONDS = 300;

  /**
   * Maximum consecutive TRANSIENT_FAILURE probes before evicting an endpoint. Gives the channel
   * time to recover from transient network issues before we tear it down and recreate.
   */
  private static final int MAX_TRANSIENT_FAILURE_COUNT = 3;

  /** Per-endpoint lifecycle state. */
  static final class EndpointState {
    final String address;
    volatile Instant lastProbeAt;
    volatile Instant lastRealTrafficAt;
    volatile Instant lastReadyAt;
    volatile ScheduledFuture<?> probeFuture;
    volatile int consecutiveTransientFailures;

    EndpointState(String address, Instant now) {
      this.address = address;
      this.lastRealTrafficAt = now;
      this.lastProbeAt = null;
      this.lastReadyAt = null;
      this.consecutiveTransientFailures = 0;
    }
  }

  private final ChannelEndpointCache endpointCache;
  private final Map<String, EndpointState> endpoints = new ConcurrentHashMap<>();

  /** Active addresses reported by each ChannelFinder (keyed by finder identity). */
  private final Map<Object, Set<String>> activeAddressesPerFinder = new ConcurrentHashMap<>();

  private final ScheduledExecutorService scheduler;
  private final AtomicBoolean isShutdown = new AtomicBoolean(false);
  private final long probeIntervalSeconds;
  private final Duration idleEvictionDuration;
  private final Clock clock;
  private final String defaultEndpointAddress;

  private ScheduledFuture<?> evictionFuture;

  EndpointLifecycleManager(ChannelEndpointCache endpointCache) {
    this(
        endpointCache,
        DEFAULT_PROBE_INTERVAL_SECONDS,
        DEFAULT_IDLE_EVICTION_DURATION,
        Clock.systemUTC());
  }

  @VisibleForTesting
  EndpointLifecycleManager(
      ChannelEndpointCache endpointCache,
      long probeIntervalSeconds,
      Duration idleEvictionDuration,
      Clock clock) {
    this.endpointCache = endpointCache;
    this.probeIntervalSeconds = probeIntervalSeconds;
    this.idleEvictionDuration = idleEvictionDuration;
    this.clock = clock;
    this.defaultEndpointAddress = endpointCache.defaultChannel().getAddress();
    this.scheduler =
        Executors.newScheduledThreadPool(
            1,
            r -> {
              Thread t = new Thread(r, "spanner-endpoint-lifecycle");
              t.setDaemon(true);
              return t;
            });

    // Start periodic eviction checks.
    this.evictionFuture =
        scheduler.scheduleAtFixedRate(
            this::checkIdleEviction,
            EVICTION_CHECK_INTERVAL_SECONDS,
            EVICTION_CHECK_INTERVAL_SECONDS,
            TimeUnit.SECONDS);
  }

  /**
   * Ensures an endpoint exists for the given address. If the endpoint does not exist, creates it in
   * the background and starts probing. If it already exists, this is a no-op.
   *
   * <p>This is called from the cache update path when new server addresses appear.
   */
  void ensureEndpointExists(String address) {
    if (isShutdown.get() || address == null || address.isEmpty()) {
      return;
    }
    // Don't manage the default endpoint.
    if (defaultEndpointAddress.equals(address)) {
      return;
    }

    endpoints.computeIfAbsent(
        address,
        addr -> {
          logger.log(Level.FINE, "Scheduling background endpoint creation for address: {0}", addr);
          EndpointState state = new EndpointState(addr, clock.instant());
          scheduler.submit(() -> createAndStartProbing(addr));
          return state;
        });
  }

  /**
   * Records that real (non-probe) traffic was routed to an endpoint. This refreshes the idle
   * eviction timer for this endpoint.
   */
  void recordRealTraffic(String address) {
    if (address == null || defaultEndpointAddress.equals(address)) {
      return;
    }
    EndpointState state = endpoints.get(address);
    if (state != null) {
      state.lastRealTrafficAt = clock.instant();
    }
  }

  /**
   * Updates the set of active addresses for a given finder and evicts any managed endpoints that
   * are no longer referenced by any finder. This handles the case where a tablet's server address
   * changes (e.g. from server1:15000 to server2:15000) — the old endpoint is shut down promptly
   * instead of lingering until idle eviction.
   *
   * @param finderKey identity of the ChannelFinder reporting its active addresses
   * @param activeAddresses server addresses currently referenced by tablets in this finder
   */
  void updateActiveAddresses(Object finderKey, Set<String> activeAddresses) {
    if (isShutdown.get()) {
      return;
    }
    activeAddressesPerFinder.put(finderKey, activeAddresses);

    // Compute the union of all active addresses across all finders.
    Set<String> allActive = new HashSet<>();
    for (Set<String> addresses : activeAddressesPerFinder.values()) {
      allActive.addAll(addresses);
    }

    // Evict managed endpoints not referenced by any finder.
    List<String> stale = new ArrayList<>();
    for (String address : endpoints.keySet()) {
      if (!allActive.contains(address)) {
        stale.add(address);
      }
    }

    for (String address : stale) {
      logger.log(
          Level.FINE, "Evicting stale endpoint {0}: no longer referenced by any tablet", address);
      evictEndpoint(address);
    }
  }

  /** Creates an endpoint and starts probing. Runs on the scheduler thread. */
  private void createAndStartProbing(String address) {
    if (isShutdown.get()) {
      return;
    }
    try {
      endpointCache.get(address);
      logger.log(Level.FINE, "Background endpoint creation completed for: {0}", address);
      startProbing(address);
    } catch (Exception e) {
      logger.log(
          Level.FINE, "Failed to create endpoint for address: " + address + ", will retry", e);
      // Schedule a retry after one probe interval.
      if (!isShutdown.get()) {
        scheduler.schedule(
            () -> createAndStartProbing(address), probeIntervalSeconds, TimeUnit.SECONDS);
      }
    }
  }

  /** Starts periodic probing for an endpoint. */
  private void startProbing(String address) {
    EndpointState state = endpoints.get(address);
    if (state == null || isShutdown.get()) {
      return;
    }

    // Cancel any existing probe schedule.
    if (state.probeFuture != null) {
      state.probeFuture.cancel(false);
    }

    state.probeFuture =
        scheduler.scheduleAtFixedRate(
            () -> probe(address), 0, probeIntervalSeconds, TimeUnit.SECONDS);
    logger.log(
        Level.FINE,
        "Prober started for endpoint {0} with interval {1}s",
        new Object[] {address, probeIntervalSeconds});
  }

  /** Stops probing for an endpoint. */
  private void stopProbing(String address) {
    EndpointState state = endpoints.get(address);
    if (state != null && state.probeFuture != null) {
      state.probeFuture.cancel(false);
      state.probeFuture = null;
      logger.log(Level.FINE, "Prober stopped for endpoint: {0}", address);
    }
  }

  /**
   * Probes the endpoint by checking channel connectivity state and warming up IDLE channels.
   *
   * <p>Uses {@code getState(true)} to request a connection attempt on IDLE channels instead of
   * sending a GetSession RPC. This is lighter weight and avoids routing application-level RPCs
   * through the endpoint's channel pool.
   *
   * <p>If the channel is in TRANSIENT_FAILURE, increments a consecutive failure counter. After
   * {@link #MAX_TRANSIENT_FAILURE_COUNT} consecutive failures, the endpoint is evicted and shut
   * down so it can be recreated fresh when needed again.
   */
  private void probe(String address) {
    if (isShutdown.get()) {
      return;
    }

    ChannelEndpoint endpoint = endpointCache.getIfPresent(address);
    if (endpoint == null) {
      logger.log(Level.FINE, "Probe skipped for {0}: endpoint not in cache", address);
      return;
    }

    EndpointState state = endpoints.get(address);
    if (state == null) {
      return;
    }

    ManagedChannel channel = endpoint.getChannel();
    state.lastProbeAt = clock.instant();

    try {
      // getState(false) reads current state without triggering a connection.
      ConnectivityState channelState = channel.getState(false);
      logger.log(
          Level.FINE, "Probe for {0}: channel state is {1}", new Object[] {address, channelState});

      switch (channelState) {
        case READY:
          state.lastReadyAt = clock.instant();
          state.consecutiveTransientFailures = 0;
          logger.log(Level.FINE, "Probe for {0}: channel READY, no action needed", address);
          break;

        case IDLE:
          // Warm up the channel by requesting a connection attempt.
          logger.log(
              Level.FINE, "Probe for {0}: channel IDLE, requesting connection (warmup)", address);
          channel.getState(true);
          state.consecutiveTransientFailures = 0;
          break;

        case CONNECTING:
          logger.log(Level.FINE, "Probe for {0}: channel CONNECTING, waiting", address);
          state.consecutiveTransientFailures = 0;
          break;

        case TRANSIENT_FAILURE:
          state.consecutiveTransientFailures++;
          logger.log(
              Level.FINE,
              "Probe for {0}: channel in TRANSIENT_FAILURE ({1}/{2})",
              new Object[] {
                address, state.consecutiveTransientFailures, MAX_TRANSIENT_FAILURE_COUNT
              });
          if (state.consecutiveTransientFailures >= MAX_TRANSIENT_FAILURE_COUNT) {
            logger.log(
                Level.FINE,
                "Evicting endpoint {0}: {1} consecutive TRANSIENT_FAILURE probes",
                new Object[] {address, state.consecutiveTransientFailures});
            evictEndpoint(address);
          }
          break;

        case SHUTDOWN:
          logger.log(Level.FINE, "Probe for {0}: channel SHUTDOWN, evicting endpoint", address);
          evictEndpoint(address);
          break;

        default:
          logger.log(
              Level.FINE,
              "Probe for {0}: unrecognized channel state {1}",
              new Object[] {address, channelState});
          break;
      }
    } catch (UnsupportedOperationException e) {
      logger.log(
          Level.FINE,
          "Probe for {0}: getState() unsupported, cannot determine channel health",
          address);
    }
  }

  /** Checks all managed endpoints for idle eviction. */
  @VisibleForTesting
  void checkIdleEviction() {
    if (isShutdown.get()) {
      return;
    }

    Instant now = clock.instant();
    List<String> toEvict = new ArrayList<>();

    for (Map.Entry<String, EndpointState> entry : endpoints.entrySet()) {
      String address = entry.getKey();
      EndpointState state = entry.getValue();

      // Never evict the default endpoint.
      if (defaultEndpointAddress.equals(address)) {
        continue;
      }

      Duration sinceLastRealTraffic = Duration.between(state.lastRealTrafficAt, now);
      if (sinceLastRealTraffic.compareTo(idleEvictionDuration) > 0) {
        toEvict.add(address);
      }
    }

    for (String address : toEvict) {
      evictEndpoint(address);
    }
  }

  /** Evicts an endpoint: stops probing, shuts down the channel pool, removes from cache. */
  private void evictEndpoint(String address) {
    logger.log(
        Level.FINE,
        "Evicting idle endpoint {0}: no real traffic for {1}",
        new Object[] {address, idleEvictionDuration});

    stopProbing(address);
    endpoints.remove(address);
    endpointCache.evict(address);
  }

  /**
   * Requests that an evicted endpoint be recreated. The endpoint is created in the background and
   * probing starts immediately. The endpoint will only become eligible for location-aware routing
   * once it reaches READY state.
   */
  void requestEndpointRecreation(String address) {
    if (isShutdown.get() || address == null || address.isEmpty()) {
      return;
    }
    if (defaultEndpointAddress.equals(address)) {
      return;
    }

    // Only recreate if not already managed.
    if (endpoints.containsKey(address)) {
      return;
    }

    logger.log(Level.FINE, "Recreating previously evicted endpoint for address: {0}", address);
    EndpointState state = new EndpointState(address, clock.instant());
    if (endpoints.putIfAbsent(address, state) == null) {
      scheduler.submit(() -> createAndStartProbing(address));
    }
  }

  /** Returns whether an endpoint is being actively managed. */
  boolean isManaged(String address) {
    return endpoints.containsKey(address);
  }

  /** Returns the endpoint state for testing. */
  @VisibleForTesting
  EndpointState getEndpointState(String address) {
    return endpoints.get(address);
  }

  /** Returns the number of managed endpoints. */
  @VisibleForTesting
  int managedEndpointCount() {
    return endpoints.size();
  }

  /** Shuts down the lifecycle manager and all probing. */
  void shutdown() {
    if (!isShutdown.compareAndSet(false, true)) {
      return;
    }

    logger.log(Level.FINE, "Shutting down endpoint lifecycle manager");

    if (evictionFuture != null) {
      evictionFuture.cancel(false);
    }

    for (EndpointState state : endpoints.values()) {
      if (state.probeFuture != null) {
        state.probeFuture.cancel(false);
      }
    }
    endpoints.clear();

    scheduler.shutdown();
    try {
      if (!scheduler.awaitTermination(5, TimeUnit.SECONDS)) {
        scheduler.shutdownNow();
      }
    } catch (InterruptedException e) {
      scheduler.shutdownNow();
      Thread.currentThread().interrupt();
    }
  }
}
