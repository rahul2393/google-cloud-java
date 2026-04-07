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
import com.google.spanner.v1.GetSessionRequest;
import com.google.spanner.v1.SpannerGrpc;
import io.grpc.CallOptions;
import io.grpc.ClientCall;
import io.grpc.ConnectivityState;
import io.grpc.ManagedChannel;
import io.grpc.Metadata;
import io.grpc.Status;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
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
 *   <li>Sends periodic {@code GetSession} probes to keep replica channels warm and in READY state.
 *   <li>Tracks real traffic vs probe traffic per endpoint.
 *   <li>Evicts endpoints that have had no real traffic for the configured idle duration.
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

  /** Timeout for probe RPCs. */
  private static final long PROBE_TIMEOUT_SECONDS = 10;

  /** Per-endpoint lifecycle state. */
  static final class EndpointState {
    final String address;
    volatile Instant lastProbeAt;
    volatile Instant lastRealTrafficAt;
    volatile Instant lastReadyAt;
    volatile ScheduledFuture<?> probeFuture;

    EndpointState(String address, Instant now) {
      this.address = address;
      this.lastRealTrafficAt = now;
      this.lastProbeAt = null;
      this.lastReadyAt = null;
    }
  }

  private final ChannelEndpointCache endpointCache;
  private final Map<String, EndpointState> endpoints = new ConcurrentHashMap<>();
  private final ScheduledExecutorService scheduler;
  private final AtomicBoolean isShutdown = new AtomicBoolean(false);
  private final long probeIntervalSeconds;
  private final Duration idleEvictionDuration;
  private final Clock clock;
  private final String defaultEndpointAddress;

  private volatile String multiplexedSessionName;
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
   * Sets the multiplexed session name used for GetSession probes. All probers reuse the same
   * session since multiplexed sessions are enabled.
   *
   * <p>The session name is updated on every call because the client may rotate multiplexed sessions
   * over time. Probes must always use the current session to avoid sending GetSession to a stale,
   * deleted session.
   */
  void setMultiplexedSessionName(String sessionName) {
    if (sessionName == null || sessionName.isEmpty()) {
      return;
    }
    String previous = this.multiplexedSessionName;
    this.multiplexedSessionName = sessionName;
    if (previous == null) {
      logger.log(
          Level.FINE, "Lifecycle manager captured session name for probing: {0}", sessionName);
    } else if (!previous.equals(sessionName)) {
      logger.log(
          Level.FINE,
          "Lifecycle manager updated session name for probing: {0} -> {1}",
          new Object[] {previous, sessionName});
    }
  }

  /** Returns the multiplexed session name, or null if not yet captured. */
  String getMultiplexedSessionName() {
    return multiplexedSessionName;
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
          logger.log(Level.INFO, "Scheduling background endpoint creation for address: {0}", addr);
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

  /** Creates an endpoint and starts probing. Runs on the scheduler thread. */
  private void createAndStartProbing(String address) {
    if (isShutdown.get()) {
      return;
    }
    try {
      endpointCache.get(address);
      logger.log(Level.INFO, "Background endpoint creation completed for: {0}", address);
      startProbing(address);
    } catch (Exception e) {
      logger.log(
          Level.WARNING, "Failed to create endpoint for address: " + address + ", will retry", e);
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
        Level.INFO,
        "Prober started for endpoint {0} with interval {1}s",
        new Object[] {address, probeIntervalSeconds});
  }

  /** Stops probing for an endpoint. */
  private void stopProbing(String address) {
    EndpointState state = endpoints.get(address);
    if (state != null && state.probeFuture != null) {
      state.probeFuture.cancel(false);
      state.probeFuture = null;
      logger.log(Level.INFO, "Prober stopped for endpoint: {0}", address);
    }
  }

  /** Sends a GetSession probe to the endpoint, but only if the channel is not already READY. */
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

    // Check channel state: only probe if the channel is not already READY.
    ManagedChannel channel = endpoint.getChannel();
    try {
      ConnectivityState channelState = channel.getState(false);
      if (channelState == ConnectivityState.READY) {
        state.lastReadyAt = clock.instant();
        logger.log(Level.FINE, "Probe skipped for {0}: channel already READY", address);
        return;
      }
      logger.log(
          Level.FINE,
          "Channel {0} in state {1}, sending GetSession probe",
          new Object[] {address, channelState});
    } catch (UnsupportedOperationException e) {
      // If getState() is unsupported, fall through and probe.
    }

    String sessionName = multiplexedSessionName;
    if (sessionName == null || sessionName.isEmpty()) {
      logger.log(
          Level.FINE,
          "Skipping probe for {0}: multiplexed session name not yet available",
          address);
      // Even without a session, request a connection to keep the channel from going idle.
      try {
        channel.getState(true);
      } catch (Exception ignored) {
        // Best effort.
      }
      return;
    }

    GetSessionRequest request = GetSessionRequest.newBuilder().setName(sessionName).build();

    try {
      ClientCall<GetSessionRequest, com.google.spanner.v1.Session> call =
          channel.newCall(
              SpannerGrpc.getGetSessionMethod(),
              CallOptions.DEFAULT.withDeadlineAfter(PROBE_TIMEOUT_SECONDS, TimeUnit.SECONDS));

      call.start(
          new ClientCall.Listener<com.google.spanner.v1.Session>() {
            @Override
            public void onMessage(com.google.spanner.v1.Session message) {
              state.lastProbeAt = clock.instant();
              if (endpoint.isHealthy()) {
                state.lastReadyAt = clock.instant();
              }
              logger.log(Level.FINE, "Probe succeeded for endpoint: {0}", address);
            }

            @Override
            public void onClose(Status status, Metadata trailers) {
              state.lastProbeAt = clock.instant();
              if (!status.isOk()) {
                logger.log(
                    Level.WARNING,
                    "Probe failed for endpoint {0}: {1}",
                    new Object[] {address, status});
              }
            }
          },
          new Metadata());

      call.sendMessage(request);
      call.halfClose();
      call.request(1);
    } catch (Exception e) {
      state.lastProbeAt = clock.instant();
      logger.log(Level.WARNING, "Probe exception for endpoint " + address, e);
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
        Level.INFO,
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

    logger.log(Level.INFO, "Recreating previously evicted endpoint for address: {0}", address);
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

    logger.log(Level.INFO, "Shutting down endpoint lifecycle manager");

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
