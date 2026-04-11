/*
 * Copyright 2026 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.google.cloud.spanner.spi.v1;

import static com.google.api.gax.grpc.GrpcCallContext.TRACER_KEY;

import com.google.api.gax.tracing.ApiTracer;
import com.google.cloud.spanner.CompositeTracer;
import io.grpc.CallOptions;
import io.grpc.Channel;
import io.grpc.ClientCall;
import io.grpc.ClientInterceptor;
import io.grpc.ForwardingClientCall.SimpleForwardingClientCall;
import io.grpc.ForwardingClientCallListener.SimpleForwardingClientCallListener;
import io.grpc.Metadata;
import io.grpc.MethodDescriptor;
import io.grpc.Status;
import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.trace.Span;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Records client-side gRPC lifecycle milestones on the active RPC span.
 *
 * <p>These events reflect interceptor callbacks in the client, not transport frame timestamps.
 */
class GrpcStreamTracerInterceptor implements ClientInterceptor {
  private static final AttributeKey<Long> REQUEST_MESSAGE_SEQUENCE_KEY =
      AttributeKey.longKey("grpc.request_message.sequence");
  private static final AttributeKey<String> STATUS_CODE_KEY =
      AttributeKey.stringKey("grpc.status_code");

  static final String REQUEST_HEADERS_SENT = "gRPC request headers sent";
  static final String REQUEST_MESSAGE_SENT = "gRPC request message sent";
  static final String RESPONSE_HEADERS_RECEIVED = "gRPC response headers received";
  static final String FIRST_RESPONSE_MESSAGE_RECEIVED = "gRPC first response message received";
  static final String RESPONSE_TRAILERS_RECEIVED = "gRPC response trailers received";
  static final String STREAM_CLOSED = "gRPC stream closed";

  @Override
  public <ReqT, RespT> ClientCall<ReqT, RespT> interceptCall(
      MethodDescriptor<ReqT, RespT> method, CallOptions callOptions, Channel next) {
    AtomicReference<Span> callSpanRef = new AtomicReference<>(resolveSpan(callOptions));
    AtomicLong requestMessageSequence = new AtomicLong();
    AtomicLong responseMessageSequence = new AtomicLong();

    return new SimpleForwardingClientCall<ReqT, RespT>(next.newCall(method, callOptions)) {
      @Override
      public void start(Listener<RespT> responseListener, Metadata headers) {
        addEvent(callSpanRef, REQUEST_HEADERS_SENT);
        super.start(
            new SimpleForwardingClientCallListener<RespT>(responseListener) {
              @Override
              public void onHeaders(Metadata headers) {
                addEvent(callSpanRef, RESPONSE_HEADERS_RECEIVED);
                super.onHeaders(headers);
              }

              @Override
              public void onMessage(RespT message) {
                if (responseMessageSequence.incrementAndGet() == 1L) {
                  addEvent(callSpanRef, FIRST_RESPONSE_MESSAGE_RECEIVED);
                }
                super.onMessage(message);
              }

              @Override
              public void onClose(Status status, Metadata trailers) {
                Attributes statusAttributes = Attributes.of(STATUS_CODE_KEY, status.getCode().name());
                addEvent(callSpanRef, RESPONSE_TRAILERS_RECEIVED, statusAttributes);
                addEvent(callSpanRef, STREAM_CLOSED, statusAttributes);
                super.onClose(status, trailers);
              }
            },
            headers);
      }

      @Override
      public void sendMessage(ReqT message) {
        addEvent(
            callSpanRef,
            REQUEST_MESSAGE_SENT,
            Attributes.of(REQUEST_MESSAGE_SEQUENCE_KEY, requestMessageSequence.incrementAndGet()));
        super.sendMessage(message);
      }
    };
  }

  private static Span captureCurrentSpan(Span existingSpan) {
    Span currentSpan = Span.current();
    if (currentSpan.getSpanContext().isValid()) {
      return currentSpan;
    }
    return existingSpan;
  }

  private static Span resolveSpan(CallOptions callOptions) {
    ApiTracer tracer = callOptions.getOption(TRACER_KEY);
    if (tracer instanceof CompositeTracer) {
      Span tracedSpan = ((CompositeTracer) tracer).getOpenTelemetrySpan();
      if (tracedSpan.getSpanContext().isValid()) {
        return tracedSpan;
      }
    }
    return Span.current();
  }

  private static void addEvent(AtomicReference<Span> callSpanRef, String eventName) {
    Span span = captureCurrentSpan(callSpanRef.get());
    callSpanRef.set(span);
    if (span != null && span.getSpanContext().isValid()) {
      span.addEvent(eventName);
    }
  }

  private static void addEvent(
      AtomicReference<Span> callSpanRef, String eventName, Attributes attributes) {
    Span span = captureCurrentSpan(callSpanRef.get());
    callSpanRef.set(span);
    if (span != null && span.getSpanContext().isValid()) {
      span.addEvent(eventName, attributes);
    }
  }
}
