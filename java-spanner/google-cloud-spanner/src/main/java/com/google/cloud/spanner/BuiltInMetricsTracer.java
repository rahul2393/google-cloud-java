/*
 * Copyright 2024 Google LLC
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

package com.google.cloud.spanner;

import com.google.api.gax.grpc.GrpcStatusCode;
import com.google.api.gax.rpc.ApiException;
import com.google.api.gax.rpc.ApiExceptionFactory;
import com.google.api.gax.rpc.StatusCode;
import com.google.api.gax.tracing.ApiTracer;
import com.google.api.gax.tracing.MethodName;
import com.google.api.gax.tracing.MetricsTracer;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CancellationException;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.annotation.Nullable;

/**
 * Implements built-in metrics tracer.
 *
 * <p>This class extends the {@link MetricsTracer} which computes generic metrics that can be
 * observed in the lifecycle of an RPC operation.
 */
class BuiltInMetricsTracer extends MetricsTracer implements ApiTracer {
  private static final Logger logger = Logger.getLogger(BuiltInMetricsTracer.class.getName());

  private final BuiltInMetricsRecorder builtInOpenTelemetryMetricsRecorder;
  private final boolean allowTargetEndpointAttribute;
  // These are RPC specific attributes and pertain to a specific API Trace
  private final Map<String, String> attributes = new HashMap<>();
  private Float gfeLatency = null;
  private Float afeLatency = null;
  private final TraceWrapper traceWrapper;
  private final ISpan currentSpan;
  private boolean isDirectPathUsed;
  private boolean isAfeEnabled;

  BuiltInMetricsTracer(
      MethodName methodName,
      BuiltInMetricsRecorder builtInOpenTelemetryMetricsRecorder,
      TraceWrapper traceWrapper,
      ISpan currentSpan,
      boolean allowTargetEndpointAttribute) {
    super(methodName, builtInOpenTelemetryMetricsRecorder);
    this.builtInOpenTelemetryMetricsRecorder = builtInOpenTelemetryMetricsRecorder;
    this.allowTargetEndpointAttribute = allowTargetEndpointAttribute;
    this.attributes.put(METHOD_ATTRIBUTE, methodName.toString());
    this.traceWrapper = traceWrapper;
    this.currentSpan = currentSpan;
  }

  /**
   * Adds an annotation that the attempt succeeded. Successful attempt add "OK" value to the status
   * attribute key.
   */
  @Override
  public void attemptSucceeded() {
    try (IScope s = this.traceWrapper.withSpan(this.currentSpan)) {
      super.attemptSucceeded();
      attributes.put(STATUS_ATTRIBUTE, StatusCode.Code.OK.toString());
      builtInOpenTelemetryMetricsRecorder.recordServerTimingHeaderMetrics(
          gfeLatency, afeLatency, attributes, isDirectPathUsed, isAfeEnabled);
    }
  }

  /**
   * Add an annotation that the attempt was cancelled by the user. Cancelled attempt add "CANCELLED"
   * to the status attribute key.
   */
  @Override
  public void attemptCancelled() {
    try (IScope s = this.traceWrapper.withSpan(this.currentSpan)) {
      super.attemptCancelled();
      attributes.put(STATUS_ATTRIBUTE, StatusCode.Code.CANCELLED.toString());
      builtInOpenTelemetryMetricsRecorder.recordServerTimingHeaderMetrics(
          gfeLatency, afeLatency, attributes, isDirectPathUsed, isAfeEnabled);
    }
  }

  /**
   * Adds an annotation that the attempt failed, but another attempt will be made after the delay.
   *
   * @param error the error that caused the attempt to fail.
   * @param delay the amount of time to wait before the next attempt will start.
   *     <p>Failed attempt extracts the error from the throwable and adds it to the status attribute
   *     key.
   */
  @Override
  public void attemptFailedDuration(Throwable error, java.time.Duration delay) {
    try (IScope s = this.traceWrapper.withSpan(this.currentSpan)) {
      Throwable metricsError = normalizeErrorForMetrics(error);
      super.attemptFailedDuration(metricsError, delay);
      attributes.put(STATUS_ATTRIBUTE, updateStatusAndLogIfUnknown("attempt", error, metricsError));
      builtInOpenTelemetryMetricsRecorder.recordServerTimingHeaderMetrics(
          gfeLatency, afeLatency, attributes, isDirectPathUsed, isAfeEnabled);
    }
  }

  /**
   * Adds an annotation that the attempt failed and that no further attempts will be made because
   * retry limits have been reached. This extracts the error from the throwable and adds it to the
   * status attribute key.
   *
   * @param error the last error received before retries were exhausted.
   */
  @Override
  public void attemptFailedRetriesExhausted(Throwable error) {
    try (IScope s = this.traceWrapper.withSpan(this.currentSpan)) {
      Throwable metricsError = normalizeErrorForMetrics(error);
      super.attemptFailedRetriesExhausted(metricsError);
      attributes.put(
          STATUS_ATTRIBUTE,
          updateStatusAndLogIfUnknown("attempt_retries_exhausted", error, metricsError));
      builtInOpenTelemetryMetricsRecorder.recordServerTimingHeaderMetrics(
          gfeLatency, afeLatency, attributes, isDirectPathUsed, isAfeEnabled);
    }
  }

  /**
   * Adds an annotation that the attempt failed and that no further attempts will be made because
   * the last error was not retryable. This extracts the error from the throwable and adds it to the
   * status attribute key.
   *
   * @param error the error that caused the final attempt to fail.
   */
  @Override
  public void attemptPermanentFailure(Throwable error) {
    try (IScope s = this.traceWrapper.withSpan(this.currentSpan)) {
      Throwable metricsError = normalizeErrorForMetrics(error);
      super.attemptPermanentFailure(metricsError);
      attributes.put(
          STATUS_ATTRIBUTE,
          updateStatusAndLogIfUnknown("attempt_permanent_failure", error, metricsError));
      builtInOpenTelemetryMetricsRecorder.recordServerTimingHeaderMetrics(
          gfeLatency, afeLatency, attributes, isDirectPathUsed, isAfeEnabled);
    }
  }

  @Override
  public void operationFailed(Throwable error) {
    try (IScope s = this.traceWrapper.withSpan(this.currentSpan)) {
      Throwable metricsError = normalizeErrorForMetrics(error);
      logIfUnknownStatus("operation", error, metricsError);
      super.operationFailed(metricsError);
    }
  }

  public void recordServerTimingHeaderMetrics(
      Float gfeLatency, Float afeLatency, boolean isDirectPathUsed, boolean isAfeEnabled) {
    this.gfeLatency = gfeLatency;
    this.isDirectPathUsed = isDirectPathUsed;
    this.afeLatency = afeLatency;
    this.isAfeEnabled = isAfeEnabled;
  }

  @Override
  public void addAttributes(Map<String, String> attributes) {
    Map<String, String> filteredAttributes = filterAttributes(attributes);
    super.addAttributes(filteredAttributes);
    this.attributes.putAll(filteredAttributes);
  }

  @Override
  public void addAttributes(String key, String value) {
    if (!shouldAcceptAttribute(key)) {
      return;
    }
    super.addAttributes(key, value);
    this.attributes.put(key, value);
  }

  private Map<String, String> filterAttributes(Map<String, String> attributes) {
    if (allowTargetEndpointAttribute) {
      return attributes;
    }
    Map<String, String> filteredAttributes = new HashMap<>(attributes);
    filteredAttributes.remove(BuiltInMetricsConstant.TARGET_ENDPOINT_KEY.getKey());
    return filteredAttributes;
  }

  private boolean shouldAcceptAttribute(String key) {
    return allowTargetEndpointAttribute
        || !BuiltInMetricsConstant.TARGET_ENDPOINT_KEY.getKey().equals(key);
  }

  private static String extractStatus(@Nullable Throwable error) {
    if (error == null) {
      return StatusCode.Code.OK.toString();
    }
    for (Throwable current = error; current != null; current = current.getCause()) {
      if (current instanceof CancellationException) {
        return StatusCode.Code.CANCELLED.toString();
      }
      if (current instanceof ApiException) {
        return ((ApiException) current).getStatusCode().getCode().toString();
      }
      if (current instanceof SpannerException) {
        return GrpcStatusCode.of(((SpannerException) current).getErrorCode().getGrpcStatusCode())
            .getCode()
            .toString();
      }
      io.grpc.Status status = io.grpc.Status.fromThrowable(current);
      if (status.getCode() != io.grpc.Status.Code.UNKNOWN
          || current instanceof io.grpc.StatusException
          || current instanceof io.grpc.StatusRuntimeException) {
        return status.getCode().toString();
      }
    }
    return StatusCode.Code.UNKNOWN.toString();
  }

  private String updateStatusAndLogIfUnknown(
      String eventType, @Nullable Throwable originalError, @Nullable Throwable metricsError) {
    String status = extractStatus(metricsError);
    if (StatusCode.Code.UNKNOWN.toString().equals(status)) {
      logIfUnknownStatus(eventType, originalError, metricsError);
    }
    return status;
  }

  private void logIfUnknownStatus(
      String eventType, @Nullable Throwable originalError, @Nullable Throwable metricsError) {
    if (!StatusCode.Code.UNKNOWN.toString().equals(extractStatus(metricsError))) {
      return;
    }
    UnknownStatusClassification classification = classifyUnknownStatus(originalError);
    String message =
        String.format(
            "Built-in metrics exported status=UNKNOWN for %s; classification=%s, method=%s,"
                + " target_endpoint=%s, original_error_chain=%s, metrics_error_chain=%s",
            eventType,
            classification.logValue,
            attributes.get(METHOD_ATTRIBUTE),
            attributes.get(BuiltInMetricsConstant.TARGET_ENDPOINT_KEY.getKey()),
            formatThrowableChain(originalError),
            formatThrowableChain(metricsError));
    logger.log(Level.WARNING, message);
    System.err.println(message);
  }

  private static Throwable normalizeErrorForMetrics(@Nullable Throwable error) {
    if (error == null) {
      return null;
    }
    for (Throwable current = error; current != null; current = current.getCause()) {
      if (current instanceof CancellationException || current instanceof ApiException) {
        return current;
      }
      io.grpc.Status status = io.grpc.Status.fromThrowable(current);
      if (status.getCode() != io.grpc.Status.Code.UNKNOWN
          || current instanceof io.grpc.StatusException
          || current instanceof io.grpc.StatusRuntimeException) {
        return ApiExceptionFactory.createException(
            current, GrpcStatusCode.of(status.getCode()), isRetryableStatus(status.getCode()));
      }
    }
    for (Throwable current = error; current != null; current = current.getCause()) {
      if (current instanceof SpannerException) {
        SpannerException spannerException = (SpannerException) current;
        return ApiExceptionFactory.createException(
            spannerException,
            GrpcStatusCode.of(spannerException.getErrorCode().getGrpcStatusCode()),
            spannerException.isRetryable());
      }
    }
    return error;
  }

  private static UnknownStatusClassification classifyUnknownStatus(@Nullable Throwable error) {
    boolean sawRecognizableWrapper = false;
    for (Throwable current = error; current != null; current = current.getCause()) {
      if (current instanceof ApiException) {
        if (((ApiException) current).getStatusCode().getCode() == StatusCode.Code.UNKNOWN) {
          return UnknownStatusClassification.ACTUAL_GRPC_STATUS_UNKNOWN;
        }
        sawRecognizableWrapper = true;
      } else if (current instanceof io.grpc.StatusException
          || current instanceof io.grpc.StatusRuntimeException) {
        io.grpc.Status.Code statusCode = io.grpc.Status.fromThrowable(current).getCode();
        if (statusCode == io.grpc.Status.Code.UNKNOWN) {
          return UnknownStatusClassification.ACTUAL_GRPC_STATUS_UNKNOWN;
        }
        sawRecognizableWrapper = true;
      } else if (current instanceof SpannerException) {
        sawRecognizableWrapper = true;
      }
    }
    if (sawRecognizableWrapper) {
      return UnknownStatusClassification.WRAPPER_WITHOUT_CONCRETE_TRANSPORT_CODE;
    }
    return UnknownStatusClassification.NON_GRPC_INTERNAL_EXCEPTION_NO_STATUS;
  }

  private static String formatThrowableChain(@Nullable Throwable error) {
    if (error == null) {
      return "null";
    }
    StringBuilder chain = new StringBuilder();
    for (Throwable current = error; current != null; current = current.getCause()) {
      if (chain.length() > 0) {
        chain.append(" -> ");
      }
      chain.append(current.getClass().getName());
      if (current.getMessage() != null && !current.getMessage().isEmpty()) {
        chain.append('(').append(current.getMessage()).append(')');
      }
    }
    return chain.toString();
  }

  private static boolean isRetryableStatus(io.grpc.Status.Code statusCode) {
    switch (statusCode) {
      case ABORTED:
      case DEADLINE_EXCEEDED:
      case INTERNAL:
      case RESOURCE_EXHAUSTED:
      case UNAVAILABLE:
        return true;
      default:
        return false;
    }
  }

  private enum UnknownStatusClassification {
    ACTUAL_GRPC_STATUS_UNKNOWN("actual_grpc_status_unknown"),
    NON_GRPC_INTERNAL_EXCEPTION_NO_STATUS("non_grpc_internal_exception_no_status"),
    WRAPPER_WITHOUT_CONCRETE_TRANSPORT_CODE("wrapper_without_concrete_transport_code");

    private final String logValue;

    UnknownStatusClassification(String logValue) {
      this.logValue = logValue;
    }
  }
}
