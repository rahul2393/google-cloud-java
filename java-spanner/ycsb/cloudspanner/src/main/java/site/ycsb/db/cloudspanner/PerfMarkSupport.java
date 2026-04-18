/**
 * Copyright (c) 2026 YCSB contributors. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you
 * may not use this file except in compliance with the License. You
 * may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied. See the License for the specific language governing
 * permissions and limitations under the License. See accompanying
 * LICENSE file.
 */
package site.ycsb.db.cloudspanner;

import io.perfmark.PerfMark;
import io.perfmark.traceviewer.TraceEventViewer;
import java.io.BufferedWriter;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.Logger;
import site.ycsb.Status;

final class PerfMarkSupport {
  private static final Logger LOGGER = Logger.getLogger(PerfMarkSupport.class.getName());

  private static final String PROP_ENABLED = "cloudspanner.perfmark.enabled";
  private static final String ENV_ENABLED = "YCSB_ENABLE_PERFMARK";
  private static final String PROP_OUTPUT_FILE = "cloudspanner.perfmark.output.file";
  private static final String ENV_OUTPUT_FILE = "YCSB_PERFMARK_TRACE_FILE";
  private static final String PROP_OUTPUT_DIR = "cloudspanner.perfmark.output.dir";
  private static final String ENV_OUTPUT_DIR = "YCSB_PERFMARK_TRACE_DIR";
  private static final String PROP_TRIGGER_FILE = "cloudspanner.perfmark.trigger.file";
  private static final String ENV_TRIGGER_FILE = "YCSB_PERFMARK_TRIGGER_FILE";
  private static final String ENV_HOSTNAME = "HOSTNAME";
  private static final String DEFAULT_OUTPUT_DIR = "/tmp/perfmark";
  private static final long DEFAULT_TRIGGER_POLL_INTERVAL_MILLIS = 1_000L;

  private static volatile boolean enabled;
  private static volatile String configuredOutputFile = "<disabled>";
  private static volatile String configuredTriggerFile = "<disabled>";
  private static volatile boolean watcherRunning;
  private static volatile Thread watcherThread;

  private PerfMarkSupport() {}

  static synchronized void configure(Properties properties) {
    if (enabled) {
      return;
    }
    enabled = getBoolean(properties, PROP_ENABLED, ENV_ENABLED, false);
    if (!enabled) {
      configuredOutputFile = "<disabled>";
      configuredTriggerFile = "<disabled>";
      return;
    }
    configuredOutputFile = resolveOutputPath(properties).toString();
    configuredTriggerFile = resolveTriggerPath(properties).toString();
    PerfMark.setEnabled(true);
    startTriggerWatcher();
    LOGGER.log(
        Level.INFO,
        "Configured PerfMark tracing: output={0}, trigger={1}",
        new Object[] {configuredOutputFile, configuredTriggerFile});
  }

  static boolean isEnabled() {
    return enabled;
  }

  static String getConfiguredOutputFile() {
    return configuredOutputFile;
  }

  static String getConfiguredTriggerFile() {
    return configuredTriggerFile;
  }

  static void startTask(String taskName, String table) {
    if (!enabled) {
      return;
    }
    PerfMark.startTask(taskName);
    attachTag("table", table);
  }

  static void attachTag(String key, String value) {
    if (!enabled || value == null || value.isEmpty()) {
      return;
    }
    PerfMark.attachTag(key, value);
  }

  static void event(String eventName) {
    if (!enabled) {
      return;
    }
    PerfMark.event(eventName);
  }

  static void stopTask(Status status, Throwable error) {
    if (!enabled) {
      return;
    }
    if (status != null) {
      attachTag("status", status.getName());
    }
    if (error != null) {
      attachTag("error", error.getClass().getSimpleName());
    }
    PerfMark.stopTask();
  }

  static synchronized void shutdown() {
    if (!enabled) {
      return;
    }
    stopTriggerWatcher();
    writeTrace("shutdown");
    PerfMark.setEnabled(false);
    enabled = false;
    configuredOutputFile = "<disabled>";
    configuredTriggerFile = "<disabled>";
  }

  private static synchronized void writeTrace(String reason) {
    Path outputPath = Paths.get(configuredOutputFile);
    try {
      Path parent = outputPath.getParent();
      if (parent != null) {
        Files.createDirectories(parent);
      }
      try (BufferedWriter writer = Files.newBufferedWriter(outputPath)) {
        TraceEventViewer.writeTraceHtml(writer);
      }
      LOGGER.log(Level.INFO, "PerfMark trace written to {0} by {1}", new Object[] {outputPath, reason});
    } catch (IOException e) {
      LOGGER.log(Level.WARNING, "Failed to write PerfMark trace to " + outputPath, e);
    }
  }

  private static Path resolveOutputPath(Properties properties) {
    String explicitFile = getString(properties, PROP_OUTPUT_FILE, ENV_OUTPUT_FILE, "");
    if (!explicitFile.isEmpty()) {
      return Paths.get(explicitFile);
    }
    String outputDir = getString(properties, PROP_OUTPUT_DIR, ENV_OUTPUT_DIR, DEFAULT_OUTPUT_DIR);
    return Paths.get(outputDir, buildDefaultFileName());
  }

  private static Path resolveTriggerPath(Properties properties) {
    String explicitFile = getString(properties, PROP_TRIGGER_FILE, ENV_TRIGGER_FILE, "");
    if (!explicitFile.isEmpty()) {
      return Paths.get(explicitFile);
    }
    return Paths.get(configuredOutputFile + ".trigger");
  }

  private static synchronized void startTriggerWatcher() {
    if (watcherRunning) {
      return;
    }
    watcherRunning = true;
    watcherThread = new Thread(new TriggerWatcher(), "perfmark-trigger-watcher");
    watcherThread.setDaemon(true);
    watcherThread.start();
  }

  private static synchronized void stopTriggerWatcher() {
    watcherRunning = false;
    if (watcherThread != null) {
      watcherThread.interrupt();
      watcherThread = null;
    }
  }

  private static final class TriggerWatcher implements Runnable {
    @Override
    public void run() {
      while (watcherRunning) {
        try {
          Path triggerPath = Paths.get(configuredTriggerFile);
          if (Files.exists(triggerPath)) {
            LOGGER.log(Level.INFO, "PerfMark trigger detected at {0}", triggerPath);
            writeTrace("trigger");
            Files.deleteIfExists(triggerPath);
          }
          Thread.sleep(DEFAULT_TRIGGER_POLL_INTERVAL_MILLIS);
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          return;
        } catch (IOException e) {
          LOGGER.log(
              Level.WARNING, "PerfMark trigger watcher failed for " + configuredTriggerFile, e);
        }
      }
    }
  }

  private static String buildDefaultFileName() {
    String hostname = System.getenv(ENV_HOSTNAME);
    if (hostname == null || hostname.isEmpty()) {
      hostname = "unknown-host";
    }
    String runtimeName = ManagementFactory.getRuntimeMXBean().getName().replace('@', '-');
    return "perfmark-" + hostname + "-" + runtimeName + ".html";
  }

  private static boolean getBoolean(
      Properties properties, String propertyName, String envName, boolean defaultValue) {
    return Boolean.parseBoolean(
        getString(properties, propertyName, envName, String.valueOf(defaultValue)));
  }

  private static String getString(
      Properties properties, String propertyName, String envName, String defaultValue) {
    String value = properties.getProperty(propertyName);
    if (value != null) {
      return value;
    }
    value = System.getenv(envName);
    if (value != null) {
      return value;
    }
    return defaultValue;
  }
}
