/**
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
package org.apache.bookkeeper.mledger.impl;


import io.netty.util.concurrent.DefaultThreadFactory;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.apache.bookkeeper.mledger.LedgerOffloaderMXBean;
import static org.apache.bookkeeper.mledger.util.SafeRun.safeRun;
import org.apache.bookkeeper.mledger.util.StatsBuckets;
import org.apache.pulsar.common.stats.Rate;

public class LedgerOffloaderMXBeanImpl implements LedgerOffloaderMXBean {

    public static final long[] READ_ENTRY_LATENCY_BUCKETS_USEC = {500, 1_000, 5_000, 10_000, 20_000, 50_000, 100_000,
            200_000, 1000_000};

    private final String name;

    private final ScheduledExecutorService scheduler;
    private long refreshIntervalSeconds;

    // offloadTimeMap record the time cost by one round offload
    private final ConcurrentHashMap<String, Rate> offloadTimeMap = new ConcurrentHashMap<>();
    // offloadErrorMap record error ocurred
    private final ConcurrentHashMap<String, Rate> offloadErrorMap = new ConcurrentHashMap<>();
    // offloadRateMap record the offload rate
    private final ConcurrentHashMap<String, Rate> offloadRateMap = new ConcurrentHashMap<>();


    // readLedgerLatencyBucketsMap record the time cost by ledger read
    private final ConcurrentHashMap<String, StatsBuckets> readLedgerLatencyBucketsMap = new ConcurrentHashMap<>();
    // writeToStorageLatencyBucketsMap record the time cost by write to storage
    private final ConcurrentHashMap<String, StatsBuckets> writeToStorageLatencyBucketsMap = new ConcurrentHashMap<>();
    // writeToStorageErrorMap record the error occurred in write storage
    private final ConcurrentHashMap<String, Rate> writeToStorageErrorMap = new ConcurrentHashMap<>();


    // streamingWriteToStorageRateMap and streamingWriteToStorageErrorMap is for streamingOffload
    private final ConcurrentHashMap<String, Rate> streamingWriteToStorageRateMap = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, Rate> streamingWriteToStorageErrorMap = new ConcurrentHashMap<>();

    // readOffloadIndexLatencyBucketsMap and readOffloadDataLatencyBucketsMap are latency metrics about index and data
    // readOffloadDataRateMap and readOffloadErrorMap is for reading offloaded data
    private final ConcurrentHashMap<String, StatsBuckets> readOffloadIndexLatencyBucketsMap = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, StatsBuckets> readOffloadDataLatencyBucketsMap = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, Rate> readOffloadDataRateMap = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, Rate> readOffloadErrorMap = new ConcurrentHashMap<>();

    public LedgerOffloaderMXBeanImpl(String name, long refreshIntervalSecond) {
        this.name = name;
        this.refreshIntervalSeconds = refreshIntervalSeconds;
        this.scheduler = Executors.newSingleThreadScheduledExecutor(
                new DefaultThreadFactory("ledger-offloader-metrics"));
        this.scheduler.scheduleAtFixedRate(
                safeRun(() -> refreshStats()), refreshIntervalSeconds, refreshIntervalSeconds, TimeUnit.SECONDS);
    }

    public void refreshStats() {
        double seconds = refreshIntervalSeconds;

        if (seconds <= 0.0) {
            // skip refreshing stats
            return;
        }
        offloadTimeMap.values().forEach(rate->rate.calculateRate(seconds));
        offloadErrorMap.values().forEach(rate->rate.calculateRate(seconds));
        offloadRateMap.values().forEach(rate-> rate.calculateRate(seconds));
        readLedgerLatencyBucketsMap.values().forEach(stat-> stat.refresh());
        writeToStorageLatencyBucketsMap.values().forEach(stat -> stat.refresh());
        writeToStorageErrorMap.values().forEach(rate->rate.calculateRate(seconds));
        streamingWriteToStorageRateMap.values().forEach(rate -> rate.calculateRate(seconds));
        streamingWriteToStorageErrorMap.values().forEach(rate->rate.calculateRate(seconds));
        readOffloadDataRateMap.values().forEach(rate->rate.calculateRate(seconds));
        readOffloadErrorMap.values().forEach(rate->rate.calculateRate(seconds));
        readOffloadIndexLatencyBucketsMap.values().forEach(stat->stat.refresh());
        readOffloadDataLatencyBucketsMap.values().forEach(stat->stat.refresh());
    }

    //TODO metrics在namespace这个level的输出。

    @Override
    public String getName() {
        return this.name;
    }

    @Override
    public Map<String, Rate> getOffloadTimes() {
        return offloadTimeMap;
    }

    @Override
    public Map<String, Rate> getOffloadErrors() {
        return offloadErrorMap;
    }

    @Override
    public Map<String, Rate> getOffloadRates() {
        return offloadRateMap;
    }

    @Override
    public Map<String, StatsBuckets> getReadLedgerLatencyBuckets() {
        return readLedgerLatencyBucketsMap;
    }

    @Override
    public Map<String, StatsBuckets> getWriteToStorageLatencyBuckets() {
        return writeToStorageLatencyBucketsMap;
    }

    @Override
    public Map<String, Rate> getWriteToStorageErrors() {
        return writeToStorageErrorMap;
    }

    @Override
    public Map<String, Rate> getStreamingWriteToStorageRates() {
        return streamingWriteToStorageRateMap;
    }

    @Override
    public Map<String, Rate> getStreamingWriteToStorageErrors() {
        return streamingWriteToStorageErrorMap;
    }


    @Override
    public Map<String, StatsBuckets> getReadOffloadIndexLatencyBuckets() {
        return readOffloadIndexLatencyBucketsMap;
    }

    @Override
    public Map<String, StatsBuckets> getReadOffloadDataLatencyBuckets() {
        return readOffloadDataLatencyBucketsMap;
    }

    @Override
    public Map<String, Rate> getReadOffloadRates() {
        return readOffloadDataRateMap;
    }

    @Override
    public Map<String, Rate> getReadOffloadErrors() {
        return readOffloadErrorMap;
    }

    public void recordOffloadTime(String managedLedgerName, long time, TimeUnit unit) {
        Rate adder = offloadTimeMap.get(managedLedgerName);
        if (adder == null) {
            adder = offloadTimeMap.compute(managedLedgerName, (k, v) -> new Rate());
        }
        adder.recordEvent(unit.toMillis(time));
    }

    public void recordOffloadError(String managedLedgerName) {
        Rate adder = offloadErrorMap.get(managedLedgerName);
        if (adder == null) {
            adder = offloadErrorMap.compute(managedLedgerName, (k, v) -> new Rate());
        }
        adder.recordEvent(1);
    }

    public void recordOffloadRate(String managedLedgerName, int size) {
        Rate rate = offloadRateMap.get(managedLedgerName);
        if (rate == null) {
            rate = offloadRateMap.compute(managedLedgerName, (k, v) -> new Rate());
        }
        rate.recordEvent(size);
    }

    public void recordReadLedgerLatency(String managedLedgerName, long latency, TimeUnit unit) {
        StatsBuckets statsBuckets = readLedgerLatencyBucketsMap.get(managedLedgerName);
        if (statsBuckets == null) {
            statsBuckets = readLedgerLatencyBucketsMap.compute(managedLedgerName,
                    (k, v) -> new StatsBuckets(READ_ENTRY_LATENCY_BUCKETS_USEC));
        }
        statsBuckets.addValue(unit.toMicros(latency));
    }

    public void recordWriteToStorageLatency(String managedLedgerName, long latency, TimeUnit unit) {
        StatsBuckets statsBuckets = writeToStorageLatencyBucketsMap.get(managedLedgerName);
        if (statsBuckets == null) {
            statsBuckets = writeToStorageLatencyBucketsMap.compute(managedLedgerName,
                    (k, v) -> new StatsBuckets(READ_ENTRY_LATENCY_BUCKETS_USEC));
        }
        statsBuckets.addValue(unit.toMicros(latency));
    }

    public void recordWriteToStorageError(String managedLedgerName) {
        Rate adder = writeToStorageErrorMap.get(managedLedgerName);
        if (adder == null) {
            adder = writeToStorageErrorMap.compute(managedLedgerName, (k, v) -> new Rate());
        }
        adder.recordEvent(1);
    }

    public void recordStreamingWriteToStorageRate(String managedLedgerName, int size) {
        Rate rate = streamingWriteToStorageRateMap.get(managedLedgerName);
        if (rate == null) {
            rate = streamingWriteToStorageRateMap.compute(managedLedgerName, (k, v) -> new Rate());
        }
        rate.recordEvent(size);
    }

    public void recordStreamingWriteToStorageError(String managedLedgerName) {
        Rate adder = streamingWriteToStorageErrorMap.get(managedLedgerName);
        if (adder == null) {
            adder = streamingWriteToStorageErrorMap.compute(managedLedgerName, (k, v) -> new Rate());
        }
        adder.recordEvent(1);
    }


    public void recordReadOffloadError(String managedLedgerName) {
        Rate adder = readOffloadErrorMap.get(managedLedgerName);
        if (adder == null) {
            adder = readOffloadErrorMap.compute(managedLedgerName, (k, v) -> new Rate());
        }
        adder.recordEvent(1);
    }

    public void recordReadOffloadRate(String managedLedgerName, int size) {
        Rate rate = readOffloadDataRateMap.get(managedLedgerName);
        if (rate == null) {
            rate = readOffloadDataRateMap.compute(managedLedgerName, (k, v) -> new Rate());
        }
        rate.recordEvent(size);
    }

    public void recordReadOffloadIndexLatency(String managedLedgerName, long latency, TimeUnit unit) {
        StatsBuckets statsBuckets = readOffloadIndexLatencyBucketsMap.get(managedLedgerName);
        if (statsBuckets == null) {
            statsBuckets = readOffloadIndexLatencyBucketsMap.compute(managedLedgerName,
                    (k, v) -> new StatsBuckets(READ_ENTRY_LATENCY_BUCKETS_USEC));
        }
        statsBuckets.addValue(unit.toMicros(latency));
    }

    public void recordReadOffloadDataLatency(String managedLedgerName, long latency, TimeUnit unit) {
        StatsBuckets statsBuckets = readOffloadDataLatencyBucketsMap.get(managedLedgerName);
        if (statsBuckets == null) {
            statsBuckets = readOffloadDataLatencyBucketsMap.compute(managedLedgerName,
                    (k, v) -> new StatsBuckets(READ_ENTRY_LATENCY_BUCKETS_USEC));
        }
        statsBuckets.addValue(unit.toMicros(latency));
    }

}
