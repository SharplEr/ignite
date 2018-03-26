/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.benchmarks.jmh.cache;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteLock;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheWriteSynchronizationMode;
import org.apache.ignite.internal.benchmarks.jmh.runner.JmhIdeBenchmarkRunner;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

/**
 * IgniteCache.lock() vs Ignite.reentrantLock().
 */
@Warmup(iterations = 40)
@Measurement(iterations = 20)
@Fork(1)
public class JmhCacheLocksBenchmark extends JmhCacheAbstractBenchmark {
    /** Fixed lock key for Ignite.reentrantLock() and IgniteCache.lock(). */
    private static final String lockKey = "key0";

    /** Number of nodes. */
    static final int MAX_NODES = 10;

    /** Number threads per node. */
    static final int THREADS_PER_NODE = 5;

    /** */
    static final Ignite[] nodes = new Ignite[MAX_NODES];

    /** The counter for getting a node. */
    static final AtomicInteger countForThread = new AtomicInteger(0);

    /** IgniteCache.lock() with a fixed lock key. */
    @State(Scope.Thread)
    public static class CacheLockState {
        /** */
        public final Lock cacheLock;

        /** */
        public CacheLockState() {
            cacheLock = nodes[countForThread.getAndIncrement() % MAX_NODES]
                .cache(DEFAULT_CACHE_NAME).lock(lockKey);
        }
    }

    /**
     * Test IgniteCache.lock() with fixed key and no-op inside.
     */
    @Benchmark
    public void cacheLock(final CacheLockState lockState) {
        lockState.cacheLock.lock();
        lockState.cacheLock.unlock();
    }

    /** Ignite.reentrantLock() with a fixed lock key. */
    @State(Scope.Thread)
    public static class IgniteLockState {
        /** */
        public IgniteLock igniteLock;

        /** */
        @Param({"false", "true"})
        public boolean fair = false;

        /** */
        @Param({"false", "true"})
        public boolean failoverSafe = false;

        /** */
        final int k;

        /** */
        public IgniteLockState() {
            k = countForThread.getAndIncrement() % MAX_NODES;
        }

        @Setup(Level.Trial)
        public void setup() {
            igniteLock = nodes[k]
                .reentrantLock(lockKey, failoverSafe, fair, true);
        }
    }

    /**
     * Test Ignite.reentrantLock() with fixed key and no-op inside.
     */
    @Benchmark
    public void igniteLock(final IgniteLockState lockState) {
        lockState.igniteLock.lock();
        lockState.igniteLock.unlock();
    }

    /**
     * State for new Ignite.reentrantLock().
     */
    @State(Scope.Thread)
    public static class IgniteLockState2 {
        /** */
        public IgniteLock igniteLock;

        /** */
        @Param({"false", "true"})
        public boolean fair = false;

        /** */
        final int k;

        /** */
        public IgniteLockState2() {
            k = countForThread.getAndIncrement() % MAX_NODES;
        }

        @Setup(Level.Trial)
        public void setup() {
            igniteLock = nodes[k]
                .reentrantLock(lockKey + "2", fair, true);
        }
    }

    /**
     * Test new Ignite.reentrantLock() with fixed key and no-op inside.
     */
    @Benchmark
    public void igniteLock2(final IgniteLockState2 lockState) {
        lockState.igniteLock.lock();
        lockState.igniteLock.unlock();
    }

    /**
     * Create locks and put values in the cache.
     */
    @Setup(Level.Trial)
    public void createLock() {
        nodes[0] = node;

        for (int i = 1; i < MAX_NODES; i++)
            nodes[i] = Ignition.start(configuration("node" + (i + 147)));

        cache.putIfAbsent(lockKey, "foo");
    }

    /**
     * Run benchmarks.
     *
     * @param args Arguments.
     * @throws Exception If failed.
     */
    public static void main(String[] args) throws Exception {
        final String simpleClsName = JmhCacheLocksBenchmark.class.getSimpleName();
        final int threads = MAX_NODES* THREADS_PER_NODE;
        final boolean client = false;
        final CacheAtomicityMode atomicityMode = CacheAtomicityMode.TRANSACTIONAL;
        final CacheWriteSynchronizationMode writeSyncMode = CacheWriteSynchronizationMode.FULL_SYNC;

        final String output = simpleClsName +
            "-" + threads + "-threads" +
            "-" + (client ? "client" : "data") +
            "-" + atomicityMode +
            "-" + writeSyncMode;

        final Options opt = new OptionsBuilder()
            .threads(threads)
            .include(simpleClsName)
            .output(output + ".jmh.log")
            .timeUnit(TimeUnit.MICROSECONDS)
            .mode(Mode.AverageTime)
            .jvmArgs(
                "-Xms1g",
                "-Xmx1g",
                "-server",
                "-XX:+AggressiveOpts",
                "-XX:MaxMetaspaceSize=256m",
                JmhIdeBenchmarkRunner.createProperty(PROP_ATOMICITY_MODE, atomicityMode),
                JmhIdeBenchmarkRunner.createProperty(PROP_WRITE_SYNC_MODE, writeSyncMode),
                JmhIdeBenchmarkRunner.createProperty(PROP_DATA_NODES, 4),
                JmhIdeBenchmarkRunner.createProperty(PROP_CLIENT_MODE, client)).build();

        new Runner(opt).run();
    }
}
