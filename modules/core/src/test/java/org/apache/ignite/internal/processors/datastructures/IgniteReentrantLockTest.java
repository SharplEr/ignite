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

package org.apache.ignite.internal.processors.datastructures;

import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteInterruptedException;
import org.apache.ignite.IgniteLock;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.configuration.AtomicConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.util.future.GridFutureAdapter;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

/**
 * Tests for {@link GridCacheLockImpl2Fair} and for {@link GridCacheLockImpl2Unfair}.
 */
public class IgniteReentrantLockTest extends GridCommonAbstractTest {
    /** IP finder. */
    protected static final TcpDiscoveryIpFinder ipFinder = new TcpDiscoveryVmIpFinder(true);

    /** */
    protected static final int NODES_CNT = 4;

    /** */
    private static final int THREAD_CNT = 5;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        TcpDiscoverySpi spi = new TcpDiscoverySpi();

        spi.setIpFinder(ipFinder);

        cfg.setDiscoverySpi(spi);

        AtomicConfiguration atomicCfg = new AtomicConfiguration();

        assertNotNull(atomicCfg);

        atomicCfg.setCacheMode(CacheMode.REPLICATED);

        cfg.setAtomicConfiguration(atomicCfg);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        startGridsMultiThreaded(NODES_CNT);
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();
    }

    /**
     * Test a creating of the reentrant lock.
     */
    public void testInitialization() {
        try (IgniteLock lock = createReentrantLock(0, "unfair lock", false)) {
        }

        try (IgniteLock lock = createReentrantLock(0, "fair lock", true)) {
        }
    }

    /**
     * Test a sequential lock acquiring and releasing.
     *
     * @throws Exception If failed.
     */
    public void testReentrantLock() throws Exception {
        testReentrantLock(true);
        testReentrantLock(false);
    }

    /**
     * Test a sequential lock acquiring and releasing.
     *
     * @throws Exception If failed.
     */
    public void testReentrantLockWithInterrupted() throws Exception {
        Thread.interrupted();
        testReentrantLock(true);
        Thread.interrupted();
        testReentrantLock(false);
    }

    /**
     * Test a sequential lock acquiring and releasing.
     *
     * @throws Exception If failed.
     */
    public void testReentrantLock(boolean fair) throws Exception {
        for (int i = 0; i < NODES_CNT; i++) {
            final IgniteLock lock = createReentrantLock(i, fair ? "fair lock" : "unfair lock", fair);

            assertEquals(0, lock.getHoldCount());

            // Other node can still don't see update, but eventually we reach lock.isLocked == false.
            GridTestUtils.waitForCondition(() -> !lock.isLocked(), 500L);

            lock.lock();

            assertEquals(1, lock.getHoldCount());

            assertTrue(lock.isLocked());
            assertTrue(lock.isHeldByCurrentThread());

            lock.lock();

            assertEquals(2, lock.getHoldCount());

            assertTrue(lock.isLocked());
            assertTrue(lock.isHeldByCurrentThread());

            lock.unlock();

            assertEquals(1, lock.getHoldCount());

            assertTrue(lock.isLocked());
            assertTrue(lock.isHeldByCurrentThread());

            lock.unlock();

            assertEquals(0, lock.getHoldCount());

            assertFalse(lock.isLocked());
            assertFalse(lock.isHeldByCurrentThread());
        }
    }

    /**
     * Test a sequential lock acquiring and releasing with timeout.
     */
    public void testReentrantLockTimeout() {
        testReentrantLockTimeout(false);
        testReentrantLockTimeout(true);
    }

    /**
     * Test a sequential lock acquiring and releasing with timeout.
     */
    void testReentrantLockTimeout(boolean fair) {
        for (int i = 0; i < NODES_CNT; i++) {
            IgniteLock lock1 = createReentrantLock(i, fair ? "fair lock" : "unfair lock", fair);
            IgniteLock lock2 = createReentrantLock((i + 1) % NODES_CNT, fair ? "fair lock" : "unfair lock", fair);

            assertTrue(lock1.tryLock(10, TimeUnit.MILLISECONDS));

            assertTrue(lock1.isLocked());
            assertTrue(lock1.isHeldByCurrentThread());

            long start = System.nanoTime();

            assertFalse(lock2.tryLock(500, TimeUnit.MILLISECONDS));

            long delta = (System.nanoTime() - start) / 1_000_000L;

            assertTrue(delta >= 500L);
            assertTrue(delta < 600L);

            lock1.unlock();

            assertFalse(lock1.isHeldByCurrentThread());
        }
    }

    /**
     * Test a sequential lock acquiring and releasing with interrupt before locking.
     *
     * @throws Exception If failed.
     */
    public void testReentrantLockInterruptiblyBefore() throws Exception {
        testReentrantLockInterruptiblyBefore(false);
        testReentrantLockInterruptiblyBefore(true);
    }

    /**
     * Test a sequential lock acquiring and releasing with interrupt before locking.
     *
     * @throws IgniteCheckedException If failed.
     */
    void testReentrantLockInterruptiblyBefore(boolean fair) throws IgniteCheckedException {
        for (int i = 0; i < NODES_CNT; i++) {
            IgniteLock lock1 = createReentrantLock(i, fair ? "fair lock" : "unfair lock", fair);
            final IgniteLock lock2 = createReentrantLock(
                (i + 1) % NODES_CNT, fair ? "fair lock" : "unfair lock", fair
            );

            lock1.lockInterruptibly();

            assertTrue(lock1.isLocked());
            assertTrue(lock1.isHeldByCurrentThread());

            IgniteInternalFuture<Void> fut = GridTestUtils.runAsync(() -> {
                Thread.currentThread().interrupt();

                try {
                    lock2.lockInterruptibly();

                    fail();
                }
                catch (IgniteInterruptedException ignored) {
                    // No-op.
                }
                return null;
            }, "worker");

            fut.get(10_000L);

            lock1.unlock();

            assertFalse(lock1.isHeldByCurrentThread());
        }
    }

    /**
     * Test a sequential lock acquiring and releasing with interrupt after locking.
     *
     * @throws Exception If failed.
     */
    public void testReentrantLockInterruptiblyAfter() throws Exception {
        testReentrantLockInterruptiblyAfter(false);
        testReentrantLockInterruptiblyAfter(true);
    }

    /**
     * Test a sequential lock acquiring and releasing with interrupt after locking.
     *
     * @throws IgniteCheckedException If failed.
     */
    void testReentrantLockInterruptiblyAfter(boolean fair) throws IgniteCheckedException {
        for (int i = 0; i < NODES_CNT; i++) {
            IgniteLock lock1 = createReentrantLock(i, fair ? "fair lock" : "unfair lock", fair);
            final IgniteLock lock2 = createReentrantLock(
                (i + 1) % NODES_CNT, fair ? "fair lock" : "unfair lock", fair
            );

            lock1.lockInterruptibly();

            assertTrue(lock1.isLocked());
            assertTrue(lock1.isHeldByCurrentThread());

            final GridFutureAdapter<Thread> threadFut = new GridFutureAdapter<>();

            IgniteInternalFuture<Long> fut = GridTestUtils.runMultiThreadedAsync((Callable<Void>) () -> {
                threadFut.onDone(Thread.currentThread());

                try {
                    lock2.lockInterruptibly();

                    fail();
                }
                catch (IgniteInterruptedException ignored) {
                    // No-op.
                }
                return null;
            }, 1, "worker");

            Thread thread = threadFut.get();

            GridTestUtils.waitForCondition(lock2::hasQueuedThreads,
                500L);

            thread.interrupt();

            fut.get(10_000L);

            lock1.unlock();

            assertFalse(lock1.isHeldByCurrentThread());
        }
    }

    /**
     * Test an async lock acquiring and releasing with many nodes.
     *
     * @throws Exception If failed.
     */
    public void testReentrantLockMultinode() throws Exception {
        testReentrantLockMultinode(false);
        testReentrantLockMultinode(true);
    }

    /**
     * Test an async lock acquiring and releasing with many nodes.
     *
     * @throws Exception If failed.
     */
    public void testReentrantLockMultinode(final boolean fair) throws Exception {
        final AtomicInteger inx = new AtomicInteger(0);

        GridTestUtils.runMultiThreadedAsync((Callable<Void>) () -> {
            IgniteLock lock = createReentrantLock(inx.getAndIncrement() % NODES_CNT,
                fair ? "fair lock" : "unfair lock", fair);

            lock.lock();

            try {
                assertTrue(lock.isLocked());
            }
            finally {
                lock.unlock();
            }

            return null;
        }, NODES_CNT * THREAD_CNT, "worker").get(30_000L);
    }

    /**
     * Test an async lock acquiring and releasing with many nodes and timeout.
     *
     * @throws Exception If failed.
     */
    public void testReentrantLockMultinodeTimeout() throws Exception {
        testReentrantLockMultinodeTimeout(false, false);
        testReentrantLockMultinodeTimeout(false, true);
        testReentrantLockMultinodeTimeout(true, false);
        testReentrantLockMultinodeTimeout(true, true);
    }

    /**
     * Test an async lock acquiring and releasing with many nodes and timeout.
     *
     * @throws Exception If failed.
     */
    public void testReentrantLockMultinodeTimeout(final boolean fair, final boolean time) throws Exception {
        final AtomicInteger inx = new AtomicInteger(0);

        GridTestUtils.runMultiThreadedAsync((Callable<Void>) () -> {
            IgniteLock lock = createReentrantLock(inx.getAndIncrement() % NODES_CNT,
                fair ? "fair lock" : "unfair lock", fair);

            if (time ? lock.tryLock(10, TimeUnit.NANOSECONDS) : lock.tryLock()) {
                try {
                    assertTrue(lock.isLocked());
                }
                finally {
                    lock.unlock();
                }
            }

            return null;
        }, NODES_CNT * THREAD_CNT, "worker").get(30_000L);
    }

    /**
     * Test an async lock acquiring and releasing with many nodes with {@link IgniteLock#tryLock()}.
     *
     * @throws Exception If failed.
     */
    public void testReentrantLockMultinodeTryLock() throws Exception {
        testReentrantLockMultinodeTryLock(false);
        testReentrantLockMultinodeTryLock(true);
    }

    /**
     * Test an async lock acquiring and releasing with many nodes with {@link IgniteLock#tryLock()}.
     *
     * @throws Exception If failed.
     */
    public void testReentrantLockMultinodeTryLock(final boolean fair) throws Exception {
        final AtomicInteger inx = new AtomicInteger(0);

        GridTestUtils.runMultiThreadedAsync((Callable<Void>) () -> {
            IgniteLock lock = createReentrantLock(inx.getAndIncrement() % NODES_CNT,
                fair ? "fair lock1" : "unfair lock", fair);

            if (lock.tryLock()) {
                try {
                    assertTrue(lock.isLocked());
                }
                finally {
                    lock.unlock();
                }
            }

            return null;
        }, NODES_CNT * THREAD_CNT, "worker").get(30_000L);
    }

    /**
     * Test an async lock acquiring and releasing with many nodes with nodes failing in unfair mode.
     *
     * @throws Exception If failed.
     */
    public void testReentrantLockMultinodeFailoverUnfair() throws Exception {
        testReentrantLockMultinodeFailover(false);
    }

    /**
     * Test an async lock acquiring and releasing with many nodes with nodes failing in fair mode.
     *
     * @throws Exception If failed.
     */
    public void testReentrantLockMultkinodeFailoverFair() throws Exception {
        testReentrantLockMultinodeFailover(true);
    }

    /**
     * Test an async lock acquiring and releasing with many nodes with nodes failing.
     *
     * @throws Exception If failed.
     */
    public void testReentrantLockMultinodeFailover(final boolean fair) throws Exception {
        final AtomicInteger inx = new AtomicInteger(0);

        GridTestUtils.runMultiThreadedAsync((Callable<Void>) () -> {
            int i = inx.getAndIncrement();

            boolean shouldClose = ThreadLocalRandom.current().nextBoolean();

            IgniteLock lock = createReentrantLock(i, fair ? "fair lock" : "unfair lock", fair);

            lock.lock();

            boolean stoped = false;
            try {
                if (shouldClose) {
                    try {
                        G.stop(grid(i).name(), true);
                        stoped = true;
                    }
                    catch (Exception ignored) {
                        // No-op.
                    }

                    return null;
                }

                assertTrue(lock.isLocked());
            }
            finally {
                try {
                    if (!stoped) {
                        lock.unlock();
                        lock.close();
                    }
                }
                catch (IllegalStateException ignored) {
                    // No-op.
                }
            }

            return null;
        }, NODES_CNT, "worker").get(30_000L);
    }

    /**
     * Test an async lock acquiring and releasing with many nodes with nodes failing with two locks with fair mode.
     *
     * @throws Exception If failed.
     */
    public void testReentrantLockMultinodeFailoverMultilocksUnfair() throws Exception {
        testReentrantLockMultinodeFailoverMultilocks(false);
    }

    /**
     * Test an async lock acquiring and releasing with many nodes with nodes failing with two locks with fair mode.
     *
     * @throws Exception If failed.
     */
    public void testReentrantLockMultinodeFailoverMultilocksFair() throws Exception {
        testReentrantLockMultinodeFailoverMultilocks(true);
    }

    /**
     * Test an async lock acquiring and releasing with many nodes with nodes failing with two locks.
     *
     * @throws Exception If failed.
     */
    public void testReentrantLockMultinodeFailoverMultilocks(final boolean fair) throws Exception {
        final AtomicInteger inx = new AtomicInteger(0);

        GridTestUtils.runMultiThreadedAsync((Callable<Void>) () -> {
            int i = inx.getAndIncrement();

            boolean shouldClose = ThreadLocalRandom.current().nextBoolean();

            IgniteLock lock1 = createReentrantLock(i, fair ? "lock1f" : "lock1u", fair);
            IgniteLock lock2 = createReentrantLock(i, fair ? "lock2f" : "lock2u", fair);

            lock1.lock();

            try {
                lock2.lock();

                try {
                    if (shouldClose) {
                        try {
                            G.stop(grid(i).name(), true);
                        }
                        catch (Exception ignored) {
                            // No-op.
                        }

                        return null;
                    }

                    assertTrue(lock2.isLocked());
                    assertTrue(lock1.isLocked());
                }
                finally {
                    try {
                        lock2.unlock();
                    }
                    catch (IllegalStateException ignored) {
                        // No-op.
                    }
                }
            }
            finally {
                try {
                    lock1.unlock();
                }
                catch (IllegalStateException ignored) {
                    // No-op.
                }
            }

            return null;
        }, NODES_CNT, "worker").get(30_000L);
    }

    /**
     * Test {@link GridCacheLockImpl2Fair#hasQueuedThreads()} and {@link GridCacheLockImpl2Unfair#hasQueuedThreads()}.
     *
     * @throws IgniteCheckedException If failed.
     */
    public void testHasQueuedThreads() throws IgniteCheckedException, InterruptedException {
        testHasQueuedThreads(false);
        testHasQueuedThreads(true);
    }

    /**
     * Test {@link GridCacheLockImpl2Fair#hasQueuedThreads()} or {@link GridCacheLockImpl2Unfair#hasQueuedThreads()}.
     *
     * @param fair Fair mode on.
     * @throws IgniteCheckedException If failed.
     */
    public void testHasQueuedThreads(final boolean fair) throws IgniteCheckedException, InterruptedException {
        final CountDownLatch holdingLatch = new CountDownLatch(1);
        final CountDownLatch lockDoneLatch = new CountDownLatch(1);
        final IgniteLock lock = createReentrantLock(0, fair ? "lockf" : "locku", fair);

        IgniteInternalFuture<Long> fut = GridTestUtils.runMultiThreadedAsync(() -> {
            lock.lock();

            try {
                lockDoneLatch.countDown();

                holdingLatch.await();
            }
            catch (InterruptedException ignored) {
                // No-op.
            }
            finally {
                lock.unlock();
            }
        }, 2, "worker");

        lockDoneLatch.await();

        try {
            assertTrue(
                "One thread will acquire the lock and another will waiting (but not immediately)," +
                    " but result is " + lock.hasQueuedThreads(),
                GridTestUtils.waitForCondition(lock::hasQueuedThreads,
                    500L));

            assertTrue(lock.hasQueuedThreads());
        }
        finally {
            holdingLatch.countDown();
        }

        fut.get();

        assertFalse(lock.hasQueuedThreads());

        lock.lock();

        try {
            assertFalse(lock.hasQueuedThreads());
        }
        finally {
            lock.unlock();
        }

        assertFalse(lock.hasQueuedThreads());
    }

    /**
     * Test {@link GridCacheLockImpl2Fair#hasQueuedThread(Thread)} or
     * {@link GridCacheLockImpl2Unfair#hasQueuedThread(Thread)}.
     *
     * @throws IgniteCheckedException If failed.
     * @throws InterruptedException If failed.
     */
    public void testHasQueuedThread() throws IgniteCheckedException, InterruptedException {
        testHasQueuedThread(false);
        testHasQueuedThread(true);
    }

    /**
     * Test {@link GridCacheLockImpl2Fair#hasQueuedThread(Thread)} or
     * {@link GridCacheLockImpl2Unfair#hasQueuedThread(Thread)}.
     *
     * @param fair Fair mode on.
     * @throws IgniteCheckedException If failed.
     * @throws InterruptedException If failed.
     */
    public void testHasQueuedThread(final boolean fair) throws IgniteCheckedException, InterruptedException {
        final CountDownLatch holdingLatch = new CountDownLatch(1);
        final CountDownLatch lockDoneLatch = new CountDownLatch(1);
        final IgniteLock lock = createReentrantLock(0, fair ? "lockf" : "locku", fair);
        final LinkedBlockingQueue<Thread> threads = new LinkedBlockingQueue<>();

        IgniteInternalFuture<Long> fut = GridTestUtils.runMultiThreadedAsync(() -> {
            threads.add(Thread.currentThread());

            lock.lock();

            try {
                lockDoneLatch.countDown();

                holdingLatch.await();
            }
            catch (InterruptedException ignored) {
                // No-op.
            }
            finally {
                lock.unlock();
            }

        }, 2, "worker");

        lockDoneLatch.await();

        Thread[] ts = new Thread[2];

        ts[0] = threads.take();
        ts[1] = threads.take();

        try {
            assertNotNull(ts[0]);
            assertNotNull(ts[1]);

            assertTrue(
                "One thread will acquire the lock and another will waiting (but not immediately)," +
                    " but result is [" + lock.hasQueuedThread(ts[0]) + ", " + lock.hasQueuedThread(ts[1]) + "]",
                GridTestUtils.waitForCondition(() -> lock.hasQueuedThread(ts[0]) ^ lock.hasQueuedThread(ts[1]),
                    500L));
        }
        finally {
            holdingLatch.countDown();
        }

        fut.get();

        assertFalse(lock.hasQueuedThread(ts[0]) || lock.hasQueuedThread(ts[1]));

        lock.lock();

        try {
            assertFalse(lock.hasQueuedThread(ts[0]) || lock.hasQueuedThread(ts[1]));
        }
        finally {
            lock.unlock();
        }

        assertFalse(lock.hasQueuedThread(ts[0]) || lock.hasQueuedThread(ts[1]));
    }

    /**
     * Create a lock.
     *
     * @param lockName Reentrant lock name.
     * @param fair Fairness flag.
     * @return Distributed reentrant lock.
     * @throws Exception If failed.
     */
    private IgniteLock createReentrantLock(int cnt, String lockName, boolean fair) {
        assert lockName != null;
        assert cnt >= 0;

        IgniteLock lock = grid(cnt).reentrantLock(lockName, fair, true);

        assertNotNull(lock);
        assertEquals(lockName, lock.name());
        assertTrue(lock.isFailoverSafe());
        assertEquals(lock.isFair(), fair);

        return lock;
    }
}
