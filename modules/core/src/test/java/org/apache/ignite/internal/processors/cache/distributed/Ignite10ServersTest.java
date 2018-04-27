package org.apache.ignite.internal.processors.cache.distributed;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.util.future.GridFutureAdapter;
import org.apache.ignite.spi.discovery.tcp.ServerImpl;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

public class Ignite10ServersTest extends GridCommonAbstractTest {
    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        return cfg;
    }

    /** {@inheritDoc} */
    protected long getPartitionMapExchangeTimeout() {
        return Long.MAX_VALUE;
    }

    final static int NODE_CNT = 10;

    final ArrayList<Integer> list = new ArrayList<>();

    final AtomicReference<GridFutureAdapter<Boolean>>[] futs = new AtomicReference[NODE_CNT];

    final static int REP = 5;

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        for (int i = 0; i < NODE_CNT; i++)
            futs[i] = new AtomicReference<>(null);

        Random random = new Random();

        for (int i = 0; i < REP; i++)
            list.addAll(Arrays.asList(shuffled(NODE_CNT, random)));
    }

    Integer[] shuffled(int n, Random random) {
        Integer[] idxs = new Integer[n];
        for (int i = 0; i < n; i++) {
            int j = random.nextInt(i + 1);
            idxs[i] = idxs[j];
            idxs[j] = i;
        }

        return idxs;
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        stopAllGrids();
    }

    public void testJoin() throws Exception {
        final AtomicInteger count = new AtomicInteger(0);

        IgniteInternalFuture<?> longRunFut = multithreadedAsync(() -> {
            final int idx = list.get(count.getAndIncrement());

            while (true) {
                GridFutureAdapter<Boolean> fut = futs[idx].get();

                if (fut == null) {
                    GridFutureAdapter<Boolean> newFut = new GridFutureAdapter<>();

                    if (futs[idx].compareAndSet(null, newFut)) {
                        try {
                            System.out.println("!!!~ try on " + idx);
                            startGrid(idx);
                        }
                        catch (Exception e) {
                            newFut.onDone(false);
                            return;
                        }
                        System.out.println("!!!~ on " + idx);
                        newFut.onDone(true);
                    }
                }
                else {
                    final Boolean res;
                    try {
                        System.out.println("!!!~ try get " + idx);
                        res = fut.get();
                        System.out.println("!!!~ got " + idx);
                    }
                    catch (IgniteCheckedException e) {
                        continue;
                    }
                    GridFutureAdapter<Boolean> newFut = new GridFutureAdapter<>();

                    if (futs[idx].compareAndSet(fut, newFut)) {
                        if (res) {
                            System.out.println("!!!~ try off " + idx);
                            stopGrid(idx);
                            System.out.println("!!!~ off" + idx);
                            newFut.onDone(false);
                        }
                        else {
                            try {
                                System.out.println("!!!~ try on " + idx);

                                startGrid(idx);
                            }
                            catch (Exception e) {
                                newFut.onDone(false);
                                return;
                            }
                            System.out.println("!!!~ on " + idx);
                            newFut.onDone(true);
                        }
                    }

                }
            }
        }, list.size(), "worker");

        IgniteInternalFuture<?> testFut = multithreadedAsync(() -> {
            try {
                ServerImpl server = ServerImpl.fut.get();

                System.out.println("!!!~ danger danger");

                Collection<ClusterNode> nodes = server.getRemoteNodes();

                for (ClusterNode node: nodes)
                    System.out.println(node);

                System.out.println("!!!~ not so danger at all");

                ServerImpl.latch.countDown();

            }
            catch (IgniteCheckedException e) {
                e.printStackTrace();
            }
        }, 1, "pam");

        longRunFut.get();

        testFut.get();
    }
}
