package org.apache.ignite.internal.processors.cache.distributed;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.ClientConnectorConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.util.future.GridFutureAdapter;
import org.apache.ignite.internal.util.lang.gridfunc.AtomicIntegerFactoryCallable;
import org.apache.ignite.spi.communication.tcp.TcpCommunicationSpi;
import org.apache.ignite.spi.discovery.tcp.ServerImpl;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.testframework.junits.multijvm.IgniteProcessProxy;
import org.jsr166.ConcurrentLinkedDeque8;

public class Ignite10ServersTest extends GridCommonAbstractTest {
    protected static TcpDiscoveryIpFinder ipFinder = new TcpDiscoveryVmIpFinder(true);

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

//        cfg.setLocalHost("127.0.0.1");
//        cfg.setNetworkTimeout(30_000);
//        cfg.setConnectorConfiguration(null);
//        cfg.setPeerClassLoadingEnabled(false);
//        cfg.setTimeServerPortRange(200);
//
//        ((TcpCommunicationSpi) cfg.getCommunicationSpi()).setSocketWriteTimeout(200);
//        ((TcpCommunicationSpi) cfg.getCommunicationSpi()).setLocalPortRange(200);
//        ((TcpCommunicationSpi) cfg.getCommunicationSpi()).setSharedMemoryPort(-1);
//
//        ((TcpDiscoverySpi) cfg.getDiscoverySpi()).setIpFinder(ipFinder);
//        ((TcpDiscoverySpi) cfg.getDiscoverySpi()).setJoinTimeout(0);
//
//        cfg.setClientFailureDetectionTimeout(200000);
//        cfg.setClientMode(!igniteInstanceName.equals(getTestIgniteInstanceName(0)));
//
//        cfg.setClientConnectorConfiguration(new ClientConnectorConfiguration().setPortRange(1000));

        return cfg;
    }

    final static int NODE_CNT = 10;

    final ArrayList<Integer> list = new ArrayList<>();

    final AtomicReference<GridFutureAdapter<Boolean>>[] futs = new AtomicReference[NODE_CNT];

    final static int REP = 10;

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        for (int i =0 ; i < NODE_CNT; i++)
            futs[i] = new AtomicReference<>(null);

        Random random = new Random();

        for (int i = 0; i < REP; i++)
            list.addAll(Arrays.asList(shuffled(NODE_CNT, random)));
    }

    Integer[] shuffled(int n, Random random) {
        Integer[] idxs = new Integer[n];
        for (int i = 0; i < n; i++) {
            int j = random.nextInt(i+1);
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

        multithreadedAsync(()-> {
                final int idx = list.get(count.getAndIncrement());

                while (true) {
                    GridFutureAdapter<Boolean> fut = futs[idx].get();

                    if (fut == null) {
                        GridFutureAdapter<Boolean> newFut = new GridFutureAdapter<>();

                        if (futs[idx].compareAndSet(null, newFut)) {
                            try {
                                System.out.println("!!!~ try on "+idx);
                                startGrid(idx);
                            }
                            catch (Exception e) {
                                newFut.onDone(false);
                                return;
                            }
                            System.out.println("!!!~ on " + idx);
                            newFut.onDone(true);
                        }
                    } else {
                        final Boolean res;
                        try {
                            System.out.println("!!!~ try get "+idx);
                            res = fut.get();
                            System.out.println("!!!~ got "+idx);
                        }
                        catch (IgniteCheckedException e) {
                            continue;
                        }
                        GridFutureAdapter<Boolean> newFut = new GridFutureAdapter<>();

                        if (futs[idx].compareAndSet(fut, newFut)) {
                            if (res) {
                                System.out.println("!!!~ try off "+idx);
                                stopGrid(idx);
                                System.out.println("!!!~ off" + idx);
                                newFut.onDone(false);
                            } else {
                                try {
                                    System.out.println("!!!~ try on "+idx);

                                    startGrid(idx);
                                }
                                catch (Exception e) {
                                    newFut.onDone(false);
                                    return;
                                }
                                System.out.println("!!!~ on "+idx);
                                newFut.onDone(true);
                            }
                        }

                    }
                }
        }, list.size(), "worker").get();
    }
}
