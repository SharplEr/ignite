package org.apache.ignite.internal;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCompute;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.compute.ComputeJob;
import org.apache.ignite.compute.ComputeJobAdapter;
import org.apache.ignite.compute.ComputeJobResult;
import org.apache.ignite.compute.ComputeTaskAdapter;
import org.apache.ignite.compute.ComputeTaskFuture;
import org.apache.ignite.compute.ComputeTaskMapAsync;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.util.future.IgniteFinishedFutureImpl;
import org.apache.ignite.lang.IgniteFuture;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * This test can't end without IgniteCompute#withExecutore() because tasks will borrow all threads from the thread pool
 * and will hang in waiting of subtasks, while subtasks are waiting for free threads.
 */
public class GridJobWithSubTasksTest extends GridCommonAbstractTest {
    /** */
    public final void testExecutors() throws Exception {
        //Test string from the Beatles song.
        final String testString = "Ob-la-di, ob-la-da life goes on, bra, Lala how the life goes on.";
        final IgniteConfiguration conf = getConfiguration("grid");
        final int poolSize = conf.getPublicThreadPoolSize();
        final ArrayList<ComputeTaskFuture<Integer>> futs = new ArrayList<>(poolSize);
        //Count of all created tasks.
        int taskCount = 0;
        try (Ignite ignite = startGrid("grid", conf)) {
            IgniteCompute comp = ignite.compute();
            for (int i = 0; i < poolSize; i++) {
                comp = comp.withAsync();
                final Integer obj = comp.execute(new TestComputeTask(ignite, String.valueOf(i)), testString);

                if (obj == null) {
                    final ComputeTaskFuture<Integer> fut = comp.<Integer>future();
                    if (fut != null)
                        futs.add(fut);
                }
                else
                    taskCount += obj;
            }

            for (final ComputeTaskFuture<Integer> fut : futs)
                taskCount += fut.get();
        }
        //18 - magic number
        assertEquals(18 * poolSize, taskCount);
        stopAllGrids();
    }

    /**
     * Task create sub tasks.
     */
    @ComputeTaskMapAsync
    public static class TestComputeTask extends ComputeTaskAdapter<String, Integer> {
        /** */
        final Ignite ignite;
        /** */
        final String name;

        /** */
        public TestComputeTask(final Ignite ignite, final String name) {
            this.ignite = ignite;
            this.name = name;
        }

        /** {@inheritDoc} */
        @NotNull @Override public final Map<? extends ComputeJob, ClusterNode> map(final List<ClusterNode> subgrid,
            @Nullable final String arg) {
            if (arg==null) return Collections.<ComputeJob, ClusterNode>emptyMap();
            Iterator<ClusterNode> it = subgrid.iterator();
            final String[] splited = arg.split(String.valueOf(arg.charAt(0)));
            final Map<ComputeJob, ClusterNode> map = new HashMap<>(splited.length);

            for (final String word : splited) {
                // If we used all nodes, restart the iterator.
                if (!it.hasNext())
                    it = subgrid.iterator();

                final ClusterNode node = it.next();

                map.put(new ComputeJobAdapter() {
                    /** {@inheritDoc} */
                    @NotNull @Override public IgniteFuture<Integer> execute() {
                        final IgniteCompute compute = ignite.compute().withAsync().withExecutor(name);
                        final Integer ans = compute.execute(TestSubComputeTask.class, word);
                        if (ans != null)
                            return new IgniteFinishedFutureImpl<>(ans);

                        final ComputeTaskFuture<Integer> fut = compute.future();
                        if (fut == null) {
                            assert false : "Never going to happen.";
                            return new IgniteFinishedFutureImpl<>(0);
                        }

                        return fut;
                    }
                }, node);
            }

            return map;
        }

        /** {@inheritDoc} */
        @NotNull @Override public final Integer reduce(final List<ComputeJobResult> results) {
            int sum = 1;

            for (final ComputeJobResult res : results)
                if ((res != null) && (res.<IgniteFuture<Integer>>getData() != null))
                    sum += (res.<IgniteFuture<Integer>>getData().get() + 1);

            return sum;
        }
    }

    /**
     * Sub task for TestComputeTask.
     */
    @ComputeTaskMapAsync
    public static class TestSubComputeTask extends ComputeTaskAdapter<String, Integer> {
        /** {@inheritDoc} */
        @NotNull @Override public final Map<? extends ComputeJob, ClusterNode> map(final List<ClusterNode> subgrid,
            @Nullable final String arg) {
            if (arg==null) return Collections.<ComputeJob, ClusterNode>emptyMap();
            Iterator<ClusterNode> it = subgrid.iterator();
            final String[] splited = arg.split(" ");
            final Map<ComputeJob, ClusterNode> map = new HashMap<>(splited.length);

            for (final String word : splited) {
                // If we used all nodes, restart the iterator.
                if (!it.hasNext())
                    it = subgrid.iterator();

                final ClusterNode node = it.next();

                map.put(new MyComputeJobAdapter(), node);
            }

            return map;
        }

        /** {@inheritDoc} */
        @NotNull @Override public final Integer reduce(final List<ComputeJobResult> results) {
            int sum = 1;

            for (final ComputeJobResult res : results)
                sum += res.<Integer>getData();

            return sum;
        }
    }

    private static class MyComputeJobAdapter extends ComputeJobAdapter {
        /** {@inheritDoc} */
        @NotNull @Override public Integer execute() {
            return 1;
        }
    }
}
