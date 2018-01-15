package org.apache.ignite.yardstick.cache;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteTransactions;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.IgniteKernal;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.transactions.Transaction;
import org.yardstickframework.BenchmarkConfiguration;
import org.yardstickframework.BenchmarkUtils;

/** */
public class IgniteSneakyTxBenchmark extends IgniteCacheAbstractBenchmark<String, Integer> {
    /** */
    private IgniteTransactions txs;

    /** */
    protected final List<ClusterNode> targetNodes = new ArrayList<>();

    /** */
    protected IgniteKernal ignite;

    protected IgniteCache<String, Integer> cache;

    protected ArrayList<ArrayList<String>> sets = new ArrayList<>();

    /** {@inheritDoc} */
    @Override public void setUp(BenchmarkConfiguration cfg) throws Exception {
        super.setUp(cfg);

        txs = ignite().transactions();

        ignite = (IgniteKernal)ignite();

        ClusterNode loc = ignite().cluster().localNode();

        Collection<ClusterNode> nodes = ignite().cluster().forServers().nodes();

        for (ClusterNode node : nodes) {
            if (!loc.equals(node))
                targetNodes.add(node);
        }

        if (targetNodes.isEmpty())
            throw new IgniteException("Failed to find remote server nodes [nodes=" + nodes + ']');

        BenchmarkUtils.println(cfg, "Initialized target nodes: " + F.nodeIds(targetNodes) + ']');

        cache = ignite.cache("tx");

        HashMap<String, Integer> map = new HashMap<>();
        for (int i = 0; i < 10000; i++)
            map.put(String.valueOf(-i), i);

        cache.putAll(map);

        Map<ClusterNode, Collection<String>> keysToNode = ignite.<String>affinity("tx")
            .mapKeysToNodes(map.keySet());

        HashMap<ClusterNode, Iterator<String>> nodeIterators = new HashMap<>();

        for (Map.Entry<ClusterNode, Collection<String>> entry : keysToNode.entrySet())
            nodeIterators.put(entry.getKey(), entry.getValue().iterator());

        boolean flag = true;

        while (flag) {
            ArrayList<String> list = new ArrayList<>();

            for (Iterator<String> iter : nodeIterators.values()) {
                if (!iter.hasNext()) {
                    flag = false;
                    break;
                }

                list.add(iter.next());
            }

            if (flag)
                sets.add(list);
        }

        BenchmarkUtils.println(cfg, "Initialized key set: " + sets.size() + ']');
    }

    final AtomicLong count = new AtomicLong(0L);

    @Override public boolean test(Map<Object, Object> ignored) throws Exception {
        try (Transaction tx = txs.txStart()) {
            ArrayList<String> keys = sets.get((int)(count.getAndIncrement() % sets.size()));

            Map<String, Integer> map = cache.getAll(new HashSet<>(keys));

            long count = 0L;

            for (int x : map.values())
                count += x;

            HashMap<String, Integer> result = new HashMap<>();

            for (int i = 0; i < keys.size(); i++) {
                String key = keys.get(i);
                result.put(key, (int)((count*13L + 5L) % (i+1)));
            }

            cache.putAll(result);

            tx.commit();
        }

        return true;
    }

    /** {@inheritDoc} */
    @Override protected IgniteCache<String, Integer> cache() {
        return ignite().cache("tx");
    }
}
