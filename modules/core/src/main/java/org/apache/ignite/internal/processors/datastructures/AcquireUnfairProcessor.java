package org.apache.ignite.internal.processors.datastructures;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.UUID;

/** {@link org.apache.ignite.cache.CacheEntryProcessor} for a acquire operation for unfair mode. */
public final class AcquireUnfairProcessor extends ReentrantProcessor<UUID> {
    /** */
    private static final long serialVersionUID = 8526685073215814916L;

    /** Lock owner. */
    private UUID nodeId;

    /**
     * Empty constructor required for {@link Externalizable}.
     */
    public AcquireUnfairProcessor() {
        // No-op.
    }

    /**
     * Constructor.
     *
     * @param nodeId Lock owner.
     */
    AcquireUnfairProcessor(UUID nodeId) {
        assert nodeId != null;

        this.nodeId = nodeId;
    }

    /** {@inheritDoc} */
    @Override protected LockedModified lock(GridCacheLockState2Base<UUID> state) {
        assert state != null;

        return state.lockOrAdd(nodeId);
    }

    /** {@inheritDoc} */
    @Override public void writeExternal(ObjectOutput out) throws IOException {
        out.writeLong(nodeId.getMostSignificantBits());
        out.writeLong(nodeId.getLeastSignificantBits());
    }

    /** {@inheritDoc} */
    @Override public void readExternal(ObjectInput in) throws IOException {
        nodeId = new UUID(in.readLong(), in.readLong());
    }
}
