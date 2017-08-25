package org.apache.ignite.internal.processors.datastructures;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.UUID;
import javax.cache.processor.EntryProcessor;
import javax.cache.processor.EntryProcessorException;
import javax.cache.processor.MutableEntry;

/** EntryProcessor for lock acquire operation for unfair mode. */
public class AcquireUnfairProcessor extends ReentrantProcessor<UUID> {
    /** */
    private static final long serialVersionUID = 8526685073215814916L;

    /** */
    UUID nodeId;

    /**
     * Empty constructor required for {@link Externalizable}.
     */
    public AcquireUnfairProcessor() {
        // No-op.
    }

    /** */
    public AcquireUnfairProcessor(UUID nodeId) {
        assert nodeId != null;

        this.nodeId = nodeId;
    }

    /** {@inheritDoc} */
    @Override protected LockedModified tryLock(GridCacheLockState2Base<UUID> state) {
        return state.lockOrAdd(nodeId);
    }

    /** {@inheritDoc} */
    @Override public void writeExternal(ObjectOutput out) throws IOException {
        out.writeLong(nodeId.getMostSignificantBits());
        out.writeLong(nodeId.getLeastSignificantBits());
    }

    /** {@inheritDoc} */
    @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        nodeId = new UUID(in.readLong(), in.readLong());
    }
}
