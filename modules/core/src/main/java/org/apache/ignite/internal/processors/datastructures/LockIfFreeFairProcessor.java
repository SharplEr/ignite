package org.apache.ignite.internal.processors.datastructures;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.UUID;

public class LockIfFreeFairProcessor extends ReentrantProcessor<NodeThread> {
    /** */
    private static final long serialVersionUID = -5203497119206044926L;

    /** */
    NodeThread owner;

    /**
     * Empty constructor required for {@link Externalizable}.
     */
    public LockIfFreeFairProcessor() {
        // No-op.
    }

    /** */
    public LockIfFreeFairProcessor(NodeThread owner) {
        assert owner != null;

        this.owner = owner;
    }

    /** {@inheritDoc} */
    @Override protected LockedModified tryLock(GridCacheLockState2Base<NodeThread> state) {
        return state.lockIfFree(owner);
    }

    /** {@inheritDoc} */
    @Override public void writeExternal(ObjectOutput out) throws IOException {
        out.writeLong(owner.nodeId.getMostSignificantBits());
        out.writeLong(owner.nodeId.getLeastSignificantBits());
        out.writeLong(owner.threadId);
    }

    /** {@inheritDoc} */
    @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        UUID nodeId = new UUID(in.readLong(), in.readLong());
        owner = new NodeThread(nodeId, in.readLong());
    }
}
