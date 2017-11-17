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

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.UUID;
import javax.cache.processor.EntryProcessorException;
import javax.cache.processor.MutableEntry;
import org.apache.ignite.cache.CacheEntryProcessor;
import org.jetbrains.annotations.Nullable;

/** {@link CacheEntryProcessor} for a release operation in fair mode. */
public final class ReleaseFairProcessor
    implements CacheEntryProcessor<GridCacheInternalKey, GridCacheLockState2Base<LockOwner>, LockOwner>,
    Externalizable {

    /** */
    private static final long serialVersionUID = 6727594514511280293L;

    /** Lock owner. */
    private LockOwner owner;

    /**
     * Empty constructor required for {@link Externalizable}.
     */
    public ReleaseFairProcessor() {
        // No-op.
    }

    /**
     * Constructor.
     *
     * @param owner Lock owner.
     */
    ReleaseFairProcessor(LockOwner owner) {
        assert owner != null;

        this.owner = owner;
    }

    /** {@inheritDoc} */
    @Nullable @Override public LockOwner process(
        MutableEntry<GridCacheInternalKey, GridCacheLockState2Base<LockOwner>> entry,
        Object... objects) throws EntryProcessorException {

        assert entry != null;

        if (entry.exists()) {
            GridCacheLockState2Base<LockOwner> state = entry.getValue();

            LockOwner nextOwner = state.unlock(owner);

            // Always update value in right using.
            entry.setValue(state);

            return nextOwner;
        }

        return null;
    }

    /** {@inheritDoc} */
    @Override public void writeExternal(ObjectOutput out) throws IOException {
        out.writeLong(owner.nodeId.getMostSignificantBits());
        out.writeLong(owner.nodeId.getLeastSignificantBits());
        out.writeLong(owner.threadId);
    }

    /** {@inheritDoc} */
    @Override public void readExternal(ObjectInput in) throws IOException {
        UUID nodeId = new UUID(in.readLong(), in.readLong());

        owner = new LockOwner(nodeId, in.readLong());
    }
}
