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
import java.util.HashSet;
import java.util.UUID;
import org.jetbrains.annotations.Nullable;

/** The unfair implementation for shared lock state. */
public final class GridCacheLockState2Unfair extends GridCacheLockState2Base<UUID> {
    /** */
    private static final long serialVersionUID = 6727594514511280291L;

    /**
     * Empty constructor required for {@link Externalizable}.
     */
    public GridCacheLockState2Unfair() {
        super();
    }

    /** {@inheritDoc} */
    @Override GridCacheLockState2Base<UUID> withNode(UUID node) {
        GridCacheLockState2Unfair state = new GridCacheLockState2Unfair();

        state.nodes = new HashSet<>(this.nodes);

        state.addNode(node);

        state.ownerSet = this.ownerSet;
        state.owners = this.owners;
        state.gridStartTime = this.gridStartTime;

        return state;
    }

    /**
     * Constructor.
     *
     * @param gridStartTime Cluster start time.
     */
    GridCacheLockState2Unfair(long gridStartTime) {
        super(gridStartTime);
    }

    /** {@inheritDoc} */
    @Nullable @Override UUID onNodeRemoved(UUID id) {
        if (ownerSet.remove(id)) {
            boolean lockReleased = owners.getFirst().equals(id);

            owners.remove(id);

            if (lockReleased && !owners.isEmpty())
                return owners.getFirst();
        }

        return null;
    }

    /** {@inheritDoc} */
    @Override public void writeItem(ObjectOutput out, UUID item) throws IOException {
        out.writeLong(item.getMostSignificantBits());
        out.writeLong(item.getLeastSignificantBits());
    }

    /** {@inheritDoc} */
    @Override public UUID readItem(ObjectInput in) throws IOException {
        return new UUID(in.readLong(), in.readLong());
    }

    /** {@inheritDoc} */
    @Override protected boolean checkConsistency() {
        if (owners.size() != ownerSet.size())
            return false;

        if (!nodes.containsAll(ownerSet))
            return false;

        for (UUID id : owners) {
            if (!ownerSet.contains(id))
                return false;
        }

        return true;
    }
}
