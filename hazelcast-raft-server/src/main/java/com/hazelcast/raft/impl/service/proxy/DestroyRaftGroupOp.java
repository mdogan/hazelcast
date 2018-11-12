/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hazelcast.raft.impl.service.proxy;

import com.hazelcast.core.ICompletableFuture;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.raft.RaftGroupId;
import com.hazelcast.raft.command.DestroyRaftGroupCmd;
import com.hazelcast.raft.impl.RaftNode;
import com.hazelcast.raft.impl.RaftOp;
import com.hazelcast.raft.impl.InvocationTargetLeaveAware;
import com.hazelcast.raft.impl.service.RaftServiceDataSerializerHook;

/**
 * Replicates a {@link DestroyRaftGroupCmd} to a Raft group.
 */
public class DestroyRaftGroupOp extends RaftReplicateOp implements InvocationTargetLeaveAware, IdentifiedDataSerializable {

    public DestroyRaftGroupOp() {
    }

    public DestroyRaftGroupOp(RaftGroupId groupId) {
        super(groupId);
    }

    @Override
    ICompletableFuture replicate(RaftNode raftNode) {
        return raftNode.replicate(new DestroyRaftGroupCmd());
    }

    @Override
    protected RaftOp getRaftOp() {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean isRetryableOnTargetLeave() {
        return false;
    }

    @Override
    public int getFactoryId() {
        return RaftServiceDataSerializerHook.F_ID;
    }

    @Override
    public int getId() {
        return RaftServiceDataSerializerHook.DESTROY_RAFT_GROUP_OP;
    }
}
