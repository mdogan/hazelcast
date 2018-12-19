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

package com.hazelcast.cp.internal.raftop.metadata;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.cp.RaftGroupId;
import com.hazelcast.cp.RaftMember;
import com.hazelcast.cp.internal.RaftMemberImpl;
import com.hazelcast.cp.internal.RaftOp;
import com.hazelcast.cp.internal.MetadataRaftGroupManager;
import com.hazelcast.cp.internal.RaftService;
import com.hazelcast.cp.internal.RaftServiceDataSerializerHook;
import com.hazelcast.cp.internal.IndeterminateOperationStateAware;

import java.io.IOException;

/**
 * When a CP node is shutting down gracefully, or a crashed CP node is removed from the CP sub-system via
 * {@link RaftService#removeCPMember(RaftMember)}, this operation is committed to the Metadata Raft group.
 */
public class TriggerRemoveRaftMemberOp extends RaftOp implements IndeterminateOperationStateAware, IdentifiedDataSerializable {

    private RaftMemberImpl member;

    public TriggerRemoveRaftMemberOp() {
    }

    public TriggerRemoveRaftMemberOp(RaftMemberImpl member) {
        this.member = member;
    }

    @Override
    public Object run(RaftGroupId groupId, long commitIndex) {
        RaftService service = getService();
        MetadataRaftGroupManager metadataManager = service.getMetadataGroupManager();
        metadataManager.triggerRemoveMember(member);
        return null;
    }

    @Override
    public boolean isRetryableOnIndeterminateOperationState() {
        return true;
    }

    @Override
    public String getServiceName() {
        return RaftService.SERVICE_NAME;
    }

    @Override
    public int getFactoryId() {
        return RaftServiceDataSerializerHook.F_ID;
    }

    @Override
    public int getId() {
        return RaftServiceDataSerializerHook.TRIGGER_REMOVE_RAFT_MEMBER_OP;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeObject(member);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        member = in.readObject();
    }

    @Override
    protected void toString(StringBuilder sb) {
        sb.append(", member=").append(member);
    }
}
