/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.cp.CPGroupId;
import com.hazelcast.cp.internal.CPMemberInfo;
import com.hazelcast.cp.internal.IndeterminateOperationStateAware;
import com.hazelcast.cp.internal.MetadataRaftGroupManager;
import com.hazelcast.cp.internal.RaftOp;
import com.hazelcast.cp.internal.RaftService;
import com.hazelcast.cp.internal.RaftServiceDataSerializerHook;
import com.hazelcast.cp.internal.raft.impl.util.PostponedResponse;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;

import java.io.IOException;

import static com.hazelcast.util.Preconditions.checkTrue;

/**
 * A {@link RaftOp} that adds a new CP member to the CP subsystem.
 * Committed to the Metadata Raft group.
 * Fails with {@link IllegalArgumentException} if the member to be added
 * is already a CP member that is currently being removed.
 */
public class AddCPMemberOp extends RaftOp implements IndeterminateOperationStateAware, IdentifiedDataSerializable {

    private CPMemberInfo member;

    public AddCPMemberOp() {
    }

    public AddCPMemberOp(CPMemberInfo member) {
        this.member = member;
    }

    @Override
    public Object run(CPGroupId groupId, long commitIndex) {
        RaftService service = getService();
        MetadataRaftGroupManager metadataGroupManager = service.getMetadataGroupManager();
        checkTrue(metadataGroupManager.getMetadataGroupId().equals(groupId),
                "Cannot perform CP subsystem management call on " + groupId);
        if (metadataGroupManager.addActiveMember(commitIndex, member)) {
            return null;
        }

        return PostponedResponse.INSTANCE;
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
        return RaftServiceDataSerializerHook.ADD_CP_MEMBER_OP;
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
