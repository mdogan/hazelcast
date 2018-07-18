/*
 *  Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.raft.service.atomicref.operation;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.raft.RaftGroupId;
import com.hazelcast.raft.service.atomicref.AtomicReferenceDataSerializerHook;
import com.hazelcast.raft.service.atomicref.RaftAtomicRef;

import java.io.IOException;

/**
 * TODO: Javadoc Pending...
 */
public class ContainsOp extends AbstractAtomicRefOp implements IdentifiedDataSerializable {

    private Data value;

    public ContainsOp() {
    }

    public ContainsOp(String name, Data value) {
        super(name);
        this.value = value;
    }

    @Override
    public Object run(RaftGroupId groupId, long commitIndex) {
        RaftAtomicRef ref = getAtomicRef(groupId);
        return ref.contains(value);
    }

    @Override
    public int getId() {
        return AtomicReferenceDataSerializerHook.CONTAINS_OP;
    }

    @Override
    public void writeData(ObjectDataOutput out)
            throws IOException {
        super.writeData(out);
        out.writeData(value);
    }

    @Override
    public void readData(ObjectDataInput in)
            throws IOException {
        super.readData(in);
        value = in.readData();
    }
}