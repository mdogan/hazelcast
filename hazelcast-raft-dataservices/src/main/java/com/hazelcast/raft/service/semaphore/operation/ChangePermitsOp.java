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

package com.hazelcast.raft.service.semaphore.operation;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.raft.RaftGroupId;
import com.hazelcast.raft.impl.RaftOp;
import com.hazelcast.raft.service.semaphore.RaftSemaphoreDataSerializerHook;
import com.hazelcast.raft.service.semaphore.RaftSemaphoreService;

import java.io.IOException;

/**
 * TODO: Javadoc Pending...
 */
public class ChangePermitsOp extends RaftOp implements IdentifiedDataSerializable {

    private String name;
    private int permits;

    public ChangePermitsOp() {
    }

    public ChangePermitsOp(String name, int permits) {
        this.name = name;
        this.permits = permits;
    }

    @Override
    public Object run(RaftGroupId groupId, long commitIndex) {
        RaftSemaphoreService service = getService();
        return service.changePermits(groupId, name, permits);
    }

    @Override
    protected String getServiceName() {
        return RaftSemaphoreService.SERVICE_NAME;
    }

    @Override
    public int getFactoryId() {
        return RaftSemaphoreDataSerializerHook.F_ID;
    }

    @Override
    public int getId() {
        return RaftSemaphoreDataSerializerHook.CHANGE_PERMITS_OP;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeUTF(name);
        out.writeInt(permits);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        name = in.readUTF();
        permits = in.readInt();
    }

    @Override
    protected void toString(StringBuilder sb) {
        sb.append(", name=").append(name)
          .append(", permits=").append(permits);
    }
}