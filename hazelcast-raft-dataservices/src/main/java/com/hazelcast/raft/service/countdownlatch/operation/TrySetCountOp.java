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

package com.hazelcast.raft.service.countdownlatch.operation;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.raft.RaftGroupId;
import com.hazelcast.raft.service.countdownlatch.RaftCountDownLatchDataSerializerHook;
import com.hazelcast.raft.service.countdownlatch.RaftCountDownLatchService;

import java.io.IOException;

/**
 * TODO: Javadoc Pending...
 */
public class TrySetCountOp extends AbstractCountDownLatchOp {

    private int count;

    public TrySetCountOp() {
    }

    public TrySetCountOp(String name, int count) {
        super(name);
        this.count = count;
    }

    @Override
    public Object run(RaftGroupId groupId, long commitIndex) {
        RaftCountDownLatchService service = getService();
        return service.trySetCount(groupId, name, count);
    }

    @Override
    public int getId() {
        return RaftCountDownLatchDataSerializerHook.TRY_SET_COUNT_OP;
    }

    @Override
    public void writeData(ObjectDataOutput out)
            throws IOException {
        super.writeData(out);
        out.writeInt(count);
    }

    @Override
    public void readData(ObjectDataInput in)
            throws IOException {
        super.readData(in);
        count = in.readInt();
    }
}