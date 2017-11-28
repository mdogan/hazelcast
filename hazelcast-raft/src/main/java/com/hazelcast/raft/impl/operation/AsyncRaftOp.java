package com.hazelcast.raft.impl.operation;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.raft.impl.RaftDataSerializerHook;
import com.hazelcast.raft.impl.service.RaftGroupId;
import com.hazelcast.raft.impl.service.RaftService;
import com.hazelcast.spi.Operation;

import java.io.IOException;

/**
 * TODO: Javadoc Pending...
 *
 */
public abstract class AsyncRaftOp extends Operation implements IdentifiedDataSerializable {

    protected RaftGroupId groupId;

    public AsyncRaftOp() {
    }

    public AsyncRaftOp(RaftGroupId groupId) {
        this.groupId = groupId;
    }

    @Override
    public final boolean returnsResponse() {
        return false;
    }

    @Override
    public final Object getResponse() {
        throw new UnsupportedOperationException();
    }

    @Override
    public String getServiceName() {
        return RaftService.SERVICE_NAME;
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        out.writeObject(groupId);
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        groupId = in.readObject();
    }

    @Override
    public int getFactoryId() {
        return RaftDataSerializerHook.F_ID;
    }
}
