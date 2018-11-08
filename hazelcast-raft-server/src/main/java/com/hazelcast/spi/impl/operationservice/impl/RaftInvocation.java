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

package com.hazelcast.spi.impl.operationservice.impl;

import com.hazelcast.core.IndeterminateOperationStateException;
import com.hazelcast.core.MemberLeftException;
import com.hazelcast.nio.Address;
import com.hazelcast.raft.RaftGroupId;
import com.hazelcast.raft.exception.LeaderDemotedException;
import com.hazelcast.raft.exception.NotLeaderException;
import com.hazelcast.raft.impl.RaftMemberImpl;
import com.hazelcast.raft.impl.service.proxy.InvocationTargetLeaveAware;
import com.hazelcast.spi.ExceptionAction;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.exception.CallerNotMemberException;
import com.hazelcast.spi.exception.TargetNotMemberException;

import static com.hazelcast.spi.ExceptionAction.RETRY_INVOCATION;
import static com.hazelcast.spi.ExceptionAction.THROW_EXCEPTION;
import static com.hazelcast.spi.InvocationBuilder.DEFAULT_DESERIALIZE_RESULT;

/**
 * TODO: Javadoc Pending...
 */
public class RaftInvocation extends Invocation {

    private final RaftInvocationContext raftInvocationContext;
    private final RaftGroupId groupId;
    private volatile MemberCursor memberCursor;
    private volatile RaftMemberImpl lastInvocationEndpoint;
    private volatile Throwable targetLeft;

    public RaftInvocation(Context context, RaftInvocationContext raftInvocationContext, RaftGroupId groupId, Operation op,
                          int retryCount, long retryPauseMillis, long callTimeoutMillis) {
        super(context, op, null, retryCount, retryPauseMillis, callTimeoutMillis, DEFAULT_DESERIALIZE_RESULT);
        this.raftInvocationContext = raftInvocationContext;
        this.groupId = groupId;

        int partitionId = context.partitionService.getPartitionId(groupId);
        op.setPartitionId(partitionId);
    }

    @Override
    protected Address getTarget() {
        RaftMemberImpl targetEndpoint = getTargetEndpoint();
        lastInvocationEndpoint = targetEndpoint;
        return targetEndpoint != null ? targetEndpoint.getAddress() : null;
    }

    @Override
    void notifyNormalResponse(Object value, int expectedBackups) {
        if (!(value instanceof MemberLeftException) && targetLeft != null && isRetryable(value)) {
            String message = this + " failed because the target has left the cluster before response is received";
            value = new IndeterminateOperationStateException(message, targetLeft);
        }

        super.notifyNormalResponse(value, expectedBackups);
        raftInvocationContext.setKnownLeader(groupId, lastInvocationEndpoint);
    }

    @Override
    protected ExceptionAction onException(Throwable t) {
        raftInvocationContext.updateKnownLeaderOnFailure(groupId, t);

        if (t instanceof MemberLeftException) {
            if (shouldFailOnIndeterminateOperationState()) {
                return THROW_EXCEPTION;
            } else if (targetLeft != null) {
                targetLeft = t;
            }
        }

        return isRetryable(t) ? RETRY_INVOCATION : op.onInvocationException(t);
    }

    private boolean isRetryable(Object cause) {
        return cause instanceof NotLeaderException
                || cause instanceof LeaderDemotedException
                || cause instanceof MemberLeftException
                || cause instanceof CallerNotMemberException
                || cause instanceof TargetNotMemberException;
    }

    private RaftMemberImpl getTargetEndpoint() {
        RaftMemberImpl target = raftInvocationContext.getKnownLeader(groupId);
        if (target != null) {
            return target;
        }

        MemberCursor cursor = memberCursor;
        if (cursor == null || !cursor.advance()) {
            cursor = raftInvocationContext.newMemberCursor(groupId);
            if (!cursor.advance()) {
                return null;
            }
            memberCursor = cursor;
        }
        return cursor.get();
    }

    @Override
    protected boolean shouldFailOnIndeterminateOperationState() {
        if (op instanceof InvocationTargetLeaveAware) {
            if (((InvocationTargetLeaveAware) op).isSafeToRetryOnTargetLeave()) {
                return false;
            }
        }

        return raftInvocationContext.shouldFailOnIndeterminateOperationState();
    }
}
