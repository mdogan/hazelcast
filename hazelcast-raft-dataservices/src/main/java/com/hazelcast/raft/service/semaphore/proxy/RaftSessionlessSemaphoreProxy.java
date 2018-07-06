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

package com.hazelcast.raft.service.semaphore.proxy;

import com.hazelcast.core.ISemaphore;
import com.hazelcast.raft.RaftGroupId;
import com.hazelcast.raft.impl.RaftOp;
import com.hazelcast.raft.impl.service.RaftInvocationManager;
import com.hazelcast.raft.service.semaphore.RaftSemaphoreService;
import com.hazelcast.raft.service.semaphore.operation.AcquirePermitsOp;
import com.hazelcast.raft.service.semaphore.operation.AvailablePermitsOp;
import com.hazelcast.raft.service.semaphore.operation.ChangePermitsOp;
import com.hazelcast.raft.service.semaphore.operation.DrainPermitsOp;
import com.hazelcast.raft.service.semaphore.operation.InitSemaphoreOp;
import com.hazelcast.raft.service.semaphore.operation.ReleasePermitsOp;
import com.hazelcast.raft.service.session.SessionAwareProxy;
import com.hazelcast.raft.service.session.SessionManagerService;
import com.hazelcast.raft.service.spi.operation.DestroyRaftObjectOp;
import com.hazelcast.util.ExceptionUtil;

import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static com.hazelcast.raft.service.session.AbstractSessionManager.NO_SESSION_ID;
import static com.hazelcast.util.Preconditions.checkNotNegative;
import static com.hazelcast.util.Preconditions.checkPositive;
import static java.lang.Math.max;

/**
 * TODO: Javadoc Pending...
 */
public class RaftSessionlessSemaphoreProxy extends SessionAwareProxy implements ISemaphore {

    private final String name;
    private final RaftInvocationManager raftInvocationManager;

    public RaftSessionlessSemaphoreProxy(RaftInvocationManager invocationManager, SessionManagerService sessionManager,
                                         RaftGroupId groupId, String name) {
        super(sessionManager, groupId);
        this.name = name;
        this.raftInvocationManager = invocationManager;
    }

    @Override
    public boolean init(int permits) {
        checkNotNegative(permits, "Permits must be non-negative!");
        return join(raftInvocationManager.<Boolean>invoke(groupId, new InitSemaphoreOp(name, permits)));
    }

    @Override
    public void acquire() {
        acquire(1);
    }

    @Override
    public void acquire(int permits) {
        checkPositive(permits, "Permits must be positive!");

        AcquirePermitsOp op = new AcquirePermitsOp(name, NO_SESSION_ID, permits, -1L);
        Future<Long> f = raftInvocationManager.invoke(groupId, op);
        join(f);
    }

    @Override
    public boolean tryAcquire() {
        return tryAcquire(1);
    }

    @Override
    public boolean tryAcquire(int permits) {
        return tryAcquire(permits, 0, TimeUnit.MILLISECONDS);
    }

    @Override
    public boolean tryAcquire(long timeout, TimeUnit unit) {
        return tryAcquire(1, timeout, unit);
    }

    @Override
    public boolean tryAcquire(int permits, long timeout, TimeUnit unit) {
        checkPositive(permits, "Permits must be positive!");
        long timeoutMs = max(0, unit.toMillis(timeout));
        AcquirePermitsOp op = new AcquirePermitsOp(name, NO_SESSION_ID, permits, timeoutMs);
        Future<Boolean> f = raftInvocationManager.invoke(groupId, op);
        return join(f);
    }

    @Override
    public void release() {
        release(1);
    }

    @Override
    public void release(int permits) {
        checkPositive(permits, "Permits must be positive!");
        Future f = raftInvocationManager.invoke(groupId, new ReleasePermitsOp(name, NO_SESSION_ID, permits));
        join(f);
    }

    @Override
    public int availablePermits() {
        return join(raftInvocationManager.<Integer>invoke(groupId, new AvailablePermitsOp(name)));
    }

    @Override
    public int drainPermits() {
        RaftOp op = new DrainPermitsOp(name, NO_SESSION_ID);
        Future<Integer> f = raftInvocationManager.invoke(groupId, op);
        return join(f);
    }

    @Override
    public void reducePermits(int reduction) {
        checkNotNegative(reduction, "Reduction must be non-negative!");
        if (reduction == 0) {
            return;
        }
        join(raftInvocationManager.invoke(groupId, new ChangePermitsOp(name, -reduction)));
    }

    @Override
    public void increasePermits(int increase) {
        checkNotNegative(increase, "Increase must be non-negative!");
        if (increase == 0) {
            return;
        }
        join(raftInvocationManager.invoke(groupId, new ChangePermitsOp(name, increase)));
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public String getPartitionKey() {
        throw new UnsupportedOperationException();
    }

    @Override
    public String getServiceName() {
        return RaftSemaphoreService.SERVICE_NAME;
    }

    @Override
    public void destroy() {
        join(raftInvocationManager.invoke(groupId, new DestroyRaftObjectOp(getServiceName(), name)));
    }

    private <T> T join(Future<T> future) {
        try {
            return future.get();
        } catch (Exception e) {
            throw ExceptionUtil.rethrow(e);
        }
    }

}