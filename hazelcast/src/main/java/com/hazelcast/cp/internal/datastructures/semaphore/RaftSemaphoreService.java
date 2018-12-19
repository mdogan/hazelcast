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

package com.hazelcast.cp.internal.datastructures.semaphore;

import com.hazelcast.config.cp.CPSemaphoreConfig;
import com.hazelcast.core.ISemaphore;
import com.hazelcast.cp.RaftGroupId;
import com.hazelcast.cp.internal.RaftInvocationManager;
import com.hazelcast.cp.internal.datastructures.exception.WaitKeyCancelledException;
import com.hazelcast.cp.internal.datastructures.semaphore.RaftSemaphore.AcquireResult;
import com.hazelcast.cp.internal.datastructures.semaphore.RaftSemaphore.ReleaseResult;
import com.hazelcast.cp.internal.datastructures.semaphore.proxy.RaftSessionAwareSemaphoreProxy;
import com.hazelcast.cp.internal.datastructures.semaphore.proxy.RaftSessionlessSemaphoreProxy;
import com.hazelcast.cp.internal.datastructures.spi.blocking.AbstractBlockingService;
import com.hazelcast.cp.internal.session.ProxySessionManagerService;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.util.ExceptionUtil;

import java.util.Collection;
import java.util.UUID;

import static com.hazelcast.cp.internal.RaftService.getObjectNameForProxy;

/**
 * Contains Raft-based semaphore instances
 */
public class RaftSemaphoreService extends AbstractBlockingService<AcquireInvocationKey, RaftSemaphore, RaftSemaphoreRegistry> {

    /**
     * Name of the service
     */
    public static final String SERVICE_NAME = "hz:raft:semaphoreService";

    public RaftSemaphoreService(NodeEngine nodeEngine) {
        super(nodeEngine);
    }

    @Override
    public ISemaphore createRaftObjectProxy(String name) {
        try {
            RaftGroupId groupId = raftService.createRaftGroupForProxy(name);
            String objectName = getObjectNameForProxy(name);
            CPSemaphoreConfig config = getConfig(name);
            ProxySessionManagerService sessionManager = nodeEngine.getService(ProxySessionManagerService.SERVICE_NAME);
            RaftInvocationManager invocationManager = raftService.getInvocationManager();
            return config != null && config.isJdkCompatible()
                    ? new RaftSessionlessSemaphoreProxy(invocationManager, sessionManager, groupId, objectName)
                    : new RaftSessionAwareSemaphoreProxy(invocationManager, sessionManager, groupId, objectName);
        } catch (Exception e) {
            throw ExceptionUtil.rethrow(e);
        }
    }

    private CPSemaphoreConfig getConfig(String name) {
        return nodeEngine.getConfig().findCPSemaphoreConfig(name);
    }

    public boolean initSemaphore(RaftGroupId groupId, String name, int permits) {
        try {
            Collection<AcquireInvocationKey> acquired = getOrInitRegistry(groupId).init(name, permits);
            notifyWaitKeys(groupId, acquired, true);

            return true;
        } catch (IllegalStateException ignored) {
            return false;
        }
    }

    public int availablePermits(RaftGroupId groupId, String name) {
        RaftSemaphoreRegistry registry = getRegistryOrNull(groupId);
        return registry != null ? registry.availablePermits(name) : 0;
    }

    public boolean acquirePermits(RaftGroupId groupId, long commitIndex, String name, long sessionId, long threadId,
                                  UUID invocationUid, int permits, long timeoutMs) {
        heartbeatSession(groupId, sessionId);
        AcquireInvocationKey key = new AcquireInvocationKey(name, commitIndex, sessionId, threadId, invocationUid, permits);
        AcquireResult result = getOrInitRegistry(groupId).acquire(name, key, timeoutMs);

        if (logger.isFineEnabled()) {
            logger.fine("Semaphore[" + name + "] in " + groupId + " acquired permits: " + result.acquired
                    + " by <" + sessionId + ", " + threadId + ", " + invocationUid + ">");
        }

        notifyCancelledWaitKeys(groupId, name, result.cancelled);

        if (result.acquired == 0) {
            scheduleTimeout(groupId, key, timeoutMs);
        }

        return (result.acquired != 0);
    }

    public void releasePermits(RaftGroupId groupId, String name, long sessionId, long threadId, UUID invocationUid, int permits) {
        heartbeatSession(groupId, sessionId);
        ReleaseResult result = getOrInitRegistry(groupId).release(name, sessionId, threadId, invocationUid, permits);
        notifyCancelledWaitKeys(groupId, name, result.cancelled);
        notifyWaitKeys(groupId, result.acquired, true);

        if (!result.success) {
            throw new IllegalArgumentException();
        }
    }

    public int drainPermits(RaftGroupId groupId, String name, long sessionId, long threadId, UUID invocationUid) {
        heartbeatSession(groupId, sessionId);
        AcquireResult result = getOrInitRegistry(groupId).drainPermits(name, sessionId, threadId, invocationUid);
        notifyCancelledWaitKeys(groupId, name, result.cancelled);

        return result.acquired;
    }

    public boolean changePermits(RaftGroupId groupId, String name, long sessionId, long threadId, UUID invocationUid,
                                 int permits) {
        heartbeatSession(groupId, sessionId);
        ReleaseResult result = getOrInitRegistry(groupId).changePermits(name, sessionId, threadId, invocationUid, permits);
        notifyCancelledWaitKeys(groupId, name, result.cancelled);
        notifyWaitKeys(groupId, result.acquired, true);

        return result.success;
    }

    public long generateThreadId(RaftGroupId groupId, long initialValue) {
        return getOrInitRegistry(groupId).generateThreadId(initialValue);
    }

    private void notifyCancelledWaitKeys(RaftGroupId groupId, String name, Collection<AcquireInvocationKey> waitKeys) {
        if (waitKeys.isEmpty()) {
            return;
        }

        logger.warning("Wait keys: " + waitKeys +  " for Semaphore[" + name + "] in " + groupId + " are cancelled.");

        notifyWaitKeys(groupId, waitKeys, new WaitKeyCancelledException());
    }

    @Override
    protected RaftSemaphoreRegistry createNewRegistry(RaftGroupId groupId) {
        return new RaftSemaphoreRegistry(groupId);
    }

    @Override
    protected Object expiredWaitKeyResponse() {
        return false;
    }

    @Override
    protected String serviceName() {
        return SERVICE_NAME;
    }

}
