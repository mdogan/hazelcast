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

package com.hazelcast.client.cp.internal.datastructures.semaphore;

import com.hazelcast.client.cp.internal.session.ClientProxySessionManager;
import com.hazelcast.client.impl.clientside.HazelcastClientInstanceImpl;
import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.impl.protocol.codec.CPGroupDestroyCPObjectCodec;
import com.hazelcast.client.impl.protocol.codec.CPSemaphoreAcquireCodec;
import com.hazelcast.client.impl.protocol.codec.CPSemaphoreAvailablePermitsCodec;
import com.hazelcast.client.impl.protocol.codec.CPSemaphoreChangeCodec;
import com.hazelcast.client.impl.protocol.codec.CPSemaphoreDrainCodec;
import com.hazelcast.client.impl.protocol.codec.CPSemaphoreInitCodec;
import com.hazelcast.client.impl.protocol.codec.CPSemaphoreReleaseCodec;
import com.hazelcast.client.spi.ClientContext;
import com.hazelcast.client.spi.ClientProxy;
import com.hazelcast.client.spi.impl.ClientInvocation;
import com.hazelcast.core.ISemaphore;
import com.hazelcast.cp.internal.RaftGroupId;
import com.hazelcast.cp.internal.datastructures.semaphore.RaftSemaphoreService;
import com.hazelcast.nio.serialization.Data;

import java.util.concurrent.TimeUnit;

import static com.hazelcast.cp.internal.session.AbstractProxySessionManager.NO_SESSION_ID;
import static com.hazelcast.util.Preconditions.checkNotNegative;
import static com.hazelcast.util.Preconditions.checkPositive;
import static com.hazelcast.util.UuidUtil.newUnsecureUUID;
import static java.lang.Math.max;

/**
 * Client-side sessionless proxy of Raft-based {@link ISemaphore}
 */
class RaftSessionlessSemaphoreProxy extends ClientProxy implements ISemaphore {

    private final ClientProxySessionManager sessionManager;
    private final RaftGroupId groupId;
    private final String objectName;

    RaftSessionlessSemaphoreProxy(ClientContext context, RaftGroupId groupId, String proxyName, String objectName) {
        super(RaftSemaphoreService.SERVICE_NAME, proxyName, context);
        this.sessionManager = getClient().getProxySessionManager();
        this.groupId = groupId;
        this.objectName = objectName;
    }

    @Override
    public boolean init(int permits) {
        checkNotNegative(permits, "Permits must be non-negative!");

        Data groupId = getSerializationService().toData(this.groupId);
        ClientMessage request = CPSemaphoreInitCodec.encodeRequest(groupId, objectName, permits);
        HazelcastClientInstanceImpl client = getClient();
        ClientMessage response = new ClientInvocation(client, request, objectName).invoke().join();
        return CPSemaphoreInitCodec.decodeResponse(response).response;
    }

    @Override
    public void acquire() {
        acquire(1);
    }

    @Override
    public void acquire(int permits) {
        checkPositive(permits, "Permits must be positive!");

        long clusterWideThreadId = sessionManager.getOrCreateUniqueThreadId(groupId);
        Data invocationUid = getSerializationService().toData(newUnsecureUUID());
        Data groupId = getSerializationService().toData(this.groupId);

        ClientMessage request = CPSemaphoreAcquireCodec.encodeRequest(groupId, objectName, NO_SESSION_ID, clusterWideThreadId,
                invocationUid, permits, -1);
        HazelcastClientInstanceImpl client = getClient();
        new ClientInvocation(client, request, objectName).invoke().join();
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

        long clusterWideThreadId = sessionManager.getOrCreateUniqueThreadId(groupId);
        Data groupId = getSerializationService().toData(this.groupId);
        Data invocationUid = getSerializationService().toData(newUnsecureUUID());
        long timeoutMs = max(0, unit.toMillis(timeout));

        ClientMessage request = CPSemaphoreAcquireCodec.encodeRequest(groupId, objectName, NO_SESSION_ID, clusterWideThreadId,
                invocationUid, permits, timeoutMs);
        HazelcastClientInstanceImpl client = getClient();
        ClientMessage response = new ClientInvocation(client, request, objectName).invoke().join();
        return CPSemaphoreAcquireCodec.decodeResponse(response).response;
    }

    @Override
    public void release() {
        release(1);
    }

    @Override
    public void release(int permits) {
        checkPositive(permits, "Permits must be positive!");

        long clusterWideThreadId = sessionManager.getOrCreateUniqueThreadId(groupId);
        Data groupId = getSerializationService().toData(this.groupId);
        Data invocationUid = getSerializationService().toData(newUnsecureUUID());

        ClientMessage request = CPSemaphoreReleaseCodec.encodeRequest(groupId, objectName, NO_SESSION_ID, clusterWideThreadId,
                invocationUid, permits);
        HazelcastClientInstanceImpl client = getClient();
        new ClientInvocation(client, request, objectName).invoke().join();
    }

    @Override
    public int availablePermits() {
        Data groupId = getSerializationService().toData(this.groupId);
        ClientMessage request = CPSemaphoreAvailablePermitsCodec.encodeRequest(groupId, objectName);
        HazelcastClientInstanceImpl client = getClient();
        ClientMessage response = new ClientInvocation(client, request, objectName).invoke().join();
        return CPSemaphoreAvailablePermitsCodec.decodeResponse(response).response;
    }

    @Override
    public int drainPermits() {
        long clusterWideThreadId = sessionManager.getOrCreateUniqueThreadId(groupId);
        Data invocationUid = getSerializationService().toData(newUnsecureUUID());
        Data groupId = getSerializationService().toData(this.groupId);

        ClientMessage request = CPSemaphoreDrainCodec.encodeRequest(groupId, objectName, NO_SESSION_ID, clusterWideThreadId,
                invocationUid);
        HazelcastClientInstanceImpl client = getClient();
        ClientMessage response = new ClientInvocation(client, request, objectName).invoke().join();
        return CPSemaphoreDrainCodec.decodeResponse(response).response;
    }

    @Override
    public void reducePermits(int reduction) {
        checkNotNegative(reduction, "Reduction must be non-negative!");
        if (reduction == 0) {
            return;
        }

        long clusterWideThreadId = sessionManager.getOrCreateUniqueThreadId(groupId);
        Data invocationUid = getSerializationService().toData(newUnsecureUUID());
        Data groupId = getSerializationService().toData(this.groupId);

        ClientMessage request = CPSemaphoreChangeCodec.encodeRequest(groupId, objectName, NO_SESSION_ID, clusterWideThreadId,
                invocationUid, -reduction);
        new ClientInvocation(getClient(), request, objectName).invoke().join();
    }

    @Override
    public void increasePermits(int increase) {
        checkNotNegative(increase, "Increase must be non-negative!");
        if (increase == 0) {
            return;
        }

        long clusterWideThreadId = sessionManager.getOrCreateUniqueThreadId(groupId);
        Data invocationUid = getSerializationService().toData(newUnsecureUUID());
        Data groupId = getSerializationService().toData(this.groupId);

        ClientMessage request = CPSemaphoreChangeCodec.encodeRequest(groupId, objectName, NO_SESSION_ID, clusterWideThreadId,
                invocationUid, increase);
        new ClientInvocation(getClient(), request, objectName).invoke().join();
    }

    @Override
    public String getPartitionKey() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void onDestroy() {
        Data groupId = getSerializationService().toData(this.groupId);
        ClientMessage request = CPGroupDestroyCPObjectCodec.encodeRequest(groupId, getServiceName(), objectName);
        new ClientInvocation(getClient(), request, name).invoke().join();
    }

    public RaftGroupId getGroupId() {
        return groupId;
    }

}
