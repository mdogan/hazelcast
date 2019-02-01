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

package com.hazelcast.cp.internal.datastructures.semaphore.client;

import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.impl.protocol.codec.CPSemaphoreAcquireCodec;
import com.hazelcast.client.impl.protocol.task.AbstractMessageTask;
import com.hazelcast.core.ExecutionCallback;
import com.hazelcast.cp.CPGroupId;
import com.hazelcast.cp.internal.RaftOp;
import com.hazelcast.cp.internal.RaftService;
import com.hazelcast.cp.internal.datastructures.semaphore.RaftSemaphoreService;
import com.hazelcast.cp.internal.datastructures.semaphore.operation.AcquirePermitsOp;
import com.hazelcast.instance.Node;
import com.hazelcast.nio.Connection;

import java.security.Permission;
import java.util.UUID;

/**
 * Client message task for {@link AcquirePermitsOp}
 */
public class AcquirePermitsMessageTask extends AbstractMessageTask<CPSemaphoreAcquireCodec.RequestParameters>
        implements ExecutionCallback<Boolean> {

    public AcquirePermitsMessageTask(ClientMessage clientMessage, Node node, Connection connection) {
        super(clientMessage, node, connection);
    }

    @Override
    protected void processMessage() {
        CPGroupId groupId = nodeEngine.toObject(parameters.groupId);
        UUID invocationUid = nodeEngine.toObject(parameters.invocationUid);
        RaftService service = nodeEngine.getService(RaftService.SERVICE_NAME);
        RaftOp op = new AcquirePermitsOp(parameters.name, parameters.sessionId, parameters.threadId, invocationUid,
                parameters.permits, parameters.timeoutMs);
        service.getInvocationManager().<Boolean>invoke(groupId, op).andThen(this);
    }

    @Override
    protected CPSemaphoreAcquireCodec.RequestParameters decodeClientMessage(ClientMessage clientMessage) {
        return CPSemaphoreAcquireCodec.decodeRequest(clientMessage);
    }

    @Override
    protected ClientMessage encodeResponse(Object response) {
        return CPSemaphoreAcquireCodec.encodeResponse((Boolean) response);
    }

    @Override
    public String getServiceName() {
        return RaftSemaphoreService.SERVICE_NAME;
    }

    @Override
    public Permission getRequiredPermission() {
        return null;
    }

    @Override
    public String getDistributedObjectName() {
        return parameters.name;
    }

    @Override
    public String getMethodName() {
        return "acquire";
    }

    @Override
    public Object[] getParameters() {
        return new Object[0];
    }

    @Override
    public void onResponse(Boolean response) {
        sendResponse(response);
    }

    @Override
    public void onFailure(Throwable t) {
        handleProcessingFailure(t);
    }
}
