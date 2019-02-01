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

package com.hazelcast.cp.internal.datastructures.atomicref.client;

import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.impl.protocol.codec.CPAtomicRefSetCodec;
import com.hazelcast.client.impl.protocol.task.AbstractMessageTask;
import com.hazelcast.core.ExecutionCallback;
import com.hazelcast.cp.CPGroupId;
import com.hazelcast.cp.internal.RaftService;
import com.hazelcast.cp.internal.datastructures.atomicref.RaftAtomicRefService;
import com.hazelcast.cp.internal.datastructures.atomicref.operation.SetOp;
import com.hazelcast.instance.Node;
import com.hazelcast.nio.Connection;

import java.security.Permission;

/**
 * Client message task for {@link SetOp}
 */
public class SetMessageTask extends AbstractMessageTask<CPAtomicRefSetCodec.RequestParameters>
        implements ExecutionCallback<Object> {

    public SetMessageTask(ClientMessage clientMessage, Node node, Connection connection) {
        super(clientMessage, node, connection);
    }

    @Override
    protected void processMessage() {
        CPGroupId groupId = nodeEngine.toObject(parameters.groupId);
        RaftService service = nodeEngine.getService(RaftService.SERVICE_NAME);
        service.getInvocationManager()
               .invoke(groupId, new SetOp(parameters.name, parameters.newValue, parameters.returnOldValue))
               .andThen(this);
    }

    @Override
    protected CPAtomicRefSetCodec.RequestParameters decodeClientMessage(ClientMessage clientMessage) {
        return CPAtomicRefSetCodec.decodeRequest(clientMessage);
    }

    @Override
    protected ClientMessage encodeResponse(Object response) {
        return CPAtomicRefSetCodec.encodeResponse(serializationService.toData(response));
    }

    @Override
    public String getServiceName() {
        return RaftAtomicRefService.SERVICE_NAME;
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
        return "set";
    }

    @Override
    public Object[] getParameters() {
        return new Object[0];
    }

    @Override
    public void onResponse(Object response) {
        sendResponse(response);
    }

    @Override
    public void onFailure(Throwable t) {
        handleProcessingFailure(t);
    }
}
