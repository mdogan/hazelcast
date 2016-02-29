/*
 * Copyright (c) 2008-2016, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.internal.partition.operation;

import com.hazelcast.internal.partition.MigrationCycleOperation;
import com.hazelcast.internal.partition.MigrationInfo;
import com.hazelcast.internal.partition.impl.InternalPartitionServiceImpl;
import com.hazelcast.internal.partition.impl.MigrationManager;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.partition.MigrationEndpoint;
import com.hazelcast.spi.AbstractOperation;
import com.hazelcast.spi.MigrationAwareService;
import com.hazelcast.spi.PartitionAwareOperation;
import com.hazelcast.spi.PartitionMigrationEvent;
import com.hazelcast.spi.impl.NodeEngineImpl;

import java.io.IOException;

// runs locally...
public final class FinalizeMigrationOperation extends AbstractOperation
        implements PartitionAwareOperation, MigrationCycleOperation {

    private final MigrationInfo migrationInfo;
    private final MigrationEndpoint endpoint;
    private final boolean success;

    public FinalizeMigrationOperation(MigrationInfo migrationInfo, MigrationEndpoint endpoint, boolean success) {
        this.migrationInfo = migrationInfo;
        this.endpoint = endpoint;
        this.success = success;
    }

    @Override
    public void run() {
        InternalPartitionServiceImpl partitionService = getService();

        int partitionId = getPartitionId();
        MigrationManager migrationManager = partitionService.getMigrationManager();
//        MigrationInfo migrationInfo = migrationManager.getActiveMigration(partitionId);
//        if (migrationInfo == null) {
//            return;
//        }

        NodeEngineImpl nodeEngine = (NodeEngineImpl) getNodeEngine();

        PartitionMigrationEvent event = new PartitionMigrationEvent(endpoint, migrationInfo.getType(), partitionId,
                migrationInfo.getReplicaIndex(), migrationInfo.getCopyBackReplicaIndex());
        for (MigrationAwareService service : nodeEngine.getServices(MigrationAwareService.class)) {
            finishMigration(event, service);
        }

        if (endpoint == MigrationEndpoint.SOURCE && success) {
            partitionService.clearPartitionReplicaVersions(partitionId);
        } else if (endpoint == MigrationEndpoint.DESTINATION && !success) {
            partitionService.clearPartitionReplicaVersions(partitionId);
        }

//        migrationManager.removeActiveMigration(partitionId);
        if (success) {
            nodeEngine.onPartitionMigrate(migrationInfo);
        }
    }

    private void finishMigration(PartitionMigrationEvent event, MigrationAwareService service) {
        try {
            if (success) {
                service.commitMigration(event);
            } else {
                service.rollbackMigration(event);
            }
        } catch (Throwable e) {
            getLogger().warning("Error while finalizing migration -> " + event, e);
        }
    }

    @Override
    public boolean returnsResponse() {
        return false;
    }

    @Override
    public boolean validatesTarget() {
        return false;
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        throw new UnsupportedOperationException();
    }
}
