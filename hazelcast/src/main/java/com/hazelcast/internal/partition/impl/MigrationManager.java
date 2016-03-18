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

package com.hazelcast.internal.partition.impl;

import com.hazelcast.core.MemberLeftException;
import com.hazelcast.core.MigrationEvent;
import com.hazelcast.instance.MemberImpl;
import com.hazelcast.instance.Node;
import com.hazelcast.internal.cluster.impl.ClusterServiceImpl;
import com.hazelcast.internal.metrics.Probe;
import com.hazelcast.internal.partition.InternalPartition;
import com.hazelcast.internal.partition.MigrationInfo;
import com.hazelcast.internal.partition.MigrationInfo.MigrationStatus;
import com.hazelcast.internal.partition.PartitionRuntimeState;
import com.hazelcast.internal.partition.impl.InternalMigrationListener.MigrationParticipant;
import com.hazelcast.internal.partition.operation.ClearReplicaOperation;
import com.hazelcast.internal.partition.operation.FinalizeMigrationOperation;
import com.hazelcast.internal.partition.operation.MigrationCommitOperation;
import com.hazelcast.internal.partition.operation.MigrationRequestOperation;
import com.hazelcast.internal.partition.operation.PromoteFromBackupOperation;
import com.hazelcast.internal.properties.GroupProperty;
import com.hazelcast.logging.ILogger;
import com.hazelcast.nio.Address;
import com.hazelcast.spi.exception.TargetNotMemberException;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.spi.partition.MigrationEndpoint;
import com.hazelcast.util.Clock;
import com.hazelcast.util.MutableInteger;
import com.hazelcast.util.Preconditions;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;
import java.util.logging.Level;

import static com.hazelcast.spi.partition.IPartitionService.SERVICE_NAME;

/**
 * TODO: Javadoc Pending...
 *
 */
public class MigrationManager {

    private static final int DEFAULT_PAUSE_MILLIS = 1000;

    private static final int PARTITION_STATE_VERSION_INCREMENT_DELTA_ON_MIGRATION_FAILURE = 2;

    enum MigrateTaskReason {
        REPAIR_PARTITION_TABLE,
        REPARTITIONING
    }

    private final Node node;
    private final NodeEngineImpl nodeEngine;
    private final InternalPartitionServiceImpl partitionService;
    private final ILogger logger;

    private final PartitionStateManager partitionStateManager;

    final MigrationQueue migrationQueue = new MigrationQueue();

    private final MigrationThread migrationThread;

    private final AtomicBoolean migrationAllowed = new AtomicBoolean(true);

    @Probe(name = "lastRepartitionTime")
    private final AtomicLong lastRepartitionTime = new AtomicLong();

    final long partitionMigrationInterval;
    private final long partitionMigrationTimeout;
    private final long migrationPauseDelayMs;

    // updates will be done under lock, but reads will be multithreaded.
    private volatile MigrationInfo activeMigrationInfo;

    // both reads and updates will be done under lock!
    private final LinkedHashSet<MigrationInfo> completedMigrations = new LinkedHashSet<MigrationInfo>();

    @Probe
    private final AtomicLong completedMigrationCounter = new AtomicLong();

    private volatile InternalMigrationListener internalMigrationListener
            = new InternalMigrationListener.NopInternalMigrationListener();

    // TODO: clarify lock usages.
    // One option is to remove lock from this class and caller to guarantee thread safety.
    private final Lock partitionServiceLock;

    public MigrationManager(Node node, InternalPartitionServiceImpl service, Lock partitionServiceLock) {
        this.node = node;
        this.nodeEngine = node.nodeEngine;
        this.partitionService = service;
        this.logger = node.getLogger(getClass());
        this.partitionServiceLock = partitionServiceLock;

        partitionStateManager = partitionService.getPartitionStateManager();

        migrationThread = new MigrationThread(this, node.getHazelcastThreadGroup(), node.getLogger(MigrationThread.class));

        long intervalMillis = node.groupProperties.getMillis(GroupProperty.PARTITION_MIGRATION_INTERVAL);
        partitionMigrationInterval = (intervalMillis > 0 ? intervalMillis : 0);
        partitionMigrationTimeout = node.groupProperties.getMillis(GroupProperty.PARTITION_MIGRATION_TIMEOUT);

        migrationPauseDelayMs = node.groupProperties.getMillis(GroupProperty.MIGRATION_MIN_DELAY_ON_MEMBER_REMOVED_SECONDS);
    }

    @Probe(name = "migrationActive")
    private int migrationActiveProbe() {
        return migrationAllowed.get() ? 1 : 0;
    }

    public void pauseMigration() {
        migrationAllowed.set(false);
    }

    public void resumeMigration() {
        migrationAllowed.set(true);
    }

    void resumeMigrationEventually() {
        // TODO: RESUME EVENTUALLY!
//        ExecutionService executionService = nodeEngine.getExecutionService();
//        executionService.schedule(new Runnable() {
//            @Override
//            public void run() {
//                resumeMigration();
//            }
//        }, migrationPauseDelayMs, TimeUnit.MILLISECONDS);
        resumeMigration();
    }

    boolean isMigrationAllowed() {
        return migrationAllowed.get();
    }

    private void finalizeMigration(MigrationInfo migrationInfo) {
        try {
            Address thisAddress = node.getThisAddress();
            int partitionId = migrationInfo.getPartitionId();

            boolean source = thisAddress.equals(migrationInfo.getSource());
            boolean destination = thisAddress.equals(migrationInfo.getDestination());

            assert migrationInfo.getStatus() == MigrationStatus.SUCCESS
                    || migrationInfo.getStatus() == MigrationStatus.FAILED : "Invalid migration: " + migrationInfo;

            if (source || destination) {
                boolean success = migrationInfo.getStatus() == MigrationStatus.SUCCESS;

                MigrationParticipant participant = source ? MigrationParticipant.SOURCE : MigrationParticipant.DESTINATION;
                if (success) {
                    internalMigrationListener.onMigrationCommit(participant, migrationInfo);
                } else {
                    internalMigrationListener.onMigrationRollback(participant, migrationInfo);
                }

                MigrationEndpoint endpoint = source ? MigrationEndpoint.SOURCE : MigrationEndpoint.DESTINATION;
                FinalizeMigrationOperation op = new FinalizeMigrationOperation(migrationInfo, endpoint, success);

                op.setPartitionId(partitionId).setNodeEngine(nodeEngine).setValidateTarget(false)
                        .setService(partitionService);
                nodeEngine.getOperationService().executeOperation(op);
                removeActiveMigration(partitionId);
            } else {
                final Address partitionOwner = partitionStateManager.getPartitionImpl(partitionId).getOwnerOrNull();
                if (node.getThisAddress().equals(partitionOwner)) {
                    removeActiveMigration(partitionId);
                } else {
                    // TODO: remove "new IllegalStateException()"
                    logger.severe("Failed to finalize migration because this member " + thisAddress
                            + " is not a participant of the migration: " + migrationInfo,
                            new IllegalStateException(activeMigrationInfo + "\n"
                                    + partitionStateManager.getPartitionImpl(partitionId).toString()));
                }
            }
        } catch (Exception e) {
            logger.warning(e);
        } finally {
            migrationInfo.doneProcessing();
        }
    }

    public boolean addActiveMigration(MigrationInfo migrationInfo) {
        partitionServiceLock.lock();
        try {
            if (activeMigrationInfo == null) {
                partitionStateManager.setMigrating(migrationInfo.getPartitionId(), true);
                activeMigrationInfo = migrationInfo;
                return true;
            }

            logger.warning(migrationInfo + " not added! Already existing active migration: " + activeMigrationInfo);
            return false;
        } finally {
            partitionServiceLock.unlock();
        }
    }

    MigrationInfo getActiveMigration() {
        return activeMigrationInfo;
    }

    public boolean removeActiveMigration(int partitionId) {
        partitionServiceLock.lock();
        try {
            if (activeMigrationInfo != null) {
                if (activeMigrationInfo.getPartitionId() == partitionId) {
                    partitionStateManager.setMigrating(partitionId, false);
                    activeMigrationInfo = null;
                    return true;
                }

                if (logger.isFinestEnabled()) {
                    logger.finest("Active migration is not removed, because it has different partitionId! "
                            + "PartitionId=" + partitionId + ", Active migration=" + activeMigrationInfo);
                }
            }
        } finally {
            partitionServiceLock.unlock();
        }
        return false;
    }

    void scheduleActiveMigrationFinalization(final MigrationInfo migrationInfo) {
        partitionServiceLock.lock();
        try {
            final MigrationInfo activeMigrationInfo = this.activeMigrationInfo;
            if (activeMigrationInfo != null && migrationInfo.equals(activeMigrationInfo)) {
                if (activeMigrationInfo.startProcessing()) {
                    activeMigrationInfo.setStatus(migrationInfo.getStatus());
                    finalizeMigration(activeMigrationInfo);
                } else {
                    logger.info("Scheduling finalization of " + migrationInfo
                            + ", because migration process is currently running.");
                    nodeEngine.getExecutionService().schedule(new Runnable() {
                        @Override
                        public void run() {
                            scheduleActiveMigrationFinalization(migrationInfo);
                        }
                    }, 3, TimeUnit.SECONDS);
                }
            } else if (migrationInfo.getSourceCurrentReplicaIndex() > 0
                    && node.getThisAddress().equals(migrationInfo.getSource())) {
                // OLD BACKUP
                finalizeMigration(migrationInfo);
            } else if (migrationInfo.getSource() == null
                    && node.getThisAddress().equals(migrationInfo.getDestination())
                    && migrationInfo.getDestinationCurrentReplicaIndex() > 0
                    && migrationInfo.getDestinationNewReplicaIndex() == 0) {
                // shift up when a partition owner is removed
                assert migrationInfo.getStatus() == MigrationStatus.SUCCESS
                        : "Promotion status should be SUCCESS! -> " + migrationInfo.getStatus();
                // TODO BASRI source is null here. we need to move partition lost event dispatching logic to PartitionStateManger
                PromoteFromBackupOperation op = new PromoteFromBackupOperation(migrationInfo.getSource(), migrationInfo.getDestinationCurrentReplicaIndex());
                op.setPartitionId(migrationInfo.getPartitionId()).setNodeEngine(nodeEngine).setService(partitionService);
                nodeEngine.getOperationService().executeOperation(op);

            } else if (migrationInfo.getSourceNewReplicaIndex() > 0) {
                if (migrationInfo.getStatus() == MigrationStatus.SUCCESS
                        && node.getThisAddress().equals(migrationInfo.getOldReplicaOwner())) {
                    // clear
                    ClearReplicaOperation op = new ClearReplicaOperation(migrationInfo.getSourceNewReplicaIndex());
                    op.setPartitionId(migrationInfo.getPartitionId()).setNodeEngine(nodeEngine).setService(partitionService);
                    nodeEngine.getOperationService().executeOperation(op);
                }
            }
        } finally {
            partitionServiceLock.unlock();
        }
    }

    private boolean commitMigrationToDestination(MigrationInfo migrationInfo) {
        if (!node.isMaster()) {
            return false;
        }

        if (node.getThisAddress().equals(migrationInfo.getDestination())) {
            return true;
        }

        try {
            PartitionRuntimeState partitionState = partitionService.createMigrationCommitPartitionState(migrationInfo);
            MigrationCommitOperation operation = new MigrationCommitOperation(partitionState);
            Future<Boolean> future = nodeEngine.getOperationService()
                    .createInvocationBuilder(SERVICE_NAME, operation,
                            migrationInfo.getDestination())
                    .setTryCount(Integer.MAX_VALUE)
                    .setCallTimeout(Long.MAX_VALUE).invoke();
            future.get();
            return true;
        } catch (Throwable t) {
            if (t instanceof MemberLeftException || t instanceof TargetNotMemberException) {
                logger.warning("Migration commit failed for " + migrationInfo + " since destination left the cluster");
            } else {
                logger.severe("Migration commit failed for " + migrationInfo, t);
            }

            return false;
        }
    }

    boolean addCompletedMigration(MigrationInfo migrationInfo) {
        if (migrationInfo.getStatus() != MigrationStatus.SUCCESS
                && migrationInfo.getStatus() != MigrationStatus.FAILED) {
            throw new IllegalArgumentException("Migration doesn't seem completed: " + migrationInfo);
        }

        partitionServiceLock.lock();
        try {
            boolean added = completedMigrations.add(migrationInfo);
            if (added) {
                completedMigrationCounter.incrementAndGet();
            }
            return added;
        } finally {
            partitionServiceLock.unlock();
        }
    }

    void retainCompletedMigrations(Collection<MigrationInfo> migrations) {
        partitionServiceLock.lock();
        try {
            completedMigrations.retainAll(migrations);
        } finally {
            partitionServiceLock.unlock();
        }
    }

    private void evictCompletedMigrations(MigrationInfo currentMigration) {
        partitionServiceLock.lock();
        try {
            assert completedMigrations.contains(currentMigration);

            Iterator<MigrationInfo> iter = completedMigrations.iterator();
            while (iter.hasNext()) {
                MigrationInfo migration = iter.next();
                iter.remove();

                // evict completed migrations including current migration
                if (migration.equals(currentMigration)) {
                    return;
                }
            }

        } finally {
            partitionServiceLock.unlock();
        }
    }

    void triggerControlTask() {
        migrationQueue.clear();
        migrationQueue.add(new ControlTask());
    }

    public InternalMigrationListener getInternalMigrationListener() {
        return internalMigrationListener;
    }

    void setInternalMigrationListener(InternalMigrationListener listener) {
        Preconditions.checkNotNull(listener);
        internalMigrationListener = listener;
    }

    void resetInternalMigrationListener() {
        internalMigrationListener = new InternalMigrationListener.NopInternalMigrationListener();
    }

    void onMemberRemove(MemberImpl member) {
        Address deadAddress = member.getAddress();
        final MigrationInfo activeMigration = activeMigrationInfo;
        if (activeMigration != null) {
            if (deadAddress.equals(activeMigration.getSource())
                    || deadAddress.equals(activeMigration.getDestination())) {
                activeMigration.setStatus(MigrationStatus.INVALID);
            }
        }
    }

    public void schedule(MigrationRunnable runnable) {
        migrationQueue.add(runnable);
    }

    public List<MigrationInfo> getCompletedMigrations() {
        partitionServiceLock.lock();
        try {
            return new ArrayList<MigrationInfo>(completedMigrations);
        } finally {
            partitionServiceLock.unlock();
        }
    }

    public boolean hasOnGoingMigration() {
        return activeMigrationInfo != null || migrationQueue.isNonEmpty()
                || migrationQueue.hasMigrationTasks();
    }

    public int getMigrationQueueSize() {
        return migrationQueue.size();
    }

    public void reset() {
        migrationQueue.clear();
        activeMigrationInfo = null;
        completedMigrations.clear();
    }

    void start() {
        migrationThread.start();
    }

    void stop() {
        migrationThread.stopNow();
    }

    void scheduleMigration(MigrationInfo migrationInfo, MigrateTaskReason reason) {
        migrationQueue.add(new MigrateTask(migrationInfo, reason));
    }

    void repairPartition(int partitionId) {
        InternalPartitionImpl partition = partitionStateManager.getPartitionImpl(partitionId);

        if (partition.getOwnerOrNull() == null) {
            Address destination = null;
            int index = 1;
            for (int i = index; i < InternalPartition.MAX_REPLICA_COUNT; i++) {
                destination = partition.getReplicaAddress(i);
                if (destination != null) {
                    partition.swapAddresses(0, i);
                    index = i;
                    break;
                }
            }

            if (logger.isFinestEnabled()) {
                if (destination != null) {
                    logger.finest("partitionId=" + partition.getPartitionId() + " owner is removed. replicaIndex=" + index
                            + " is shifted up to 0. " + partition);
                } else {
                    logger.finest("partitionId=" + partition.getPartitionId() + " owner is removed. there is no other replica to shift up. " + partition);
                }
            }

            if (destination != null) {
                // TODO: how do we inform the services about promotion?
                // run promotion when you get this completed migration?
                MigrationInfo migration =
                        new MigrationInfo(partition.getPartitionId(), null, destination, -1, -1, index, 0);
                migration.setMaster(node.getThisAddress());
                migration.setStatus(MigrationInfo.MigrationStatus.SUCCESS);
                partitionService.getMigrationManager().addCompletedMigration(migration);
                partitionService.getMigrationManager().scheduleActiveMigrationFinalization(migration);
            }
        }

        if (partition.getOwnerOrNull() == null) {
            // we lost the partition!
            // TODO BASRI partition lost
            return;
        }

        int lastIndex = InternalPartition.MAX_REPLICA_COUNT - 1;
        for (int index = 1; index < InternalPartition.MAX_REPLICA_COUNT; index++) {
            if (partition.getReplicaAddress(index) != null) {
                continue;
            }

            // search for a destination to assign empty index
            Address destination;
            for (int i = lastIndex; i > index; i--) {
                destination = partition.getReplicaAddress(i);
                if (destination != null) {
                    MigrationInfo migration = new MigrationInfo(partition.getPartitionId(), null, destination,
                            -1, -1, i, index);
                    scheduleMigration(migration, MigrateTaskReason.REPAIR_PARTITION_TABLE);
                    lastIndex = i - 1;
                    break;
                }
            }
        }
    }

    private class RepartitioningTask implements MigrationRunnable {

        @Override
        public void run() {
            if (!node.isMaster()) {
                return;
            }

            partitionServiceLock.lock();
            try {
                if (!isAllowed()) {
                    return;
                }

                Address[][] newState = partitionStateManager.repartition();
                if (newState == null) {
                    return;
                }

                if (!isAllowed()) {
                    return;
                }

                lastRepartitionTime.set(Clock.currentTimeMillis());

                processNewPartitionState(newState);
                partitionService.syncPartitionRuntimeState();
            } finally {
                partitionServiceLock.unlock();
            }
        }

        private void processNewPartitionState(Address[][] newState) {
            final MutableInteger lostCount = new MutableInteger();
            final MutableInteger migrationCount = new MutableInteger();

            for (int partitionId = 0; partitionId < newState.length; partitionId++) {
                InternalPartitionImpl currentPartition = partitionStateManager.getPartitionImpl(partitionId);
                Address[] newReplicas = newState[partitionId];

                MigrationDecision.migrate(currentPartition.getReplicaAddresses(), newReplicas,
                        new MigrationTaskScheduler(currentPartition, migrationCount, lostCount));
            }

            logMigrationStatistics(migrationCount.value, lostCount.value);
        }

        private void logMigrationStatistics(int migrationCount, int lostCount) {
            if (lostCount > 0) {
                logger.warning("Assigning new owners for " + lostCount + " LOST partitions!");
            }

            if (migrationCount > 0) {
                logger.info("Re-partitioning cluster data... Migration queue size: " + migrationCount);
            } else {
                logger.info("Partition balance is ok, no need to re-partition cluster data... ");
            }
        }

        private void assignNewPartitionOwner(int partitionId, InternalPartitionImpl currentPartition, Address newOwner) {
            MigrationInfo migrationInfo = new MigrationInfo(partitionId, null, newOwner, -1, -1, -1, 0);
            PartitionEventManager partitionEventManager = partitionService.getPartitionEventManager();
            partitionEventManager.sendMigrationEvent(migrationInfo, MigrationEvent.MigrationStatus.STARTED);
            currentPartition.setReplicaAddress(0, newOwner);
            partitionEventManager.sendMigrationEvent(migrationInfo, MigrationEvent.MigrationStatus.COMPLETED);
        }

        private boolean isAllowed() {
            boolean migrationAllowed = isMigrationAllowed();
            boolean hasMigrationTasks = migrationQueue.hasMigrationTasks();
            if (migrationAllowed && !hasMigrationTasks) {
                return true;
            }
            logger.severe("Scheduling ControlTask");
            triggerControlTask();
            return false;
        }

        private class MigrationTaskScheduler implements MigrationDecision.MigrationCallback {
            private final int partitionId;
            private final InternalPartitionImpl partition;
            private final MutableInteger migrationCount;
            private final MutableInteger lostCount;

            public MigrationTaskScheduler(InternalPartitionImpl partition, MutableInteger migrationCount,
                    MutableInteger lostCount) {
                partitionId = partition.getPartitionId();
                this.partition = partition;
                this.migrationCount = migrationCount;
                this.lostCount = lostCount;
            }

            @Override
            public void migrate(Address source, int sourceCurrentReplicaIndex, int sourceNewReplicaIndex,
                    Address destination, int destinationCurrentReplicaIndex, int destinationNewReplicaIndex) {

                if (source == null && destinationNewReplicaIndex == 0) {
                    assert destination != null;
                    assert sourceCurrentReplicaIndex == -1 : "invalid index: " + sourceCurrentReplicaIndex;;
                    assert sourceNewReplicaIndex == -1 : "invalid index: " + sourceNewReplicaIndex;;

                    if (destinationCurrentReplicaIndex == -1) {
                        lostCount.value++;
                        assignNewPartitionOwner(partitionId, partition, destination);
                    } else {
                        // shift-up
                        schedule(null, -1, -1, destination, destinationCurrentReplicaIndex, 0);
                    }
                } else if (destination == null && sourceNewReplicaIndex == -1) {
                    assert source != null;
                    assert sourceCurrentReplicaIndex != -1;
                    assert source.equals(partition.getReplicaAddress(sourceCurrentReplicaIndex));

                    partition.setReplicaAddress(sourceCurrentReplicaIndex, null);
                } else {
                    schedule(source, sourceCurrentReplicaIndex, sourceNewReplicaIndex, destination,
                            destinationCurrentReplicaIndex, destinationNewReplicaIndex);
                }
            }

            private void schedule(Address source, int sourceCurrentReplicaIndex, int sourceNewReplicaIndex,
                    Address destination, int destinationCurrentReplicaIndex, int destinationNewReplicaIndex) {

                migrationCount.value++;
                MigrationInfo migration = new MigrationInfo(partitionId, source, destination, sourceCurrentReplicaIndex,
                        sourceNewReplicaIndex, destinationCurrentReplicaIndex, destinationNewReplicaIndex);
                if (sourceNewReplicaIndex > 0) {
                    migration.setOldReplicaOwner(partition.getReplicaAddress(sourceNewReplicaIndex));
                }
                scheduleMigration(migration, MigrateTaskReason.REPARTITIONING);
            }
        }
    }

    class MigrateTask implements MigrationRunnable {

        final MigrationInfo migrationInfo;

        final MigrateTaskReason reason;

        public MigrateTask(MigrationInfo migrationInfo, MigrateTaskReason reason) {
            this.migrationInfo = migrationInfo;
            migrationInfo.setMaster(node.getThisAddress());
            this.reason = reason;
        }

        @Override
        public void run() {
            if (!node.isMaster()) {
                return;
            }

            try {
                MemberImpl partitionOwner = getPartitionOwner();
                if (partitionOwner == null) {
                    logger.fine("Partition owner is null. Ignoring migration task.");
//                    triggerRepartitioningAfterMigrationFailure();
                    return;
                }

                if (migrationInfo.getSource() != null) {
                    if (node.getClusterService().getMember(migrationInfo.getSource()) == null) {
                        logger.fine("Source is not member anymore. Ignoring migration task.");
                        triggerRepartitioningAfterMigrationFailure();
                        return;
                    }
                }

                if (node.getClusterService().getMember(migrationInfo.getDestination()) == null) {
                    logger.fine("Destination is not member anymore. Ignoring migration task.");
                    triggerRepartitioningAfterMigrationFailure();
                    return;
                }

                internalMigrationListener.onMigrationStart(MigrationParticipant.MASTER, migrationInfo);
                partitionService.getPartitionEventManager()
                        .sendMigrationEvent(migrationInfo, MigrationEvent.MigrationStatus.STARTED);

                if (logger.isFineEnabled()) {
                    logger.fine("Starting Migration: " + migrationInfo);
                }

                long start = Clock.currentTimeMillis();
                Boolean result = executeMigrateOperation(partitionOwner);
                long end = Clock.currentTimeMillis();

                if (end - start > 1000) {
                    logger.severe("================ Result: " + result + ", Owner: " + partitionOwner.getAddress()
                            + ", owner-member?: " + (node.getClusterService().getMember(partitionOwner.getAddress()) != null)
                            + ", destination-member?: " + (node.getClusterService().getMember(migrationInfo.getDestination()) != null)
                            + ", elapsed: " + (end - start) + "ms. -> " + migrationInfo);
                }
                processMigrationResult(result);
            } catch (Throwable t) {
                final Level level = migrationInfo.isValid() ? Level.WARNING : Level.FINEST;
                logger.log(level, "Error [" + t.getClass() + ": " + t.getMessage() + "] during " + migrationInfo);
                logger.finest(t);
                migrationOperationFailed();
            }
        }

        private MemberImpl getPartitionOwner() {
            InternalPartitionImpl partition = partitionStateManager.getPartitionImpl(migrationInfo.getPartitionId());
            Address owner = partition.getOwnerOrNull();
            if (owner == null) {
                if (migrationInfo.isValid()) {
                    logger.severe("Partition owner is not set! -> partitionId=" + migrationInfo.getPartitionId()
                            + " , " + partition + " -VS- " + migrationInfo);
                }
                return null;
            }

            return node.getClusterService().getMember(owner);
        }

        private void processMigrationResult(Boolean result) {
            if (!migrationInfo.isValid()) {
                // TODO: is this valid check needed?
                assert !result : "Unexpected result SUCCESS" ;
                migrationOperationFailed();
            } else if (Boolean.TRUE.equals(result)) {
//                if (logger.isFineEnabled()) {
                    logger.info("Finished Migration: " + migrationInfo);
//                }
                migrationOperationSucceeded();
            } else {
                final Level level = migrationInfo.isValid() ? Level.WARNING : Level.FINE;
                logger.log(level, "Migration failed: " + migrationInfo);
                migrationOperationFailed();
            }
        }

        private Boolean executeMigrateOperation(MemberImpl fromMember) {
            MigrationRequestOperation migrationRequestOp = new MigrationRequestOperation(migrationInfo,
                    partitionService.getPartitionStateVersion());

            Future future = nodeEngine.getOperationService().createInvocationBuilder(SERVICE_NAME, migrationRequestOp,
                    fromMember.getAddress())
                    .setCallTimeout(partitionMigrationTimeout)
                    .setTryPauseMillis(DEFAULT_PAUSE_MILLIS).invoke();

            try {
                Object response = future.get();
                return (Boolean) nodeEngine.toObject(response);
            } catch (Throwable e) {
                Level level = nodeEngine.isRunning() && migrationInfo.isValid() ? Level.WARNING : Level.FINEST;
                logger.log(level, "Failed migration from " + fromMember + " for " + migrationRequestOp.getMigrationInfo(), e);
            }
            return Boolean.FALSE;
        }

        private void migrationOperationFailed() {
            migrationInfo.setStatus(MigrationStatus.FAILED);
            internalMigrationListener.onMigrationComplete(MigrationParticipant.MASTER, migrationInfo, false);
            partitionServiceLock.lock();
            try {
                addCompletedMigration(migrationInfo);
                internalMigrationListener.onMigrationRollback(MigrationParticipant.MASTER, migrationInfo);
                scheduleActiveMigrationFinalization(migrationInfo);
                int delta = PARTITION_STATE_VERSION_INCREMENT_DELTA_ON_MIGRATION_FAILURE;
                partitionService.getPartitionStateManager().incrementVersion(delta);
                if (partitionService.syncPartitionRuntimeState()) {
                    evictCompletedMigrations(migrationInfo);
                }
                triggerRepartitioningAfterMigrationFailure();
            } finally {
                partitionServiceLock.unlock();
            }

            partitionService.getPartitionEventManager().sendMigrationEvent(migrationInfo, MigrationEvent.MigrationStatus.FAILED);

        }

        private void triggerRepartitioningAfterMigrationFailure() {
            // Migration failed.
            // Pause migration process for a small amount of time, if a migration attempt is failed.
            // Otherwise, migration failures can do a busy spin until migration problem is resolved.
            // Migration can fail either a node's just joined and not completed start yet or it's just left the cluster.

            // Re-execute RepartitioningTask when all other migration tasks are done,
            // an imbalance may occur because of this failure.

            partitionServiceLock.lock();
            try {
                pauseMigration();
                triggerControlTask();
                resumeMigrationEventually();
            } finally {
                partitionServiceLock.unlock();
            }
        }

        private void migrationOperationSucceeded() {
            migrationInfo.setStatus(MigrationStatus.SUCCESS);
            internalMigrationListener.onMigrationComplete(MigrationParticipant.MASTER, migrationInfo, true);
            addCompletedMigration(migrationInfo);

            boolean commitSuccessful = commitMigrationToDestination(migrationInfo);

            partitionServiceLock.lock();
            try {
                if (commitSuccessful) {
                    internalMigrationListener.onMigrationCommit(MigrationParticipant.MASTER, migrationInfo);

                    // updates partition table after successful commit
                    InternalPartitionImpl partition = partitionStateManager.getPartitionImpl(migrationInfo.getPartitionId());

                    // TODO: set replica addresses in a batch, instead of setting individually
                    if (migrationInfo.getSourceCurrentReplicaIndex() > -1) {
                        partition.setReplicaAddress(migrationInfo.getSourceCurrentReplicaIndex(), null);
                    }

                    if (migrationInfo.getDestinationCurrentReplicaIndex() > -1) {
                        partition.setReplicaAddress(migrationInfo.getDestinationCurrentReplicaIndex(), null);
                    }

                    partition.setReplicaAddress(migrationInfo.getDestinationNewReplicaIndex(), migrationInfo.getDestination());

                    if (migrationInfo.getSourceNewReplicaIndex() > -1) {
                        partition.setReplicaAddress(migrationInfo.getSourceNewReplicaIndex(), migrationInfo.getSource());
                    }

                } else {
                    migrationInfo.setStatus(MigrationStatus.FAILED);
                    internalMigrationListener.onMigrationRollback(MigrationParticipant.MASTER, migrationInfo);
                    triggerRepartitioningAfterMigrationFailure();
                }

                scheduleActiveMigrationFinalization(migrationInfo);
                if (partitionService.syncPartitionRuntimeState()) {
                    evictCompletedMigrations(migrationInfo);
                }
            } finally {
                partitionServiceLock.unlock();
            }
            partitionService.getPartitionEventManager().sendMigrationEvent(migrationInfo, MigrationEvent.MigrationStatus.COMPLETED);
        }

        @Override
        public String toString() {
            return getClass().getSimpleName() + "{" + "migrationInfo=" + migrationInfo + '}';
        }

    }

    private class RepairPartitionTableTask implements MigrationRunnable {

        @Override
        public void run() {
            if (!partitionStateManager.isInitialized()) {
                return;
            }

            partitionServiceLock.lock();
            try {
                Collection<Address> addresses = new HashSet<Address>();
                InternalPartition[] partitions = partitionStateManager.getPartitions();
                ClusterServiceImpl clusterService = node.getClusterService();

                for (InternalPartition partition : partitions) {
                    for (int i = 0; i < InternalPartition.MAX_REPLICA_COUNT; i++) {
                        Address address = partition.getReplicaAddress(i);
                        if (address != null && clusterService.getMember(address) == null) {
                            addresses.add(address);
                        }
                    }
                }

                Collection<Integer> partitionIdSet = new HashSet<Integer>();
                for (Address address : addresses) {
                    partitionStateManager.removeDeadAddress(partitionIdSet, address);
                }

                if (!partitionIdSet.isEmpty()) {
                    for (Integer partitionId : partitionIdSet) {
                        repairPartition(partitionId);
                    }
                }

                partitionService.syncPartitionRuntimeState();

                logger.severe("Scheduling RepartitioningTask");
                migrationQueue.add(new RepartitioningTask());

            } finally {
                partitionServiceLock.unlock();
            }
        }

    }

    private class ControlTask implements MigrationRunnable {

        @Override
        public void run() {
            partitionServiceLock.lock();
            try {
                migrationQueue.clear();

                if (partitionService.scheduleFetchFetchMostRecentPartitionTableTaskIfRequired()) {
                    logger.severe("FetchTask scheduled");
                    migrationQueue.add(new ControlTask());
                    return;
                }

                logger.severe("RepairTask scheduled");
                migrationQueue.add(new RepairPartitionTableTask());
            } finally {
                partitionServiceLock.unlock();
            }
        }
    }

}
