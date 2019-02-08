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

package com.hazelcast.cp.internal;

import com.hazelcast.config.cp.CPSubsystemConfig;
import com.hazelcast.core.Member;
import com.hazelcast.cp.CPGroup.CPGroupStatus;
import com.hazelcast.cp.CPGroupId;
import com.hazelcast.cp.exception.CPGroupDestroyedException;
import com.hazelcast.cp.internal.exception.CannotCreateRaftGroupException;
import com.hazelcast.cp.internal.exception.CannotRemoveCPMemberException;
import com.hazelcast.cp.internal.exception.MetadataRaftGroupNotInitializedException;
import com.hazelcast.cp.internal.raft.SnapshotAwareService;
import com.hazelcast.cp.internal.raft.impl.RaftNode;
import com.hazelcast.cp.internal.raft.impl.RaftNodeImpl;
import com.hazelcast.cp.internal.raftop.metadata.CreateRaftNodeOp;
import com.hazelcast.cp.internal.raftop.metadata.DestroyRaftNodesOp;
import com.hazelcast.cp.internal.raftop.metadata.InitializeMetadataRaftGroupOp;
import com.hazelcast.cp.internal.raftop.metadata.PublishActiveCPMembersOp;
import com.hazelcast.cp.internal.util.Tuple2;
import com.hazelcast.logging.ILogger;
import com.hazelcast.spi.ExecutionService;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.OperationService;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.spi.impl.operationservice.impl.RaftInvocationContext;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static com.hazelcast.cp.CPGroup.CPGroupStatus.ACTIVE;
import static com.hazelcast.cp.CPGroup.CPGroupStatus.DESTROYED;
import static com.hazelcast.cp.CPGroup.CPGroupStatus.DESTROYING;
import static com.hazelcast.cp.CPGroup.METADATA_CP_GROUP_NAME;
import static com.hazelcast.cp.internal.MembershipChangeContext.CPGroupMembershipChangeContext;
import static com.hazelcast.util.Preconditions.checkFalse;
import static com.hazelcast.util.Preconditions.checkNotNull;
import static com.hazelcast.util.Preconditions.checkState;
import static com.hazelcast.util.Preconditions.checkTrue;
import static java.util.Collections.singletonList;
import static java.util.Collections.sort;
import static java.util.Collections.unmodifiableCollection;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;

/**
 * Maintains the CP subsystem metadata, such as CP groups, active CP members,
 * leaving and joining CP members, etc.
 */
@SuppressWarnings({"checkstyle:methodcount", "checkstyle:classdataabstractioncoupling"})
public class MetadataRaftGroupManager implements SnapshotAwareService<MetadataRaftGroupSnapshot>  {

    public static final RaftGroupId INITIAL_METADATA_GROUP_ID = new RaftGroupId(METADATA_CP_GROUP_NAME, 0, 0);

    private static final long DISCOVER_INITIAL_CP_MEMBERS_TASK_DELAY_MILLIS = 1000;
    private static final long BROADCAST_ACTIVE_CP_MEMBERS_TASK_PERIOD_SECONDS = 10;

    private final NodeEngine nodeEngine;
    private final RaftService raftService;
    private final ILogger logger;
    private final CPSubsystemConfig config;
    private final AtomicReference<CPMemberInfo> localMember = new AtomicReference<CPMemberInfo>();
    // groups are read outside of Raft
    private final ConcurrentMap<CPGroupId, CPGroupInfo> groups = new ConcurrentHashMap<CPGroupId, CPGroupInfo>();
    private final AtomicReference<RaftGroupId> metadataGroupIdRef = new AtomicReference<RaftGroupId>(INITIAL_METADATA_GROUP_ID);
    // activeMembers must be an ordered non-null collection
    private volatile Collection<CPMemberInfo> activeMembers = Collections.emptySet();
    private volatile long activeMembersCommitIndex;
    private final AtomicBoolean discoveryCompleted = new AtomicBoolean();
    private volatile Collection<CPMemberInfo> initialCPMembers;
    private volatile MembershipChangeContext membershipChangeContext;

    MetadataRaftGroupManager(NodeEngine nodeEngine, RaftService raftService, CPSubsystemConfig config) {
        this.nodeEngine = nodeEngine;
        this.raftService = raftService;
        this.logger = nodeEngine.getLogger(getClass());
        this.config = config;
    }

    boolean init() {
        boolean cpSubsystemEnabled = (config.getCPMemberCount() > 0);
        if (cpSubsystemEnabled) {
            scheduleDiscoverInitialCPMembersTask();
        } else {
            disableDiscovery();
        }

        return cpSubsystemEnabled;
    }

    void initPromotedCPMember(CPMemberInfo member) {
        if (!localMember.compareAndSet(null, member)) {
            return;
        }

        scheduleRaftGroupMembershipManagementTasks();
    }

    private void scheduleRaftGroupMembershipManagementTasks() {
        ExecutionService executionService = nodeEngine.getExecutionService();
        executionService.scheduleWithRepetition(new BroadcastActiveCPMembersTask(),
                BROADCAST_ACTIVE_CP_MEMBERS_TASK_PERIOD_SECONDS, BROADCAST_ACTIVE_CP_MEMBERS_TASK_PERIOD_SECONDS, SECONDS);

        RaftGroupMembershipManager membershipManager = new RaftGroupMembershipManager(nodeEngine, raftService);
        membershipManager.init();
    }

    void restart(long seed) {
        // reset order:
        // 1. active members
        // 2. active members index
        // 3. metadata group id ref

        activeMembers = Collections.emptySet();
        activeMembersCommitIndex = 0;
        groups.clear();
        initialCPMembers = null;
        metadataGroupIdRef.set(new RaftGroupId(METADATA_CP_GROUP_NAME, seed, 0));
        membershipChangeContext = null;
        localMember.set(null);
        discoveryCompleted.set(false);

        scheduleDiscoverInitialCPMembersTask();
    }

    @Override
    public MetadataRaftGroupSnapshot takeSnapshot(CPGroupId groupId, long commitIndex) {
        if (!getMetadataGroupId().equals(groupId)) {
            return null;
        }

        if (logger.isFineEnabled()) {
            logger.fine("Taking snapshot for commit-index: " + commitIndex);
        }

        MetadataRaftGroupSnapshot snapshot = new MetadataRaftGroupSnapshot();
        for (CPMemberInfo member : activeMembers) {
            snapshot.addMember(member);
        }
        snapshot.setMembersCommitIndex(activeMembersCommitIndex);

        for (CPGroupInfo group : groups.values()) {
            snapshot.addRaftGroup(group);
        }

        snapshot.setMembershipChangeContext(membershipChangeContext);
        return snapshot;
    }

    @Override
    public void restoreSnapshot(CPGroupId groupId, long commitIndex, MetadataRaftGroupSnapshot snapshot) {
        ensureMetadataGroupId(groupId);
        checkNotNull(snapshot);

        Set<CPGroupId> snapshotGroupIds = new HashSet<CPGroupId>();
        for (CPGroupInfo group : snapshot.getGroups()) {
            groups.put(group.id(), group);
            snapshotGroupIds.add(group.id());
        }

        Iterator<CPGroupId> it = groups.keySet().iterator();
        while (it.hasNext()) {
            if (!snapshotGroupIds.contains(it.next())) {
                it.remove();
            }
        }

        doSetActiveMembers(snapshot.getMembersCommitIndex(), new LinkedHashSet<CPMemberInfo>(snapshot.getMembers()));

        membershipChangeContext = snapshot.getMembershipChangeContext();

        if (logger.isFineEnabled()) {
            logger.fine("Restored snapshot at commit-index: " + commitIndex);
        }
    }

    private void ensureMetadataGroupId(CPGroupId groupId) {
        CPGroupId metadataGroupId = getMetadataGroupId();
        checkTrue(metadataGroupId.equals(groupId), "Invalid RaftGroupId! Expected: " + metadataGroupId
                + ", Actual: " + groupId);
    }

    CPMemberInfo getLocalCPMember() {
        return localMember.get();
    }

    public RaftGroupId getMetadataGroupId() {
        return metadataGroupIdRef.get();
    }

    long getGroupIdSeed() {
        return getMetadataGroupId().seed();
    }

    public Collection<CPGroupId> getGroupIds() {
        List<CPGroupId> groupIds = new ArrayList<CPGroupId>(groups.keySet());
        sort(groupIds, new CPGroupIdComparator());

        return groupIds;
    }

    public Collection<CPGroupId> getActiveGroupIds() {
        List<CPGroupId> activeGroupIds = new ArrayList<CPGroupId>(1);
        for (CPGroupInfo group : groups.values()) {
            if (group.status() == ACTIVE) {
                activeGroupIds.add(group.id());
            }
        }

        sort(activeGroupIds, new CPGroupIdComparator());

        return activeGroupIds;
    }

    public CPGroupInfo getRaftGroup(CPGroupId groupId) {
        checkNotNull(groupId);

        if ((groupId instanceof RaftGroupId) && ((RaftGroupId) groupId).seed() < getGroupIdSeed()) {
            throw new CPGroupDestroyedException(groupId);
        }

        return groups.get(groupId);
    }

    public CPGroupInfo getActiveRaftGroup(String groupName) {
        for (CPGroupInfo group : groups.values()) {
            if (group.status() == CPGroupStatus.ACTIVE && group.name().equals(groupName)) {
                return group;
            }
        }

        return null;
    }

    public void initializeMetadataRaftGroup(long commitIndex, List<CPMemberInfo> initialMembers, int metadataMembersCount,
                                            long expectedGroupIdSeed) {
        checkNotNull(initialMembers);
        checkTrue(metadataMembersCount > 1, "initial METADATA CP group must contain at least 2 members: "
                + metadataMembersCount);
        checkTrue(initialMembers.size() >= metadataMembersCount, "Initial CP members should contain all metadata members");

        if (initialCPMembers != null) {
            checkTrue(initialCPMembers.size() == initialMembers.size(), "Invalid initial CP members! Expected: "
                    + initialCPMembers + ", Actual: " + initialMembers);
            checkTrue(initialCPMembers.containsAll(initialMembers), "Invalid initial CP members! Expected: "
                    + initialCPMembers + ", Actual: " + initialMembers);
        }

        List<CPMemberInfo> metadataMembers = initialMembers.subList(0, metadataMembersCount);
        CPGroupInfo metadataGroup = new CPGroupInfo(getMetadataGroupId(), metadataMembers);
        CPGroupInfo existingMetadataGroup = groups.putIfAbsent(getMetadataGroupId(), metadataGroup);
        if (existingMetadataGroup != null) {
            checkTrue(metadataMembersCount == existingMetadataGroup.initialMemberCount(),
                    "Cannot create METADATA CP group with " + metadataMembersCount
                            + " because it already exists with a different member list: " + existingMetadataGroup);

            for (CPMemberInfo member : metadataMembers) {
                checkTrue(existingMetadataGroup.containsInitialMember(member),
                        "Cannot create METADATA CP group with " + metadataMembersCount
                        + " because it already exists with a different member list: " + existingMetadataGroup);
            }

            return;
        }

        long groupIdSeed = getGroupIdSeed();
        checkTrue(groupIdSeed == expectedGroupIdSeed, "Cannot create METADATA CP group. Local groupId seed: "
                + groupIdSeed + ", expected groupId seed: " + expectedGroupIdSeed);

        if (initialCPMembers != null) {
            // already initialized...
            return;
        }

        Collection<CPMemberInfo> cpMembers = new LinkedHashSet<CPMemberInfo>(initialMembers);
        initialCPMembers = unmodifiableCollection(cpMembers);
        doSetActiveMembers(commitIndex, initialMembers);

        if (logger.isFineEnabled()) {
            logger.fine("METADATA CP group is initialized: " + metadataGroup + " seed: " + expectedGroupIdSeed);
        }
    }

    public CPGroupId createRaftGroup(String groupName, Collection<CPMemberInfo> members, long commitIndex) {
        checkFalse(METADATA_CP_GROUP_NAME.equalsIgnoreCase(groupName), groupName + " is reserved for internal usage!");
        checkIfMetadataRaftGroupInitialized();

        // keep configuration on every metadata node
        CPGroupInfo group = getRaftGroupByName(groupName);
        if (group != null) {
            if (group.memberCount() == members.size()) {
                if (logger.isFineEnabled()) {
                    logger.fine("CP group " + groupName + " already exists.");
                }

                return group.id();
            }

            throw new IllegalStateException(group.getId() + " already exists with a different size: " + group.memberCount());
        }

        CPMemberInfo leavingMember = membershipChangeContext != null ? membershipChangeContext.getLeavingMember() : null;
        for (CPMemberInfo member : members) {
            if (member.equals(leavingMember) || !activeMembers.contains(member)) {
                throw new CannotCreateRaftGroupException("Cannot create CP group: " + groupName + " since " + member
                        + " is not active");
            }
        }

        return createRaftGroup(new CPGroupInfo(new RaftGroupId(groupName, getGroupIdSeed(), commitIndex), members));
    }

    @SuppressWarnings("unchecked")
    private CPGroupId createRaftGroup(CPGroupInfo group) {
        addRaftGroup(group);

        logger.info("New " + group.id() + " is created with " + group.members());

        RaftGroupId groupId = group.id();
        if (group.containsMember(getLocalCPMember())) {
            raftService.createRaftNode(groupId, group.memberImpls());
        } else {
            // Broadcast group-info to non-metadata group members
            OperationService operationService = nodeEngine.getOperationService();
            CPGroupInfo metadataGroup = groups.get(getMetadataGroupId());
            for (CPMemberInfo member : group.memberImpls()) {
                if (!metadataGroup.containsMember(member)) {
                    Operation op = new CreateRaftNodeOp(group.id(), (Collection) group.initialMembers());
                    operationService.send(op, member.getAddress());
                }
            }
        }

        return groupId;
    }

    private void addRaftGroup(CPGroupInfo group) {
        CPGroupId groupId = group.id();
        checkState(!groups.containsKey(groupId), group + " already exists!");
        groups.put(groupId, group);
    }

    private CPGroupInfo getRaftGroupByName(String name) {
        for (CPGroupInfo group : groups.values()) {
            if (group.status() != DESTROYED && group.name().equals(name)) {
                return group;
            }
        }
        return null;
    }

    public void triggerDestroyRaftGroup(CPGroupId groupId) {
        checkNotNull(groupId);
        checkState(membershipChangeContext == null,
                "Cannot destroy " + groupId + " while there are ongoing CP membership changes!");

        CPGroupInfo group = groups.get(groupId);
        checkNotNull(group, "No CP group exists for " + groupId + " to destroy!");

        if (group.setDestroying()) {
            logger.info("Destroying " + groupId);
        } else if (logger.isFineEnabled()) {
            logger.fine(groupId + " is already " + group.status());
        }
    }

    public void completeDestroyRaftGroups(Set<CPGroupId> groupIds) {
        checkNotNull(groupIds);

        for (CPGroupId groupId : groupIds) {
            checkNotNull(groupId);

            CPGroupInfo group = groups.get(groupId);
            checkNotNull(group, groupId + " does not exist to complete destroy");

            completeDestroyRaftGroup(group);
        }
    }

    private void completeDestroyRaftGroup(CPGroupInfo group) {
        CPGroupId groupId = group.id();
        if (group.setDestroyed()) {
            logger.info(groupId + " is destroyed.");
            sendDestroyRaftNodeOps(group);
        } else if (logger.isFineEnabled()) {
            logger.fine(groupId + " is already destroyed.");
        }
    }

    public void forceDestroyRaftGroup(String groupName) {
        checkNotNull(groupName);
        checkFalse(METADATA_CP_GROUP_NAME.equalsIgnoreCase(groupName), "Cannot force-destroy the METADATA CP group!");

        boolean found = false;

        for (CPGroupInfo group : groups.values()) {
            if (group.name().equals(groupName)) {
                if (group.forceSetDestroyed()) {
                    logger.info(group.id() + " is force-destroyed.");
                    sendDestroyRaftNodeOps(group);
                } else if (logger.isFineEnabled()) {
                    logger.fine(group.id() + " is already force-destroyed.");
                }

                found = true;
            }
        }

        if (!found) {
            throw new IllegalArgumentException("CP group with name: " + groupName + " does not exist to force-destroy!");
        }
    }

    private void sendDestroyRaftNodeOps(CPGroupInfo group) {
        OperationService operationService = nodeEngine.getOperationService();
        Operation op = new DestroyRaftNodesOp(Collections.<CPGroupId>singleton(group.id()));
        for (CPMemberInfo member : group.memberImpls())  {
            if (member.equals(getLocalCPMember())) {
                raftService.destroyRaftNode(group.id());
            } else {
                operationService.send(op, member.getAddress());
            }
        }
    }

    /**
     * this method is idempotent
     */
    public boolean triggerRemoveMember(long commitIndex, CPMemberInfo leavingMember) {
        checkNotNull(leavingMember);
        checkIfMetadataRaftGroupInitialized();

        if (!activeMembers.contains(leavingMember)) {
            logger.warning("Not removing " + leavingMember + " since it is not an active CP member");
            return true;
        }

        if (membershipChangeContext != null) {
            if (leavingMember.equals(membershipChangeContext.getLeavingMember())) {
                membershipChangeContext = membershipChangeContext.addRetriedCommitIndex(commitIndex);

                if (logger.isFineEnabled()) {
                    logger.fine(leavingMember + " is already marked as leaving.");
                }

                return false;
            }

            throw new CannotRemoveCPMemberException("There is already an ongoing CP membership change process. "
                    + "Cannot process remove request of " + leavingMember);
        }

        if (activeMembers.size() <= 2) {
            logger.warning(leavingMember + " is directly removed as there are only " + activeMembers.size() + " CP members");
            removeActiveMember(commitIndex, leavingMember);
            return true;
        }

        return initializeMembershipChangeContextForLeavingMember(commitIndex, leavingMember);
    }

    private boolean initializeMembershipChangeContextForLeavingMember(long commitIndex, CPMemberInfo leavingMember) {
        List<CPGroupId> leavingGroupIds = new ArrayList<CPGroupId>();
        List<CPGroupMembershipChangeContext> leavingGroups = new ArrayList<CPGroupMembershipChangeContext>();
        for (CPGroupInfo group : groups.values()) {
            CPGroupId groupId = group.id();
            if (!group.containsMember(leavingMember) || group.status() == DESTROYED) {
                continue;
            }

            CPMemberInfo substitute = findSubstitute(group);
            if (substitute != null) {
                leavingGroupIds.add(groupId);
                leavingGroups.add(new CPGroupMembershipChangeContext(groupId, group.getMembersCommitIndex(),
                        group.memberImpls(), substitute, leavingMember));
            } else {
                leavingGroupIds.add(groupId);
                leavingGroups.add(new CPGroupMembershipChangeContext(groupId, group.getMembersCommitIndex(),
                        group.memberImpls(), null, leavingMember));
            }
        }

        if (leavingGroups.isEmpty()) {
            if (logger.isFineEnabled()) {
                logger.fine("Removing " + leavingMember + " directly since it is not present in any CP group.");
            }
            removeActiveMember(commitIndex, leavingMember);
            return true;
        }

        membershipChangeContext = MembershipChangeContext.memberLeaving(singletonList(commitIndex), leavingMember, leavingGroups);
        if (logger.isFineEnabled()) {
            logger.info(leavingMember + " will be removed from " + leavingGroups);
        } else {
            logger.info(leavingMember + " will be removed from " + leavingGroupIds);
        }

        return false;
    }

    private CPMemberInfo findSubstitute(CPGroupInfo group) {
        for (CPMemberInfo substitute : activeMembers) {
            if (activeMembers.contains(substitute) && !group.containsMember(substitute)) {
                return substitute;
            }
        }

        return null;
    }

    public MembershipChangeContext completeRaftGroupMembershipChanges(long commitIndex,
                                                                      Map<CPGroupId, Tuple2<Long, Long>> changedGroups) {
        checkNotNull(changedGroups);
        checkState(membershipChangeContext != null, "Cannot apply CP membership changes: "
                + changedGroups + " since there is no membership change context!");

        for (CPGroupMembershipChangeContext ctx : membershipChangeContext.getChanges()) {
            CPGroupId groupId = ctx.getGroupId();
            CPGroupInfo group = groups.get(groupId);
            checkState(group != null, groupId + "not found in CP groups: " + groups.keySet()
                    + "to apply " + ctx);
            Tuple2<Long, Long> t = changedGroups.get(groupId);

            if (t == null) {
                if (group.status() == DESTROYED && !changedGroups.containsKey(groupId)) {
                    if (logger.isFineEnabled()) {
                        logger.warning(groupId + " is already destroyed so will skip: " + ctx);
                    }
                    changedGroups.put(groupId, Tuple2.of(0L, 0L));
                }
                continue;
            }

            if (!applyMembershipChange(ctx, group, t.element1, t.element2)) {
                changedGroups.remove(groupId);
            }
        }

        membershipChangeContext = membershipChangeContext.excludeCompletedChanges(changedGroups.keySet());

        CPMemberInfo leavingMember = membershipChangeContext.getLeavingMember();
        if (checkSafeToRemove(leavingMember)) {
            checkState(membershipChangeContext.getChanges().isEmpty(), "Leaving " + leavingMember
                    + " is removed from all groups but there are still pending membership changes: "
                    + membershipChangeContext);
            removeActiveMember(commitIndex, leavingMember);
            completeFutures(getMetadataGroupId(), membershipChangeContext.getMembershipChangeCommitIndices(), null);
            membershipChangeContext = null;
            logger.info(leavingMember + " is removed from the CP subsystem.");

        } else if (membershipChangeContext.getChanges().isEmpty()) {
            completeFutures(getMetadataGroupId(), membershipChangeContext.getMembershipChangeCommitIndices(), null);
            membershipChangeContext = null;
            logger.info("Rebalancing is completed.");
        }

        return membershipChangeContext;
    }

    private void completeFutures(CPGroupId groupId, Collection<Long> indices, Object result) {
        if (!indices.isEmpty()) {
            RaftNodeImpl raftNode = (RaftNodeImpl) raftService.getRaftNode(groupId);
            if (raftNode != null) {
                for (Long index : indices) {
                    raftNode.completeFuture(index, result);
                }
            } else {
                logger.severe("RaftNode not found for " + groupId + " to notify commit indices " + indices + " with " + result);
            }
        }
    }

    private boolean applyMembershipChange(CPGroupMembershipChangeContext ctx, CPGroupInfo group,
                                          long expectedMembersCommitIndex, long newMembersCommitIndex) {
        CPMemberInfo addedMember = ctx.getMemberToAdd();
        CPMemberInfo removedMember = ctx.getMemberToRemove();

        if (group.applyMembershipChange(removedMember, addedMember, expectedMembersCommitIndex, newMembersCommitIndex)) {
            if (logger.isFineEnabled()) {
                logger.fine("Applied add-member: " + (addedMember != null ? addedMember : "-") + " and remove-member: "
                        + (removedMember != null ? removedMember : "-") + " in "  + group.id()
                        + " with new members commit index: " + newMembersCommitIndex);
            }
            if (getLocalCPMember().equals(addedMember)) {
                // we are the added member to the group, we can try to create the local raft node if not created already
                raftService.createRaftNode(group.id(), group.memberImpls());
            }

            return true;
        }

        logger.severe("Could not apply add-member: " + (addedMember != null ? addedMember : "-")
                + " and remove-member: " + (removedMember != null ? removedMember : "-") + " in "  + group
                + " with new members commit index: " + newMembersCommitIndex + " expected members commit index: "
                + expectedMembersCommitIndex + " known members commit index: " + group.getMembersCommitIndex());

        return false;
    }

    private boolean checkSafeToRemove(CPMemberInfo leavingMember) {
        if (leavingMember == null) {
            return false;
        }

        for (CPGroupInfo group : groups.values()) {
            if (group.containsMember(leavingMember)) {
                if (group.status() != DESTROYED) {
                    return false;
                } else if (logger.isFineEnabled()) {
                    logger.warning("Leaving " + leavingMember + " was in the destroyed " + group.id());
                }
            }
        }

        return true;
    }

    private List<CPGroupMembershipChangeContext> getGroupMembershipChangesForNewMember(CPMemberInfo newMember) {
        List<CPGroupMembershipChangeContext> changes = new ArrayList<CPGroupMembershipChangeContext>();
        for (CPGroupInfo group : groups.values()) {
            if (group.status() == ACTIVE && group.initialMemberCount() > group.memberCount()) {
                checkState(!group.memberImpls().contains(newMember), group + " already contains: " + newMember);

                changes.add(new CPGroupMembershipChangeContext(group.id(), group.getMembersCommitIndex(), group.memberImpls(),
                        newMember, null));
            }
        }

        return changes;
    }

    public Collection<CPMemberInfo> getActiveMembers() {
        return activeMembers;
    }

    public void handleMetadataGroupId(RaftGroupId newMetadataGroupId) {
        checkNotNull(newMetadataGroupId);
        RaftGroupId metadataGroupId = getMetadataGroupId();
        while (metadataGroupId.seed() < newMetadataGroupId.seed()) {
            if (metadataGroupIdRef.compareAndSet(metadataGroupId, newMetadataGroupId)) {
                if (logger.isFineEnabled()) {
                    logger.fine("Updated METADATA groupId: " + newMetadataGroupId);
                }

                return;
            }

            metadataGroupId = getMetadataGroupId();
        }
    }

    private void updateInvocationManagerMembers(long groupIdSeed, long membersCommitIndex, Collection<CPMemberInfo> members) {
        RaftInvocationContext context = raftService.getInvocationManager().getRaftInvocationContext();
        context.setMembers(groupIdSeed, membersCommitIndex, members);
    }

    public Collection<CPGroupId> getDestroyingGroupIds() {
        Collection<CPGroupId> groupIds = new ArrayList<CPGroupId>();
        for (CPGroupInfo group : groups.values()) {
            if (group.status() == DESTROYING) {
                groupIds.add(group.id());
            }
        }
        return groupIds;
    }

    public MembershipChangeContext getMembershipChangeContext() {
        return membershipChangeContext;
    }

    // could return stale information
    boolean isMetadataGroupLeader() {
        CPMemberInfo member = getLocalCPMember();
        if (member == null) {
            return false;
        }
        RaftNode raftNode = raftService.getRaftNode(getMetadataGroupId());
        return raftNode != null && !raftNode.isTerminatedOrSteppedDown() && member.equals(raftNode.getLeader());
    }

    /**
     * this method is idempotent
     */
    public boolean addActiveMember(long commitIndex, CPMemberInfo member) {
        checkNotNull(member);
        checkIfMetadataRaftGroupInitialized();

        for (CPMemberInfo existingMember : activeMembers) {
            if (existingMember.getAddress().equals(member.getAddress())) {
                if (existingMember.getUuid().equals(member.getUuid())) {
                    if (logger.isFineEnabled()) {
                        logger.fine(member + " already exists.");
                    }

                    if (membershipChangeContext != null && member.equals(membershipChangeContext.getAddedMember())) {
                        membershipChangeContext = membershipChangeContext.addRetriedCommitIndex(commitIndex);
                        logger.info("CP groups are already being rebalanced for " + member);
                        return false;
                    }

                    return true;
                }

                throw new IllegalStateException(member + " cannot be added to the CP subsystem because another " + existingMember
                        + " exists with the same address!");
            }
        }

        checkState(membershipChangeContext == null,
                "Cannot rebalance CP groups because there is ongoing " + membershipChangeContext);

        Collection<CPMemberInfo> newMembers = new LinkedHashSet<CPMemberInfo>(activeMembers);
        newMembers.add(member);
        doSetActiveMembers(commitIndex, unmodifiableCollection(newMembers));
        logger.info("Added new " + member + ". New active CP members list: " + newMembers);

        List<CPGroupMembershipChangeContext> changes = getGroupMembershipChangesForNewMember(member);
        if (changes.size() > 0) {
            membershipChangeContext = MembershipChangeContext.memberAdded(singletonList(commitIndex), member, changes);
            if (logger.isFineEnabled()) {
                logger.fine("CP group rebalancing is triggered for " + member + ", changes: " + membershipChangeContext);
            }

            return false;
        }

        return true;
    }

    private void removeActiveMember(long commitIndex, CPMemberInfo member) {
        Collection<CPMemberInfo> newMembers = new LinkedHashSet<CPMemberInfo>(activeMembers);
        newMembers.remove(member);
        doSetActiveMembers(commitIndex, unmodifiableCollection(newMembers));
    }

    private void doSetActiveMembers(long commitIndex, Collection<CPMemberInfo> members) {
        // first set the active members, then set the commit index.
        // because readers will use commit index for comparison, etc.
        // When a caller reads commit index first, it knows that the active members
        // it has read is at least up to date as the commit index
        activeMembers = unmodifiableCollection(members);
        activeMembersCommitIndex = commitIndex;
        updateInvocationManagerMembers(getMetadataGroupId().seed(), commitIndex, members);
        raftService.updateMissingMembers();
        broadcastActiveCPMembers();
    }

    private void checkIfMetadataRaftGroupInitialized() {
        if (!groups.containsKey(getMetadataGroupId())) {
            throw new MetadataRaftGroupNotInitializedException();
        }
    }

    void broadcastActiveCPMembers() {
        if (!isMetadataGroupLeader()) {
            return;
        }

        RaftGroupId metadataGroupId = getMetadataGroupId();
        long commitIndex = this.activeMembersCommitIndex;
        Collection<CPMemberInfo> cpMembers = this.activeMembers;

        if (cpMembers.isEmpty()) {
            return;
        }

        Set<Member> clusterMembers = nodeEngine.getClusterService().getMembers();
        OperationService operationService = nodeEngine.getOperationService();
        Operation op = new PublishActiveCPMembersOp(metadataGroupId, commitIndex, cpMembers);
        for (Member member : clusterMembers) {
            if (member.localMember()) {
                continue;
            }
            operationService.send(op, member.getAddress());
        }
    }

    boolean isDiscoveryCompleted() {
        return discoveryCompleted.get();
    }

    public void disableDiscovery() {
        if (config.getCPMemberCount() > 0) {
            logger.info("Disabling discovery of initial CP members since it is already completed...");
        }

        discoveryCompleted.set(true);
    }

    private void scheduleDiscoverInitialCPMembersTask() {
        Runnable task = new DiscoverInitialCPMembersTask();
        ExecutionService executionService = nodeEngine.getExecutionService();
        executionService.schedule(task, DISCOVER_INITIAL_CP_MEMBERS_TASK_DELAY_MILLIS, MILLISECONDS);
    }

    private class BroadcastActiveCPMembersTask implements Runnable {
        @Override
        public void run() {
            if (!isMetadataGroupLeader()) {
                return;
            }
            broadcastActiveCPMembers();
        }
    }

    private class DiscoverInitialCPMembersTask implements Runnable {

        private Collection<Member> latestMembers = Collections.emptySet();

        @Override
        public void run() {
            if (shouldSkipOrReschedule()) {
                return;
            }

            Collection<Member> members = nodeEngine.getClusterService().getMembers();
            for (Member member : latestMembers) {
                if (!members.contains(member)) {
                    logger.severe(member + " left the cluster while discovering initial CP members!");
                    terminateNode();
                    return;
                }
            }

            latestMembers = members;

            if (rescheduleIfCPMemberCountNotSatisfied(members)) {
                return;
            }

            CPMemberInfo localMemberCandidate = new CPMemberInfo(nodeEngine.getLocalMember());
            List<CPMemberInfo> cpMembers = getInitialCPMembers(members);

            if (completeDiscoveryIfNotCPMember(cpMembers, localMemberCandidate)) {
                return;
            }

            // By default, we use the same member UUID for both AP and CP members.
            // But it's not guaranteed to be same. For example;
            // - During a split-brain merge, AP member UUID is renewed but CP member UUID remains the same.
            // - While promoting a member to CP when Hot Restart is enabled, CP member doesn't use the AP member's UUID
            // but instead generates a new UUID.
            // We must set the local member before initializing the Metadata group
            // so that the local RaftNode object will be created if I am a Metadata group member
            localMember.set(localMemberCandidate);

            // we must update invocation manager's member list before making the first raft invocation
            updateInvocationManagerMembers(getMetadataGroupId().seed(), 0, cpMembers);

            if (!initializeMetadataRaftGroup(cpMembers)) {
                terminateNode();
                return;
            }

            logger.info("Initial CP members: " + activeMembers + ", local: " + getLocalCPMember());
            discoveryCompleted.set(true);
            broadcastActiveCPMembers();
            scheduleRaftGroupMembershipManagementTasks();
        }

        private boolean shouldSkipOrReschedule() {
            if (isDiscoveryCompleted()) {
                return true;
            }

            if (!nodeEngine.getClusterService().isJoined()) {
                scheduleDiscoverInitialCPMembersTask();
                return true;
            }

            return false;
        }

        private boolean rescheduleIfCPMemberCountNotSatisfied(Collection<Member> members) {
            if (members.size() < config.getCPMemberCount()) {
                if (logger.isFineEnabled()) {
                    logger.fine("Waiting for " + config.getCPMemberCount() + " CP members to join the cluster. "
                            + "Current CP member count: " + members.size());
                }

                scheduleDiscoverInitialCPMembersTask();
                return true;
            }
            return false;
        }

        private boolean completeDiscoveryIfNotCPMember(List<CPMemberInfo> cpMembers, CPMemberInfo localCPMemberCandidate) {
            if (!cpMembers.contains(localCPMemberCandidate)) {
                if (logger.isFineEnabled()) {
                    logger.fine("I am not an initial CP member! I'll serve as an AP member.");
                }

                disableDiscovery();
                return true;
            }

            return false;
        }

        private boolean initializeMetadataRaftGroup(List<CPMemberInfo> initialCPMembers) {
            int metadataGroupSize = config.getGroupSize();
            List<CPMemberInfo> metadataMembers = initialCPMembers.subList(0, metadataGroupSize);
            try {
                RaftGroupId metadataGroupId = getMetadataGroupId();
                if (metadataMembers.contains(getLocalCPMember())) {
                    raftService.createRaftNode(metadataGroupId, metadataMembers);
                }

                RaftOp op = new InitializeMetadataRaftGroupOp(initialCPMembers, metadataGroupSize, metadataGroupId.seed());
                raftService.getInvocationManager().invoke(metadataGroupId, op).get();
                logger.info("METADATA CP group is created with " + metadataMembers);
            } catch (Exception e) {
                logger.severe("Could not create METADATA CP group with initial CP members: " + metadataMembers
                        + " and METADATA CP group members: " + metadataMembers, e);
                return false;
            }
            return true;
        }

        private void terminateNode() {
            ((NodeEngineImpl) nodeEngine).getNode().shutdown(true);
        }

        private List<CPMemberInfo> getInitialCPMembers(Collection<Member> members) {
            assert members.size() >= config.getCPMemberCount();
            List<Member> memberList = new ArrayList<Member>(members).subList(0, config.getCPMemberCount());
            List<CPMemberInfo> cpMembers = new ArrayList<CPMemberInfo>(config.getCPMemberCount());
            for (Member member : memberList) {
                // During the discovery process (both initial or cp subsystem restart),
                // it's guaranteed that AP and CP member UUIDs will be the same.
                cpMembers.add(new CPMemberInfo(member));
            }

            sort(cpMembers, new CPMemberComparator());
            return cpMembers;
        }
    }

    @SuppressFBWarnings("SE_COMPARATOR_SHOULD_BE_SERIALIZABLE")
    private static class CPMemberComparator implements Comparator<CPMemberInfo> {
        @Override
        public int compare(CPMemberInfo o1, CPMemberInfo o2) {
            return o1.getUuid().compareTo(o2.getUuid());
        }
    }

    @SuppressFBWarnings("SE_COMPARATOR_SHOULD_BE_SERIALIZABLE")
    private static class CPGroupIdComparator implements Comparator<CPGroupId> {
        @Override
        public int compare(CPGroupId o1, CPGroupId o2) {
            return Long.valueOf(o1.id()).compareTo(o2.id());
        }
    }
}
