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

package com.hazelcast.raft.impl.service;

import com.hazelcast.config.raft.RaftMetadataGroupConfig;
import com.hazelcast.core.Member;
import com.hazelcast.logging.ILogger;
import com.hazelcast.nio.Address;
import com.hazelcast.raft.RaftGroupId;
import com.hazelcast.raft.SnapshotAwareService;
import com.hazelcast.raft.impl.MetadataRaftGroupNotInitializedException;
import com.hazelcast.raft.impl.RaftGroupIdImpl;
import com.hazelcast.raft.impl.RaftMemberImpl;
import com.hazelcast.raft.impl.RaftNode;
import com.hazelcast.raft.impl.RaftOp;
import com.hazelcast.raft.impl.service.exception.CannotCreateRaftGroupException;
import com.hazelcast.raft.impl.service.exception.CannotRemoveMemberException;
import com.hazelcast.raft.impl.service.operation.metadata.CreateMetadataRaftGroupOp;
import com.hazelcast.raft.impl.service.operation.metadata.CreateRaftNodeOp;
import com.hazelcast.raft.impl.service.operation.metadata.DestroyRaftNodesOp;
import com.hazelcast.raft.impl.service.operation.metadata.SendActiveRaftMembersOp;
import com.hazelcast.raft.impl.util.Tuple2;
import com.hazelcast.spi.ExecutionService;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.OperationService;
import com.hazelcast.spi.impl.NodeEngineImpl;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicReference;

import static com.hazelcast.raft.RaftGroup.RaftGroupStatus.ACTIVE;
import static com.hazelcast.raft.RaftGroup.RaftGroupStatus.DESTROYED;
import static com.hazelcast.raft.RaftGroup.RaftGroupStatus.DESTROYING;
import static com.hazelcast.raft.impl.service.MembershipChangeContext.RaftGroupMembershipChangeContext;
import static com.hazelcast.util.Preconditions.checkFalse;
import static com.hazelcast.util.Preconditions.checkNotNull;
import static com.hazelcast.util.Preconditions.checkState;
import static com.hazelcast.util.Preconditions.checkTrue;
import static java.util.Collections.singleton;
import static java.util.Collections.sort;
import static java.util.Collections.unmodifiableCollection;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;

/**
 * TODO: Javadoc Pending...
 */
@SuppressWarnings({"checkstyle:methodcount", "checkstyle:classdataabstractioncoupling"})
public class RaftMetadataManager implements SnapshotAwareService<MetadataSnapshot>  {

    public static final RaftGroupId METADATA_GROUP_ID = new RaftGroupIdImpl("METADATA", 0);

    private static final long DISCOVER_INITIAL_RAFT_MEMBERS_TASK_DELAY_MILLIS = 500;
    private static final long BROADCAST_ACTIVE_RAFT_MEMBERS_TASK_PERIOD_SECONDS = 10;

    private final NodeEngine nodeEngine;
    private final RaftService raftService;
    private final ILogger logger;
    private final RaftMetadataGroupConfig config;

    private final AtomicReference<RaftMemberImpl> localMember = new AtomicReference<RaftMemberImpl>();
    // groups are read outside of Raft
    private final ConcurrentMap<RaftGroupId, RaftGroupInfo> groups = new ConcurrentHashMap<RaftGroupId, RaftGroupInfo>();
    // activeMembers must be an ordered non-null collection
    private volatile Collection<RaftMemberImpl> activeMembers = Collections.emptySet();
    private volatile boolean discoveryCompleted;
    private Collection<RaftMemberImpl> initialRaftMembers;
    private MembershipChangeContext membershipChangeContext;

    RaftMetadataManager(NodeEngine nodeEngine, RaftService raftService, RaftMetadataGroupConfig config) {
        this.nodeEngine = nodeEngine;
        this.raftService = raftService;
        this.logger = nodeEngine.getLogger(getClass());
        this.config = config;
    }

    void initInitialRaftMember() {
        boolean initialRaftMember = config != null;
        if (!initialRaftMember) {
            logger.warning("I am not a Raft member :(");
            return;
        }
        initLocalMember();

        // task for initial Raft members
        nodeEngine.getExecutionService()
                  .schedule(new DiscoverInitialRaftMembersTask(), DISCOVER_INITIAL_RAFT_MEMBERS_TASK_DELAY_MILLIS, MILLISECONDS);
    }

    void init() {
        if (!initLocalMember()) {
            return;
        }

        scheduleManagementTasks();
    }

    private void scheduleManagementTasks() {
        ExecutionService executionService = nodeEngine.getExecutionService();
        executionService.scheduleWithRepetition(new BroadcastActiveRaftMembersTask(),
                BROADCAST_ACTIVE_RAFT_MEMBERS_TASK_PERIOD_SECONDS, BROADCAST_ACTIVE_RAFT_MEMBERS_TASK_PERIOD_SECONDS, SECONDS);

        RaftCleanupHandler cleanupHandler = new RaftCleanupHandler(nodeEngine, raftService);
        cleanupHandler.init();
    }

    private boolean initLocalMember() {
        Member localMember = nodeEngine.getLocalMember();
        return this.localMember.compareAndSet(null, new RaftMemberImpl(localMember));
    }

    void reset() {
        activeMembers = Collections.emptySet();
        groups.clear();
        initialRaftMembers = null;

        if (config == null) {
            return;
        }
        discoveryCompleted = false;
        initLocalMember();

        nodeEngine.getExecutionService()
                  .schedule(new DiscoverInitialRaftMembersTask(), DISCOVER_INITIAL_RAFT_MEMBERS_TASK_DELAY_MILLIS, MILLISECONDS);
    }

    @Override
    public MetadataSnapshot takeSnapshot(RaftGroupId groupId, long commitIndex) {
        if (!METADATA_GROUP_ID.equals(groupId)) {
            return null;
        }

        logger.info("Taking snapshot for commit-index: " + commitIndex);
        MetadataSnapshot snapshot = new MetadataSnapshot();
        for (RaftGroupInfo group : groups.values()) {
            assert group.commitIndex() <= commitIndex
                    : "Group commit index: " + group.commitIndex() + ", snapshot commit index: " + commitIndex;
            snapshot.addRaftGroup(group);
        }
        for (RaftMemberImpl member : activeMembers) {
            snapshot.addMember(member);
        }
        snapshot.setMembershipChangeContext(membershipChangeContext);
        return snapshot;
    }

    @Override
    public void restoreSnapshot(RaftGroupId groupId, long commitIndex, MetadataSnapshot snapshot) {
        ensureMetadataGroupId(groupId);
        checkNotNull(snapshot);

        logger.info("Restoring snapshot for commit-index: " + commitIndex);
        for (RaftGroupInfo group : snapshot.getRaftGroups()) {
            RaftGroupInfo existingGroup = groups.get(group.id());

            if (group.status() == ACTIVE && existingGroup == null) {
                createRaftGroup(group);
                continue;
            }

            if (group.status() == DESTROYING) {
                if (existingGroup == null) {
                    createRaftGroup(group);
                } else {
                    existingGroup.setDestroying();
                }
                continue;
            }

            if (group.status() == DESTROYED) {
                if (existingGroup == null) {
                    addRaftGroup(group);
                } else {
                    completeDestroyRaftGroup(existingGroup);
                }
            }
        }

        activeMembers = unmodifiableCollection(new LinkedHashSet<RaftMemberImpl>(snapshot.getMembers()));
        membershipChangeContext = snapshot.getMembershipChangeContext();

        updateInvocationManagerMembers(getActiveMembers());
    }

    private static void ensureMetadataGroupId(RaftGroupId groupId) {
        checkTrue(METADATA_GROUP_ID.equals(groupId), "Invalid RaftGroupId! Expected: " + METADATA_GROUP_ID
                + ", Actual: " + groupId);
    }

    public RaftMemberImpl getLocalMember() {
        return localMember.get();
    }

    public RaftGroupInfo getRaftGroup(RaftGroupId groupId) {
        checkNotNull(groupId);

        return groups.get(groupId);
    }

    public Collection<RaftGroupId> getRaftGroupIds() {
        return groups.keySet();
    }

    public void createInitialMetadataRaftGroup(List<RaftMemberImpl> initialMembers, int metadataMembersCount) {
        checkNotNull(initialMembers);
        checkTrue(metadataMembersCount > 1, "initial metadata raft group must contain at least 2 members: "
                + metadataMembersCount);
        checkTrue(initialMembers.size() >= metadataMembersCount, "Initial raft members should contain all metadata members");

        if (initialRaftMembers != null) {
            checkTrue(initialRaftMembers.size() == initialMembers.size(), "Invalid initial raft members! Expected: "
                    + initialRaftMembers + ", Actual: " + initialMembers);
            checkTrue(initialRaftMembers.containsAll(initialMembers), "Invalid initial raft members! Expected: "
                    + initialRaftMembers + ", Actual: " + initialMembers);
        }

        List<RaftMemberImpl> metadataMembers = initialMembers.subList(0, metadataMembersCount);
        RaftGroupInfo metadataGroup = new RaftGroupInfo(METADATA_GROUP_ID, metadataMembers);
        RaftGroupInfo existingMetadataGroup = groups.putIfAbsent(METADATA_GROUP_ID, metadataGroup);
        if (existingMetadataGroup != null) {
            checkTrue(metadataMembersCount == existingMetadataGroup.initialMemberCount(),
                    "Cannot create metadata raft group with " + metadataMembersCount
                            + " because it already exists with a different member list: " + existingMetadataGroup);

            for (RaftMemberImpl member : metadataMembers) {
                checkTrue(existingMetadataGroup.containsInitialMember(member),
                        "Cannot create metadata raft group with " + metadataMembersCount
                        + " because it already exists with a different member list: " + existingMetadataGroup);
            }

            return;
        }

        initialRaftMembers = initialMembers;

        logger.fine("METADATA raft group is created: " + metadataGroup);
    }

    public RaftGroupId createRaftGroup(String groupName, Collection<RaftMemberImpl> members, long commitIndex) {
        checkFalse(METADATA_GROUP_ID.name().equals(groupName), groupName + " is reserved for internal usage!");
        checkIfMetadataRaftGroupInitialized();

        // keep configuration on every metadata node
        RaftGroupInfo group = getRaftGroupByName(groupName);
        if (group != null) {
            if (group.memberCount() == members.size()) {
                logger.warning("Raft group " + groupName + " already exists. Ignoring add raft node request.");
                return group.id();
            }

            throw new IllegalStateException("Raft group " + groupName + " already exists with different group size.");
        }

        RaftMemberImpl leavingMember = membershipChangeContext != null ? membershipChangeContext.getLeavingMember() : null;
        for (RaftMemberImpl member : members) {
            if (member.equals(leavingMember) || !activeMembers.contains(member)) {
                throw new CannotCreateRaftGroupException("Cannot create raft group: " + groupName + " since " + member
                        + " is not active");
            }
        }

        return createRaftGroup(new RaftGroupInfo(new RaftGroupIdImpl(groupName, commitIndex), members));
    }

    private RaftGroupId createRaftGroup(RaftGroupInfo group) {
        addRaftGroup(group);
        logger.info("New raft group: " + group.id() + " is created with members: " + group.members());

        RaftGroupId groupId = group.id();
        if (group.containsMember(localMember.get())) {
            raftService.createRaftNode(groupId, group.members());
        } else {
            // Broadcast group-info to non-metadata group members
            OperationService operationService = nodeEngine.getOperationService();
            RaftGroupInfo metadataGroup = groups.get(METADATA_GROUP_ID);
            for (RaftMemberImpl member : group.memberImpls()) {
                if (!metadataGroup.containsMember(member)) {
                    operationService.send(new CreateRaftNodeOp(group.id(), group.initialMembers()), member.getAddress());
                }
            }
        }

        return groupId;
    }

    private void addRaftGroup(RaftGroupInfo group) {
        RaftGroupId groupId = group.id();
        checkState(!groups.containsKey(groupId), group + " already exists!");
        groups.put(groupId, group);
    }

    private RaftGroupInfo getRaftGroupByName(String name) {
        for (RaftGroupInfo group : groups.values()) {
            if (group.status() != DESTROYED && group.name().equals(name)) {
                return group;
            }
        }
        return null;
    }

    public void triggerRebalanceRaftGroups() {
        checkIfMetadataRaftGroupInitialized();

        if (membershipChangeContext != null) {
            checkState(membershipChangeContext.getLeavingMember() == null,
                    "Cannot rebalance raft groups because there is ongoing " + membershipChangeContext);
            return;
        }

        Map<RaftGroupId, List<RaftMemberImpl>> memberMissingGroups = getMemberMissingActiveRaftGroups();
        if (memberMissingGroups.size() > 0) {
            logger.info("Raft group rebalancing is triggered for " + memberMissingGroups);
            membershipChangeContext = new MembershipChangeContext(memberMissingGroups);
        }
    }

    public MembershipChangeContext triggerExpandRaftGroups(Map<RaftGroupId, RaftMemberImpl> membersToAdd) {
        checkNotNull(membersToAdd);
        checkState(membershipChangeContext != null,
                "There is no membership context to expand groups with members: " + membersToAdd);

        for (RaftMemberImpl member : membersToAdd.values()) {
            checkTrue(activeMembers.contains(member), membersToAdd + " is not in active members: " + activeMembers);
        }

        Map<RaftGroupId, List<RaftMemberImpl>> memberMissingGroups = membershipChangeContext.getMemberMissingGroups();
        List<RaftGroupMembershipChangeContext> changes = new ArrayList<RaftGroupMembershipChangeContext>();
        for (Entry<RaftGroupId, RaftMemberImpl> e : membersToAdd.entrySet()) {
            RaftGroupId groupId = e.getKey();
            RaftMemberImpl memberToAdd = e.getValue();
            RaftGroupInfo group = groups.get(groupId);
            checkTrue(group != null, groupId + " not found in the raft groups");

            Collection<RaftMemberImpl> candidates = memberMissingGroups.get(groupId);
            checkTrue(candidates != null, groupId + " has no membership change");
            checkTrue(candidates.contains(memberToAdd), groupId + " does not have " + membersToAdd
                    + " in its candidate list");

            if (group.status() == DESTROYED) {
                logger.warning("Will not expand " + groupId + " with " + membersToAdd + " since the group is already destroyed");
                continue;
            }

            long idx = group.getMembersCommitIndex();
            Collection<RaftMemberImpl> members = group.memberImpls();
            changes.add(new RaftGroupMembershipChangeContext(groupId, idx, members, memberToAdd, null));
        }

        logger.info("Raft groups will be expanded with the following changes: " + changes);

        membershipChangeContext = membershipChangeContext.setChanges(changes);
        return membershipChangeContext;
    }

    public void triggerDestroyRaftGroup(RaftGroupId groupId) {
        checkNotNull(groupId);
        checkState(membershipChangeContext == null,
                "Cannot destroy raft group while there are raft group membership changes");

        RaftGroupInfo group = groups.get(groupId);
        checkNotNull(group, "No raft group exists for " + groupId + " to trigger destroy");

        if (group.setDestroying()) {
            logger.info("Destroying " + groupId);
        } else {
            logger.info(groupId + " is already " + group.status());
        }
    }

    public void completeDestroyRaftGroups(Set<RaftGroupId> groupIds) {
        checkNotNull(groupIds);

        for (RaftGroupId groupId : groupIds) {
            checkNotNull(groupId);

            RaftGroupInfo group = groups.get(groupId);
            checkNotNull(group, "No raft group exists for " + groupId + " to complete destroy");

            completeDestroyRaftGroup(group);
        }
    }

    private void completeDestroyRaftGroup(RaftGroupInfo group) {
        RaftGroupId groupId = group.id();
        if (group.setDestroyed()) {
            logger.info(groupId + " is destroyed.");
            sendDestroyRaftNodeOps(group);
        } else {
            logger.fine(groupId + " is already destroyed.");
        }
    }

    public void forceDestroyRaftGroup(RaftGroupId groupId) {
        checkNotNull(groupId);
        checkFalse(METADATA_GROUP_ID.equals(groupId), "Cannot force-destroy the METADATA raft group");

        RaftGroupInfo group = groups.get(groupId);
        checkNotNull(group, "No raft group exists for " + groupId + " to force-destroy");

        if (group.forceSetDestroyed()) {
            logger.info(groupId + " is force-destroyed.");
            sendDestroyRaftNodeOps(group);
        } else {
            logger.fine(groupId + " is already force-destroyed.");
        }
    }

    private void sendDestroyRaftNodeOps(RaftGroupInfo group) {
        OperationService operationService = nodeEngine.getOperationService();
        Operation op = new DestroyRaftNodesOp(singleton(group.id()));
        for (RaftMemberImpl member : group.memberImpls())  {
            if (member.equals(getLocalMember())) {
                raftService.destroyRaftNode(group.id());
            } else {
                operationService.send(op, member.getAddress());
            }
        }
    }

    /**
     * this method is idempotent
     */
    public void triggerRemoveMember(RaftMemberImpl leavingMember) {
        checkNotNull(leavingMember);
        checkIfMetadataRaftGroupInitialized();

        if (shouldRemoveMember(leavingMember)) {
            return;
        }

        if (activeMembers.size() <= 2) {
            logger.warning(leavingMember + " is directly removed as there are only " + activeMembers.size() + " members");
            removeActiveMember(leavingMember);
            return;
        }

        List<RaftGroupMembershipChangeContext> leavingGroups = new ArrayList<RaftGroupMembershipChangeContext>();
        for (RaftGroupInfo group : groups.values()) {
            RaftGroupId groupId = group.id();
            if (!group.containsMember(leavingMember) || group.status() == DESTROYED) {
                continue;
            }

            RaftMemberImpl substitute = findSubstitute(group, leavingMember);
            if (substitute != null) {
                leavingGroups.add(new RaftGroupMembershipChangeContext(group.id(), group.getMembersCommitIndex(),
                        group.memberImpls(), substitute, leavingMember));
                logger.fine("Substituted " + leavingMember + " with " + substitute + " in " + group);
            } else {
                logger.fine("Cannot find a substitute for " + leavingMember + " in " + group);
                leavingGroups.add(new RaftGroupMembershipChangeContext(groupId, group.getMembersCommitIndex(),
                        group.memberImpls(), null, leavingMember));
            }
        }

        if (leavingGroups.isEmpty()) {
            logger.info(leavingMember + " is not present in any raft group. Removing it directly.");
            removeActiveMember(leavingMember);
            return;
        }

        membershipChangeContext = new MembershipChangeContext(leavingMember, leavingGroups);
        logger.info("Removing " + leavingMember + " from raft groups: " + leavingGroups);
    }

    private boolean shouldRemoveMember(RaftMemberImpl leavingMember) {
        if (!activeMembers.contains(leavingMember)) {
            logger.warning("Not removing " + leavingMember + " since it is not present in the active members");
            return false;
        }

        if (membershipChangeContext != null) {
            if (leavingMember.equals(membershipChangeContext.getLeavingMember())) {
                logger.info(leavingMember + " is already marked as leaving.");
                return true;
            }

            throw new CannotRemoveMemberException("There is already an ongoing raft group membership change process. "
                    + "Cannot process remove request of " + leavingMember);
        }

        return false;
    }

    private RaftMemberImpl findSubstitute(RaftGroupInfo group, RaftMemberImpl leavingMember) {
        for (RaftMemberImpl substitute : activeMembers) {
            if (activeMembers.contains(substitute) && !group.containsMember(substitute)) {
                return substitute;
            }
        }

        return null;
    }

    public MembershipChangeContext completeRaftGroupMembershipChanges(Map<RaftGroupId, Tuple2<Long, Long>> changedGroups) {
        checkNotNull(changedGroups);
        checkState(membershipChangeContext != null, "Cannot apply raft group membership changes: "
                + changedGroups + " since there is no membership change context!");

        for (RaftGroupMembershipChangeContext ctx : membershipChangeContext.getChanges()) {
            RaftGroupId groupId = ctx.getGroupId();
            RaftGroupInfo group = groups.get(groupId);
            checkState(group != null, groupId + "not found in raft groups: " + groups.keySet()
                    + "to apply " + ctx);
            Tuple2<Long, Long> t = changedGroups.get(groupId);

            if (t == null) {
                if (group.status() == DESTROYED && !changedGroups.containsKey(groupId)) {
                    logger.warning(groupId + " is already destroyed so will skip: " + ctx);
                    changedGroups.put(groupId, Tuple2.of(0L, 0L));
                }
                continue;
            }

            applyMembershipChange(ctx, group, t.element1, t.element2);
        }

        membershipChangeContext = membershipChangeContext.excludeCompletedChanges(changedGroups.keySet());

        RaftMemberImpl leavingMember = membershipChangeContext.getLeavingMember();
        if (checkSafeToRemove(leavingMember)) {
            checkState(membershipChangeContext.hasNoPendingChanges(), "Leaving " + leavingMember
                    + " is removed from all groups but there are still pending membership changes: "
                    + membershipChangeContext);
            logger.info(leavingMember + " is removed from all raft groups and active members");
            removeActiveMember(leavingMember);
            membershipChangeContext = null;
            return null;
        }

        if (membershipChangeContext.shouldContinueRaftGroupRebalancing()) {
            // the current raft group rebalancing step is completed. let's attempt for another one
            Map<RaftGroupId, List<RaftMemberImpl>> memberMissingGroups = getMemberMissingActiveRaftGroups();
            if (memberMissingGroups.size() > 0) {
                membershipChangeContext = new MembershipChangeContext(memberMissingGroups);
                logger.info("Raft group rebalancing continues with " + memberMissingGroups);
            } else {
                membershipChangeContext = null;
                logger.info("Rebalancing is completed.");
            }
        }

        return membershipChangeContext;
    }

    private void applyMembershipChange(RaftGroupMembershipChangeContext ctx, RaftGroupInfo group,
                                       long expectedMembersCommitIndex, long newMembersCommitIndex) {
        RaftMemberImpl addedMember = ctx.getMemberToAdd();
        RaftMemberImpl removedMember = ctx.getMemberToRemove();

        if (group.applyMembershipChange(removedMember, addedMember, expectedMembersCommitIndex, newMembersCommitIndex)) {
            logger.fine("Applied add-member: " + (addedMember != null ? addedMember : "-") + " and remove-member: "
                    + (removedMember != null ? removedMember : "-") + " in "  + group.id()
                    + " with new members commit index: " + newMembersCommitIndex);
            if (localMember.get().equals(addedMember)) {
                // we are the added member to the group, we can try to create the local raft node if not created already
                raftService.createRaftNode(group.id(), group.members());
            } else if (addedMember != null) {
                // publish group-info to the joining member
                Operation op = new CreateRaftNodeOp(group.id(), group.initialMembers());
                nodeEngine.getOperationService().send(op, addedMember.getAddress());
            }
        } else {
            logger.severe("Could not apply add-member: " + (addedMember != null ? addedMember : "-")
                    + " and remove-member: " + (removedMember != null ? removedMember : "-") + " in "  + group
                    + " with new members commit index: " + newMembersCommitIndex + " expected members commit index: "
                    + expectedMembersCommitIndex + " known members commit index: " + group.getMembersCommitIndex());
        }
    }

    private boolean checkSafeToRemove(RaftMemberImpl leavingMember) {
        if (leavingMember == null) {
            return false;
        }

        for (RaftGroupInfo group : groups.values()) {
            if (group.containsMember(leavingMember)) {
                if (group.status() == DESTROYED) {
                    logger.warning("Leaving " + leavingMember + " was in the destroyed " + group.id());
                } else {
                    return false;
                }
            }
        }

        return true;
    }

    private Map<RaftGroupId, List<RaftMemberImpl>> getMemberMissingActiveRaftGroups() {
        Map<RaftGroupId, List<RaftMemberImpl>> memberMissingGroups = new HashMap<RaftGroupId, List<RaftMemberImpl>>();
        for (RaftGroupInfo group : groups.values()) {
            if (group.status() == ACTIVE && group.initialMemberCount() > group.memberCount()
                    && activeMembers.size() > group.memberCount()) {
                List<RaftMemberImpl> candidates = new ArrayList<RaftMemberImpl>(activeMembers);
                candidates.removeAll(group.memberImpls());
                memberMissingGroups.put(group.id(), candidates);
            }
        }
        return memberMissingGroups;
    }

    public boolean isMemberRemoved(RaftMemberImpl member) {
        checkNotNull(member);

        return !activeMembers.contains(member);
    }

    public Collection<RaftMemberImpl> getActiveMembers() {
        if (membershipChangeContext == null) {
            return activeMembers;
        }
        List<RaftMemberImpl> active = new ArrayList<RaftMemberImpl>(activeMembers);
        active.remove(membershipChangeContext.getLeavingMember());
        return active;
    }

    public void setActiveMembers(Collection<RaftMemberImpl> members) {
        if (!discoveryCompleted) {
            logger.fine("Ignore received active members " + members + ", discovery is in progress.");
            return;
        }
        checkNotNull(members);
        checkTrue(members.size() > 1, "active members must contain at least 2 members: " + members);
        checkState(getLocalMember() == null, "This node is already part of Raft members!");

        logger.fine("Setting active members to " + members);
        activeMembers = unmodifiableCollection(new LinkedHashSet<RaftMemberImpl>(members));
        updateInvocationManagerMembers(members);
    }

    private void updateInvocationManagerMembers(Collection<RaftMemberImpl> members) {
        raftService.getInvocationManager().setAllMembers(members);
    }

    public Collection<RaftGroupId> getDestroyingRaftGroupIds() {
        Collection<RaftGroupId> groupIds = new ArrayList<RaftGroupId>();
        for (RaftGroupInfo group : groups.values()) {
            if (group.status() == DESTROYING) {
                groupIds.add(group.id());
            }
        }
        return groupIds;
    }

    public MembershipChangeContext getMembershipChangeContext() {
        return membershipChangeContext;
    }

    boolean isMetadataLeader() {
        RaftMemberImpl member = localMember.get();
        if (member == null) {
            return false;
        }
        RaftNode raftNode = raftService.getRaftNode(METADATA_GROUP_ID);
        // even if the local leader information is stale, it is fine.
        return raftNode != null && !raftNode.isTerminatedOrSteppedDown() && member.equals(raftNode.getLeader());
    }

    /**
     * this method is idempotent
     */
    public void addActiveMember(RaftMemberImpl member) {
        checkNotNull(member);
        checkIfMetadataRaftGroupInitialized();

        if (activeMembers.contains(member)) {
            logger.fine(member + " already exists. Silently returning from addActiveMember().");
            return;
        }
        if (membershipChangeContext != null && member.equals(membershipChangeContext.getLeavingMember())) {
            throw new IllegalArgumentException(member + " is already being removed!");
        }
        Collection<RaftMemberImpl> newMembers = new LinkedHashSet<RaftMemberImpl>(activeMembers);
        newMembers.add(member);
        activeMembers = unmodifiableCollection(newMembers);
        updateInvocationManagerMembers(newMembers);
        broadcastActiveMembers();
        logger.info("Added " + member + ". Active members are " + newMembers);
    }

    private void removeActiveMember(RaftMemberImpl member) {
        Collection<RaftMemberImpl> newMembers = new LinkedHashSet<RaftMemberImpl>(activeMembers);
        newMembers.remove(member);
        activeMembers = unmodifiableCollection(newMembers);
        updateInvocationManagerMembers(newMembers);
        broadcastActiveMembers();
    }

    private void checkIfMetadataRaftGroupInitialized() {
        if (!groups.containsKey(METADATA_GROUP_ID)) {
            throw new MetadataRaftGroupNotInitializedException();
        }
    }

    void broadcastActiveMembers() {
        if (localMember.get() == null) {
            return;
        }
        Collection<RaftMemberImpl> members = activeMembers;
        if (members.isEmpty()) {
            return;
        }

        Set<Address> addresses = new HashSet<Address>(members.size());
        for (RaftMemberImpl member : members) {
            addresses.add(member.getAddress());
        }

        Set<Member> clusterMembers = nodeEngine.getClusterService().getMembers();
        OperationService operationService = nodeEngine.getOperationService();
        Operation op = new SendActiveRaftMembersOp(getActiveMembers());
        for (Member member : clusterMembers) {
            if (addresses.contains(member.getAddress())) {
                continue;
            }
            operationService.send(op, member.getAddress());
        }
    }

    public void disableDiscovery() {
        logger.info("Initial discovery is already completed. Disabling discovery...");
        localMember.set(null);
        discoveryCompleted = true;
    }

    public boolean isDiscoveryCompleted() {
        return discoveryCompleted;
    }

    private class BroadcastActiveRaftMembersTask implements Runnable {
        @Override
        public void run() {
            if (!isMetadataLeader()) {
                return;
            }
            broadcastActiveMembers();
        }
    }

    private class DiscoverInitialRaftMembersTask implements Runnable {

        private Collection<Member> latestMembers = Collections.emptySet();

        @Override
        public void run() {
            if (discoveryCompleted) {
                return;
            }
            Collection<Member> members = nodeEngine.getClusterService().getMembers();
            for (Member member : latestMembers) {
                if (!members.contains(member)) {
                    logger.severe(member + " is removed while discovering initial Raft members!");
                    terminateNode();
                    return;
                }
            }
            latestMembers = members;

            if (members.size() < config.getGroupSize()) {
                logger.warning("Waiting for " + config.getGroupSize() + " Raft members to join the cluster. "
                        + "Current Raft members count: " + members.size());
                ExecutionService executionService = nodeEngine.getExecutionService();
                executionService.schedule(this, DISCOVER_INITIAL_RAFT_MEMBERS_TASK_DELAY_MILLIS, MILLISECONDS);
                return;
            }

            List<RaftMemberImpl> raftMembers = getInitialRaftMembers(members);
            logger.fine("Initial Raft members: " + raftMembers);
            if (!raftMembers.contains(localMember.get())) {
                logger.info("I am not one of initial Raft members! I'll serve as an AP member. Raft members: " + raftMembers);
                disableDiscovery();
                return;
            }

            activeMembers = unmodifiableCollection(new LinkedHashSet<RaftMemberImpl>(raftMembers));
            updateInvocationManagerMembers(activeMembers);

            if (!commitInitialMetadataRaftGroup(raftMembers)) {
                terminateNode();
                return;
            }

            broadcastActiveMembers();
            scheduleManagementTasks();
            logger.info("Raft members: " + activeMembers + ", local: " + localMember.get());
            discoveryCompleted = true;
        }

        @SuppressWarnings("unchecked")
        private boolean commitInitialMetadataRaftGroup(List<RaftMemberImpl> initialRaftMembers) {
            int metadataGroupSize = config.getMetadataGroupSize();
            List<RaftMemberImpl> metadataMembers = initialRaftMembers.subList(0, metadataGroupSize);
            try {
                if (metadataMembers.contains(localMember.get())) {
                    raftService.createRaftNode(METADATA_GROUP_ID, (Collection) metadataMembers);
                }

                RaftOp op = new CreateMetadataRaftGroupOp(initialRaftMembers, metadataGroupSize);
                raftService.getInvocationManager().invoke(METADATA_GROUP_ID, op).get();
                logger.info("METADATA Raft group is created with " + metadataMembers);
            } catch (Exception e) {
                logger.severe("Could not create METADATA Raft group with " + metadataMembers, e);
                return false;
            }
            return true;
        }

        private void terminateNode() {
            ((NodeEngineImpl) nodeEngine).getNode().shutdown(true);
        }

        private List<RaftMemberImpl> getInitialRaftMembers(Collection<Member> members) {
            assert members.size() >= config.getGroupSize();
            List<Member> memberList = new ArrayList<Member>(members).subList(0, config.getGroupSize());
            List<RaftMemberImpl> raftMembers = new ArrayList<RaftMemberImpl>(config.getGroupSize());
            for (Member member : memberList) {
                RaftMemberImpl raftMember = new RaftMemberImpl(member);
                raftMembers.add(raftMember);
            }

            sort(raftMembers, new RaftMemberComparator());
            return raftMembers;
        }
    }

    private static class RaftMemberComparator implements Comparator<RaftMemberImpl> {
        @Override
        public int compare(RaftMemberImpl e1, RaftMemberImpl e2) {
            return e1.getUid().compareTo(e2.getUid());
        }
    }
}
