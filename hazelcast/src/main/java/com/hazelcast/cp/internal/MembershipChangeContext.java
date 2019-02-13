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

import com.hazelcast.cp.CPGroupId;
import com.hazelcast.cp.internal.raft.MembershipChangeMode;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;

import static com.hazelcast.util.Preconditions.checkNotNull;
import static java.util.Collections.unmodifiableList;

/**
 * When there is a membership change in the CP subsystem,
 * all decided membership changes of Raft groups are maintained here.
 * {@link RaftGroupMembershipManager} realizes these membership changes.
 *
 * This class is IMMUTABLE because it can be returned as a response to
 * local queries of {@link RaftGroupMembershipManager}
 */
public class MembershipChangeContext implements IdentifiedDataSerializable {

    private List<Long> membershipChangeCommitIndices;
    private CPMemberInfo member;
    private MembershipChangeMode membershipChangeMode;
    private final List<CPGroupMembershipChangeContext> changes = new ArrayList<CPGroupMembershipChangeContext>();

    MembershipChangeContext() {
    }

    private MembershipChangeContext(List<Long> membershipChangeCommitIndices, CPMemberInfo member,
                                    MembershipChangeMode membershipChangeMode, List<CPGroupMembershipChangeContext> changes) {
        this.membershipChangeCommitIndices = membershipChangeCommitIndices;
        this.member = member;
        this.membershipChangeMode = membershipChangeMode;
        this.changes.addAll(changes);
    }

    CPMemberInfo getAddedMember() {
        return membershipChangeMode == MembershipChangeMode.ADD ? member : null;
    }

    CPMemberInfo getLeavingMember() {
        return membershipChangeMode == MembershipChangeMode.REMOVE ? member : null;
    }

    List<CPGroupMembershipChangeContext> getChanges() {
        return unmodifiableList(changes);
    }

    MembershipChangeContext excludeCompletedChanges(Collection<CPGroupId> completedGroupIds) {
        checkNotNull(completedGroupIds);

        List<CPGroupMembershipChangeContext> remainingChanges = new ArrayList<CPGroupMembershipChangeContext>(changes);
        Iterator<CPGroupMembershipChangeContext> it = remainingChanges.iterator();
        while (it.hasNext()) {
            CPGroupMembershipChangeContext ctx = it.next();
            if (completedGroupIds.contains(ctx.groupId)) {
                it.remove();
            }
        }

        return new MembershipChangeContext(membershipChangeCommitIndices, member, membershipChangeMode, remainingChanges);
    }

    List<Long> getMembershipChangeCommitIndices() {
        return membershipChangeCommitIndices;
    }

    MembershipChangeContext addRetriedCommitIndex(long commitIndex) {
        List<Long> membershipChangeCommitIndices = new ArrayList<Long>(this.membershipChangeCommitIndices);
        membershipChangeCommitIndices.add(commitIndex);
        return new MembershipChangeContext(membershipChangeCommitIndices, member, membershipChangeMode, changes);
    }

    static MembershipChangeContext memberAdded(List<Long> membershipChangeCommitIndices, CPMemberInfo member,
                                               List<CPGroupMembershipChangeContext> changes) {
        return new MembershipChangeContext(membershipChangeCommitIndices, member, MembershipChangeMode.ADD, changes);
    }

    static MembershipChangeContext memberLeaving(List<Long> membershipChangeCommitIndices, CPMemberInfo member,
                                                 List<CPGroupMembershipChangeContext> changes) {
        return new MembershipChangeContext(membershipChangeCommitIndices, member, MembershipChangeMode.REMOVE, changes);
    }

    /**
     * Contains a membership change that will be performed on a CP group
     */
    public static class CPGroupMembershipChangeContext implements IdentifiedDataSerializable {

        private CPGroupId groupId;

        private long membersCommitIndex;

        private Collection<CPMemberInfo> members;

        private CPMemberInfo memberToAdd;

        private CPMemberInfo memberToRemove;

        CPGroupMembershipChangeContext() {
        }

        CPGroupMembershipChangeContext(CPGroupId groupId, long membersCommitIndex, Collection<CPMemberInfo> members,
                                       CPMemberInfo memberToAdd, CPMemberInfo memberToRemove) {
            this.groupId = groupId;
            this.membersCommitIndex = membersCommitIndex;
            this.members = members;
            this.memberToAdd = memberToAdd;
            this.memberToRemove = memberToRemove;
        }

        CPGroupId getGroupId() {
            return groupId;
        }

        long getMembersCommitIndex() {
            return membersCommitIndex;
        }

        Collection<CPMemberInfo> getMembers() {
            return members;
        }

        CPMemberInfo getMemberToAdd() {
            return memberToAdd;
        }

        CPMemberInfo getMemberToRemove() {
            return memberToRemove;
        }

        @Override
        public void writeData(ObjectDataOutput out) throws IOException {
            out.writeLong(membersCommitIndex);
            out.writeInt(members.size());
            for (CPMemberInfo member : members) {
                out.writeObject(member);
            }
            out.writeObject(memberToAdd);
            out.writeObject(memberToRemove);
        }

        @Override
        public void readData(ObjectDataInput in) throws IOException {
            membersCommitIndex = in.readLong();
            int len = in.readInt();
            members = new HashSet<CPMemberInfo>(len);
            for (int i = 0; i < len; i++) {
                CPMemberInfo member = in.readObject();
                members.add(member);
            }
            memberToAdd = in.readObject();
            memberToRemove = in.readObject();
        }

        @Override
        public int getFactoryId() {
            return RaftServiceDataSerializerHook.F_ID;
        }

        @Override
        public int getId() {
            return RaftServiceDataSerializerHook.GROUP_MEMBERSHIP_CHANGE_CTX;
        }

        @Override
        public String toString() {
            return "CPGroupMembershipChangeContext{" + "groupId=" + groupId + ", membersCommitIndex=" + membersCommitIndex
                    + ", members=" + members + ", memberToAdd=" + memberToAdd + ", memberToRemove=" + memberToRemove + '}';
        }
    }

    @Override
    public int getFactoryId() {
        return RaftServiceDataSerializerHook.F_ID;
    }

    @Override
    public int getId() {
        return RaftServiceDataSerializerHook.MEMBERSHIP_CHANGE_CTX;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeInt(membershipChangeCommitIndices.size());
        for (long commitIndex : membershipChangeCommitIndices) {
            out.writeLong(commitIndex);
        }
        out.writeObject(member);
        out.writeUTF(membershipChangeMode.name());
        out.writeInt(changes.size());
        for (CPGroupMembershipChangeContext ctx : changes) {
            ctx.writeData(out);
        }
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        int membershipChangeCommitIndexCount = in.readInt();
        membershipChangeCommitIndices = new ArrayList<Long>(membershipChangeCommitIndexCount);
        for (int i = 0; i < membershipChangeCommitIndexCount; i++) {
            long commitIndex = in.readLong();
            membershipChangeCommitIndices.add(commitIndex);
        }
        member = in.readObject();
        membershipChangeMode = MembershipChangeMode.valueOf(in.readUTF());
        int groupCount = in.readInt();
        for (int i = 0; i < groupCount; i++) {
            CPGroupMembershipChangeContext context = new CPGroupMembershipChangeContext();
            context.readData(in);
            changes.add(context);
        }
    }

    @Override
    public String toString() {
        return "MembershipChangeContext{" + "membershipChangeCommitIndices=" + membershipChangeCommitIndices + ", member="
                + member + ", membershipChangeMode=" + membershipChangeMode + ", changes=" + changes + '}';
    }
}
