package com.hazelcast.raft.impl;

import com.hazelcast.config.raft.RaftAlgorithmConfig;
import com.hazelcast.raft.exception.CannotReplicateException;
import com.hazelcast.raft.exception.RaftGroupTerminatedException;
import com.hazelcast.raft.impl.dto.AppendRequest;
import com.hazelcast.raft.impl.service.ApplyRaftRunnable;
import com.hazelcast.raft.impl.testing.LocalRaftGroup;
import com.hazelcast.raft.command.TerminateRaftGroupCmd;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.concurrent.ExecutionException;

import static com.hazelcast.raft.impl.RaftUtil.getCommitIndex;
import static com.hazelcast.raft.impl.RaftUtil.getLeaderMember;
import static com.hazelcast.raft.impl.RaftUtil.getStatus;
import static com.hazelcast.raft.impl.RaftUtil.newGroupWithService;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;

@RunWith(HazelcastSerialClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class TerminateRaftGroupTest extends HazelcastTestSupport {

    private LocalRaftGroup group;

    @Before
    public void init() {
    }

    @After
    public void destroy() {
        if (group != null) {
            group.destroy();
        }
    }

    @Test
    public void when_terminateOpIsAppendedButNotCommitted_then_cannotAppendNewEntry() throws ExecutionException, InterruptedException {
        group = newGroupWithService(2, new RaftAlgorithmConfig());
        group.start();

        RaftNodeImpl leader = group.waitUntilLeaderElected();
        RaftNodeImpl follower = group.getAnyFollowerNode();

        group.dropAllMessagesToMember(leader.getLocalMember(), follower.getLocalMember());

        leader.replicate(new TerminateRaftGroupCmd());

        try {
            leader.replicate(new ApplyRaftRunnable("val")).get();
            fail();
        } catch (CannotReplicateException ignored) {
        }
    }

    @Test
    public void when_terminateOpIsAppended_then_statusIsTerminating() {
        group = newGroupWithService(2, new RaftAlgorithmConfig());
        group.start();

        final RaftNodeImpl leader = group.waitUntilLeaderElected();
        final RaftNodeImpl follower = group.getAnyFollowerNode();

        group.dropAllMessagesToMember(follower.getLocalMember(), leader.getLocalMember());

        leader.replicate(new TerminateRaftGroupCmd());

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                assertEquals(RaftNodeStatus.TERMINATING, getStatus(leader));
                assertEquals(RaftNodeStatus.TERMINATING, getStatus(follower));
            }
        });
    }

    @Test
    public void when_terminateOpIsCommitted_then_raftNodeIsTerminated() throws ExecutionException, InterruptedException {
        group = newGroupWithService(2, new RaftAlgorithmConfig());
        group.start();

        final RaftNodeImpl leader = group.waitUntilLeaderElected();
        final RaftNodeImpl follower = group.getAnyFollowerNode();

        leader.replicate(new TerminateRaftGroupCmd()).get();

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                assertEquals(1, getCommitIndex(leader));
                assertEquals(1, getCommitIndex(follower));
                assertEquals(RaftNodeStatus.TERMINATED, getStatus(leader));
                assertEquals(RaftNodeStatus.TERMINATED, getStatus(follower));
            }
        });

        try {
            leader.replicate(new ApplyRaftRunnable("val")).get();
            fail();
        } catch (RaftGroupTerminatedException ignored) {

        }

        try {
            follower.replicate(new ApplyRaftRunnable("val")).get();
            fail();
        } catch (RaftGroupTerminatedException ignored) {
        }
    }

    @Test
    public void when_terminateOpIsTruncated_then_statusIsActive() throws ExecutionException, InterruptedException {
        group = newGroupWithService(3, new RaftAlgorithmConfig());
        group.start();

        final RaftNodeImpl leader = group.waitUntilLeaderElected();
        final RaftNodeImpl[] followers = group.getNodesExcept(leader.getLocalMember());

        group.dropMessagesToAll(leader.getLocalMember(), AppendRequest.class);

        leader.replicate(new TerminateRaftGroupCmd());

        group.split(leader.getLocalMember());

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                for (RaftNodeImpl raftNode : followers) {
                    RaftMember leaderEndpoint = getLeaderMember(raftNode);
                    assertNotNull(leaderEndpoint);
                    assertNotEquals(leader.getLocalMember(), leaderEndpoint);
                }
            }
        });

        final RaftNodeImpl newLeader = group.getNode(getLeaderMember(followers[0]));

        for (int i = 0; i < 10; i++) {
            newLeader.replicate(new ApplyRaftRunnable("val" + i)).get();
        }

        group.merge();

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                for (RaftNodeImpl raftNode : group.getNodes()) {
                    assertEquals(RaftNodeStatus.ACTIVE, getStatus(raftNode));
                }
            }
        });
    }

}
