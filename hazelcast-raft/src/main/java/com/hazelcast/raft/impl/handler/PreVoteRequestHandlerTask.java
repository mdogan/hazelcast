package com.hazelcast.raft.impl.handler;

import com.hazelcast.raft.impl.RaftMember;
import com.hazelcast.raft.impl.RaftNodeImpl;
import com.hazelcast.raft.impl.dto.PreVoteRequest;
import com.hazelcast.raft.impl.dto.PreVoteResponse;
import com.hazelcast.raft.impl.log.RaftLog;
import com.hazelcast.raft.impl.state.RaftState;
import com.hazelcast.raft.impl.task.RaftNodeStatusAwareTask;
import com.hazelcast.util.Clock;

/**
 * Handles {@link PreVoteRequest} and responds to the sender with a {@link PreVoteResponse}.
 * Pre-voting is initiated by {@link com.hazelcast.raft.impl.task.PreVoteTask}.
 * <p>
 * Grants vote or rejects the request as if responding to a {@link com.hazelcast.raft.impl.dto.VoteRequest}
 * but differently Raft state is not mutated/updated, this task is completely read-only.
 *
 * @see PreVoteRequest
 * @see PreVoteResponse
 * @see com.hazelcast.raft.impl.task.PreVoteTask
 */
public class PreVoteRequestHandlerTask extends RaftNodeStatusAwareTask implements Runnable {
    private final PreVoteRequest req;

    public PreVoteRequestHandlerTask(RaftNodeImpl raftNode, PreVoteRequest req) {
        super(raftNode);
        this.req = req;
    }

    @Override
    protected void innerRun() {
        RaftState state = raftNode.state();
        RaftMember localEndpoint = raftNode.getLocalMember();

        // Reply false if term < currentTerm (§5.1)
        if (state.term() > req.nextTerm()) {
            logger.info("Rejecting " + req + " since current term: " + state.term() + " is bigger");
            raftNode.send(new PreVoteResponse(localEndpoint, state.term(), false), req.candidate());
            return;
        }

        // Reply false if last AppendEntries call was received less than election timeout ago (leader stickiness)
        if (raftNode.lastAppendEntriesTimestamp() > Clock.currentTimeMillis() - raftNode.getLeaderElectionTimeoutInMillis()) {
            logger.info("Rejecting " + req + " since received append entries recently.");
            raftNode.send(new PreVoteResponse(localEndpoint, state.term(), false), req.candidate());
            return;
        }

        RaftLog raftLog = state.log();
        if (raftLog.lastLogOrSnapshotTerm() > req.lastLogTerm()) {
            logger.info("Rejecting " + req + " since our last log term: " + raftLog.lastLogOrSnapshotTerm() + " is greater");
            raftNode.send(new PreVoteResponse(localEndpoint, req.nextTerm(), false), req.candidate());
            return;
        }

        if (raftLog.lastLogOrSnapshotTerm() == req.lastLogTerm() && raftLog.lastLogOrSnapshotIndex() > req.lastLogIndex()) {
            logger.info("Rejecting " + req + " since our last log index: " + raftLog.lastLogOrSnapshotIndex() + " is greater");
            raftNode.send(new PreVoteResponse(localEndpoint, req.nextTerm(), false), req.candidate());
            return;
        }

        logger.info("Granted pre-vote for " + req);
        raftNode.send(new PreVoteResponse(localEndpoint, req.nextTerm(), true), req.candidate());
    }
}