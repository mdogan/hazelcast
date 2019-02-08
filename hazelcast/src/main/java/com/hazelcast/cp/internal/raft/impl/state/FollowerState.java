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

package com.hazelcast.cp.internal.raft.impl.state;

/**
 * Mutable state maintained by the leader of the Raft group for each follower.
 * In follower state, three variables are stored:
 * <ul>
 * <li>{@code nextIndex}: index of the next log entry to send to that server
 * (initialized to leader's {@code lastLogIndex + 1})</li>
 * <li>{@code matchIndex}: index of highest log entry known to be replicated
 * on server (initialized to 0, increases monotonically)</li>
 * <li>{@code appendRequestBackoff}: a boolean flag indicating that leader is still
 * waiting for a response to the last sent append request</li>
 * </ul>
 */
public class FollowerState {

    private long matchIndex;

    private long nextIndex;

    private int backoffRound;

    private int nextBackoffRound = 1;

    FollowerState(long matchIndex, long nextIndex) {
        this.matchIndex = matchIndex;
        this.nextIndex = nextIndex;
    }

    /**
     * Returns the match index for follower.
     */
    public long matchIndex() {
        return matchIndex;
    }

    /**
     * Sets the match index for follower.
     */
    public void matchIndex(long matchIndex) {
        this.matchIndex = matchIndex;
    }

    /**
     * Returns the next index for follower.
     */
    public long nextIndex() {
        return nextIndex;
    }

    /**
     * Sets the next index for follower.
     */
    public void nextIndex(long nextIndex) {
        this.nextIndex = nextIndex;
    }

    /**
     * Returns whether leader is waiting for response of the last append request.
     */
    public boolean isAppendRequestBackoffSet() {
        return backoffRound > 0;
    }

    /**
     * Sets the flag for append request backoff. A new append request will not be sent
     * to this follower either until it sends an append response or a backoff timeout occurs.
     */
    public void setAppendRequestBackoff() {
        backoffRound = nextBackoffRound;
        nextBackoffRound = 1 << nextBackoffRound;
    }

    /**
     * Completes a single round of append request backoff.
     *
     * @return true if round number reaches to {@code 0}, false otherwise
     */
    public boolean completeAppendRequestBackoffRound() {
        return --backoffRound == 0;
    }

    /**
     * Clears the flag for the append request backoff.
     */
    public void resetAppendRequestBackoff() {
        backoffRound = 0;
        nextBackoffRound = 1;
    }

    @Override
    public String toString() {
        return "FollowerState{" + "matchIndex=" + matchIndex + ", nextIndex=" + nextIndex + ", backoffRound=" + backoffRound
                + ", nextBackoffRound=" + nextBackoffRound + '}';
    }
}
