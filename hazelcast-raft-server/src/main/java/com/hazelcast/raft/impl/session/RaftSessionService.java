/*
 *  Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.raft.impl.session;

import com.hazelcast.core.ICompletableFuture;
import com.hazelcast.logging.ILogger;
import com.hazelcast.raft.RaftGroupId;
import com.hazelcast.raft.SnapshotAwareService;
import com.hazelcast.raft.impl.RaftNode;
import com.hazelcast.raft.impl.service.RaftService;
import com.hazelcast.raft.impl.service.TermChangeAwareService;
import com.hazelcast.raft.impl.session.operation.CloseSessionsOp;
import com.hazelcast.spi.ExecutionService;
import com.hazelcast.spi.ManagedService;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.util.Clock;

import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;

/**
 * TODO: Javadoc Pending...
 */
public class RaftSessionService
        implements ManagedService, SnapshotAwareService<SessionRegistrySnapshot>, SessionValidator, TermChangeAwareService {

    public static String SERVICE_NAME = "hz:core:raftSession";
    private static final long CHECK_EXPIRED_SESSIONS_TASK_PERIOD_IN_MILLIS = SECONDS.toMillis(1);

    private final NodeEngine nodeEngine;
    private final ILogger logger;
    private volatile RaftService raftService;

    private final Map<RaftGroupId, SessionRegistry> registries = new ConcurrentHashMap<RaftGroupId, SessionRegistry>();

    public RaftSessionService(NodeEngine nodeEngine) {
        this.nodeEngine = nodeEngine;
        this.logger = nodeEngine.getLogger(getClass());
    }

    @Override
    public void init(NodeEngine nodeEngine, Properties properties) {
        this.raftService = nodeEngine.getService(RaftService.SERVICE_NAME);
        Collection<SessionAwareService> services = nodeEngine.getServices(SessionAwareService.class);
        for (SessionAwareService service : services) {
            service.setSessionValidator(this);
        }
        ExecutionService executionService = nodeEngine.getExecutionService();
        executionService.scheduleWithRepetition(new CheckExpiredSessions(), CHECK_EXPIRED_SESSIONS_TASK_PERIOD_IN_MILLIS,
                CHECK_EXPIRED_SESSIONS_TASK_PERIOD_IN_MILLIS, MILLISECONDS);
    }

    @Override
    public void reset() {
    }

    @Override
    public void shutdown(boolean terminate) {
        registries.clear();
    }

    @Override
    public SessionRegistrySnapshot takeSnapshot(RaftGroupId groupId, long commitIndex) {
        SessionRegistry registry = registries.get(groupId);
        return registry != null ? registry.toSnapshot() : null;
    }

    @Override
    public void restoreSnapshot(RaftGroupId groupId, long commitIndex, SessionRegistrySnapshot snapshot) {
        if (snapshot != null) {
            SessionRegistry registry = new SessionRegistry(groupId, snapshot);
            registries.put(groupId, registry);
        }
    }

    @Override
    public void onNewTermCommit(RaftGroupId groupId) {
        SessionRegistry registry = registries.get(groupId);
        if (registry != null) {
            registry.shiftExpirationTimes(getHeartbeatIntervalMillis());
            logger.info("Session expiration times are shifted in " + groupId);
        }
    }

    public SessionResponse createNewSession(RaftGroupId groupId) {
        SessionRegistry registry = registries.get(groupId);
        if (registry == null) {
            registry = new SessionRegistry(groupId);
            registries.put(groupId, registry);
            logger.info("Created new session registry for " + groupId);
        }

        long sessionTTLMillis = getSessionTimeToLiveMillis();
        long sessionId = registry.createNewSession(sessionTTLMillis);
        logger.info("Created new session: " + sessionId + " in " + groupId);
        return new SessionResponse(sessionId, sessionTTLMillis, getHeartbeatIntervalMillis());
    }

    public void heartbeat(RaftGroupId groupId, long sessionId) {
        SessionRegistry registry = registries.get(groupId);
        if (registry == null) {
            throw new IllegalStateException("No session: " + sessionId + " for raft group: " + groupId);
        }

        registry.heartbeat(sessionId, getSessionTimeToLiveMillis());
        logger.info("Session: " + sessionId + " heartbeat in " + groupId);
    }

    public void closeSessions(RaftGroupId groupId, Collection<Long> sessionIds) {
        SessionRegistry registry = registries.get(groupId);
        if (registry == null) {
            return;
        }

        Iterator<Long> iterator = sessionIds.iterator();
        while (iterator.hasNext()) {
            long sessionId = iterator.next();
            if (!registry.closeSession(sessionId)) {
                iterator.remove();
            }
        }

        logger.info("Sessions: " + sessionIds + " are closed in " + groupId);
        notifyServices(groupId, sessionIds);
    }

    // queried locally in tests
    public SessionRegistry getSessionRegistryOrNull(RaftGroupId groupId) {
        return registries.get(groupId);
    }


    private long getHeartbeatIntervalMillis() {
        return raftService.getConfig().getSessionHeartbeatIntervalMillis();
    }

    // queried locally
    private long getSessionTimeToLiveMillis() {
        return SECONDS.toMillis(raftService.getConfig().getSessionTimeToLiveSeconds());
    }

    private void notifyServices(RaftGroupId groupId, Collection<Long> sessionIds) {
        Collection<SessionAwareService> services = nodeEngine.getServices(SessionAwareService.class);
        for (SessionAwareService sessionAwareService : services) {
            for (long sessionId : sessionIds) {
                sessionAwareService.invalidateSession(groupId, sessionId);
            }
        }
    }

    @Override
    public SessionStatus isValid(RaftGroupId groupId, long sessionId) {
        SessionRegistry sessionRegistry = registries.get(groupId);
        if (sessionRegistry == null) {
            return SessionStatus.UNKNOWN;
        }
        Session session = sessionRegistry.getSession(sessionId);
        if (session == null) {
            return SessionStatus.UNKNOWN;
        }
        return session.isExpired(Clock.currentTimeMillis()) ? SessionStatus.EXPIRED : SessionStatus.VALID;
    }

    private Map<RaftGroupId, Collection<Long>> getExpiredSessions() {
        Map<RaftGroupId, Collection<Long>> expired = new HashMap<RaftGroupId, Collection<Long>>();
        for (SessionRegistry registry : registries.values()) {
            Collection<Long> e = registry.getExpiredSessions();
            if (!e.isEmpty()) {
                expired.put(registry.groupId(), e);
            }
        }

        return expired;
    }

    private class CheckExpiredSessions implements Runnable {

        @Override
        public void run() {
            Map<RaftGroupId, Collection<Long>> expiredSessions = getExpiredSessions();
            for (Entry<RaftGroupId, Collection<Long>> entry : expiredSessions.entrySet()) {
                RaftGroupId groupId = entry.getKey();
                RaftNode raftNode = raftService.getRaftNode(groupId);
                if (raftNode != null) {
                    Collection<Long> sessionIds = entry.getValue();
                    try {
                        ICompletableFuture f = raftNode.replicate(new CloseSessionsOp(sessionIds));
                        f.get();
                    } catch (Exception e) {
                        logger.fine("Could not close sessions: " + sessionIds + " of " + groupId, e);
                    }
                }
            }
        }
    }
}