package com.hazelcast.raft.impl.service;

import com.hazelcast.core.HazelcastException;
import com.hazelcast.internal.util.RuntimeAvailableProcessors;
import com.hazelcast.logging.ILogger;
import com.hazelcast.nio.Address;
import com.hazelcast.raft.RaftConfig;
import com.hazelcast.raft.RaftMember;
import com.hazelcast.raft.SnapshotAwareService;
import com.hazelcast.raft.impl.RaftEndpoint;
import com.hazelcast.raft.impl.RaftIntegration;
import com.hazelcast.raft.impl.RaftNode;
import com.hazelcast.raft.impl.dto.AppendFailureResponse;
import com.hazelcast.raft.impl.dto.AppendRequest;
import com.hazelcast.raft.impl.dto.AppendSuccessResponse;
import com.hazelcast.raft.impl.dto.InstallSnapshot;
import com.hazelcast.raft.impl.dto.PreVoteRequest;
import com.hazelcast.raft.impl.dto.PreVoteResponse;
import com.hazelcast.raft.impl.dto.VoteRequest;
import com.hazelcast.raft.impl.dto.VoteResponse;
import com.hazelcast.spi.ConfigurableService;
import com.hazelcast.spi.ManagedService;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.util.AddressUtil;
import com.hazelcast.util.executor.StripedExecutor;

import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import static com.hazelcast.util.ThreadUtil.createThreadName;

/**
 * TODO: Javadoc Pending...
 */
public class RaftService implements ManagedService, ConfigurableService<RaftConfig>,
        SnapshotAwareService<Collection<RaftGroupInfo>> {

    public static final String SERVICE_NAME = "hz:core:raft";
    private static final String METADATA_RAFT = "METADATA";
    public static final RaftGroupId METADATA_GROUP_ID = new RaftGroupId(METADATA_RAFT, 0);

    private final Map<String, RaftGroupInfo> raftGroups = new ConcurrentHashMap<String, RaftGroupInfo>();
    private final Map<RaftGroupId, RaftNode> nodes = new ConcurrentHashMap<RaftGroupId, RaftNode>();
    private final ConcurrentMap<String, RaftEndpoint> knownLeaders = new ConcurrentHashMap<String, RaftEndpoint>();
    private final NodeEngine nodeEngine;
    private final ILogger logger;

    private volatile StripedExecutor executor;
    private volatile RaftConfig config;
    private volatile Collection<RaftEndpoint> endpoints;
    private volatile RaftEndpoint localEndpoint;

    public RaftService(NodeEngine nodeEngine) {
        this.nodeEngine = nodeEngine;
        this.logger = nodeEngine.getLogger(getClass());
    }

    @Override
    public void init(NodeEngine nodeEngine, Properties properties) {
        try {
            endpoints = Collections.unmodifiableCollection(parseEndpoints());
        } catch (UnknownHostException e) {
            throw new HazelcastException(e);
        }
        logger.info("CP nodes: " + endpoints);
        raftGroups.put(METADATA_RAFT, new RaftGroupInfo(SERVICE_NAME, METADATA_RAFT, endpoints, 0));

        localEndpoint = findLocalEndpoint(endpoints);
        if (localEndpoint == null) {
            logger.warning("We are not in CP nodes group :(");
            return;
        }

        String threadPoolName = createThreadName(nodeEngine.getHazelcastInstance().getName(), "raft");
        this.executor = new StripedExecutor(logger, threadPoolName, RuntimeAvailableProcessors.get(), Integer.MAX_VALUE);

        RaftIntegration raftIntegration = new NodeEngineRaftIntegration(nodeEngine, METADATA_GROUP_ID);
        RaftNode node = new RaftNode(SERVICE_NAME, METADATA_RAFT, localEndpoint, endpoints, config, raftIntegration, executor);
        nodes.put(METADATA_GROUP_ID, node);
        node.start();
    }

    private RaftEndpoint findLocalEndpoint(Collection<RaftEndpoint> endpoints) {
        for (RaftEndpoint endpoint : endpoints) {
            if (nodeEngine.getThisAddress().equals(endpoint.getAddress())) {
                return endpoint;
            }
        }
        return null;
    }

    private Collection<RaftEndpoint> parseEndpoints() throws UnknownHostException {
        Collection<RaftMember> members = config.getMembers();
        Set<RaftEndpoint> endpoints = new HashSet<RaftEndpoint>(members.size());
        for (RaftMember member : members) {
            AddressUtil.AddressHolder addressHolder = AddressUtil.getAddressHolder(member.getAddress());
            Address address = new Address(addressHolder.getAddress(), addressHolder.getPort());
            address.setScopeId(addressHolder.getScopeId());
            endpoints.add(new RaftEndpoint(member.getId(), address));
        }
        return endpoints;
    }

    @Override
    public void reset() {
    }

    @Override
    public void shutdown(boolean terminate) {
        if (executor != null) {
            executor.shutdown();
            executor = null;
        }
    }

    @Override
    public void configure(RaftConfig config) {
        // cloning given RaftConfig to avoid further mutations
        this.config = new RaftConfig(config);
    }

    @Override
    public Collection<RaftGroupInfo> takeSnapshot(String raftName, int commitIndex) {
        assert METADATA_RAFT.equals(raftName);

        Collection<RaftGroupInfo> groupInfos = new ArrayList<RaftGroupInfo>();
        for (RaftGroupInfo groupInfo : raftGroups.values()) {
            assert groupInfo.commitIndex() <= commitIndex
                    : "Group commit index: " + groupInfo.commitIndex() + ", snapshot commit index: " + commitIndex;
            if (!METADATA_RAFT.equals(groupInfo.name())) {
                groupInfos.add(groupInfo);
            }
        }
        return groupInfos;
    }

    @Override
    public void restoreSnapshot(String raftName, int commitIndex, Collection<RaftGroupInfo> snapshot) {
        assert METADATA_RAFT.equals(raftName);

        for (RaftGroupInfo groupInfo : snapshot) {
            if (!raftGroups.containsKey(groupInfo.name())) {
                createRaftGroup(groupInfo.serviceName(), groupInfo.name(), groupInfo.members(), groupInfo.commitIndex());
            }
        }
    }

    public void handlePreVoteRequest(RaftGroupId groupId, PreVoteRequest request) {
        RaftNode node = nodes.get(groupId);
        if (node == null) {
            logger.severe("RaftNode[" + groupId.name() + "] does not exist to handle: " + request);
            return;
        }
        node.handlePreVoteRequest(request);
    }

    public void handlePreVoteResponse(RaftGroupId groupId, PreVoteResponse response) {
        RaftNode node = nodes.get(groupId);
        if (node == null) {
            logger.severe("RaftNode[" + groupId.name() + "] does not exist to handle: " + response);
            return;
        }
        node.handlePreVoteResponse(response);
    }

    public void handleVoteRequest(RaftGroupId groupId, VoteRequest request) {
        RaftNode node = nodes.get(groupId);
        if (node == null) {
            logger.severe("RaftNode[" + groupId.name() + "] does not exist to handle: " + request);
            return;
        }
        node.handleVoteRequest(request);
    }

    public void handleVoteResponse(RaftGroupId groupId, VoteResponse response) {
        RaftNode node = nodes.get(groupId);
        if (node == null) {
            logger.severe("RaftNode[" + groupId.name() + "] does not exist to handle: " + response);
            return;
        }
        node.handleVoteResponse(response);
    }

    public void handleAppendEntries(RaftGroupId groupId, AppendRequest request) {
        RaftNode node = nodes.get(groupId);
        if (node == null) {
            logger.severe("RaftNode[" + groupId.name() + "] does not exist to handle: " + request);
            return;
        }
        node.handleAppendRequest(request);
    }

    public void handleAppendResponse(RaftGroupId groupId, AppendSuccessResponse response) {
        RaftNode node = nodes.get(groupId);
        if (node == null) {
            logger.severe("RaftNode[" + groupId.name() + "] does not exist to handle: " + response);
            return;
        }
        node.handleAppendResponse(response);
    }

    public void handleAppendResponse(RaftGroupId groupId, AppendFailureResponse response) {
        RaftNode node = nodes.get(groupId);
        if (node == null) {
            logger.severe("RaftNode[" + groupId.name() + "] does not exist to handle: " + response);
            return;
        }
        node.handleAppendResponse(response);
    }

    public void handleSnapshot(RaftGroupId groupId, InstallSnapshot request) {
        RaftNode node = nodes.get(groupId);
        if (node == null) {
            logger.severe("RaftNode[" + groupId.name() + "] does not exist to handle: " + request);
            return;
        }
        node.handleInstallSnapshot(request);
    }


    public ILogger getLogger(Class clazz, String raftName) {
        return nodeEngine.getLogger(clazz.getName() + "(" + raftName + ")");
    }

    public RaftNode getRaftNode(RaftGroupId groupId) {
        return nodes.get(groupId);
    }

    public RaftGroupInfo getRaftGroupInfo(String name) {
        return raftGroups.get(name);
    }

    public RaftConfig getConfig() {
        return config;
    }

    public Collection<RaftEndpoint> getAllEndpoints() {
        return endpoints;
    }

    public RaftEndpoint getLocalEndpoint() {
        return localEndpoint;
    }

    RaftGroupId createRaftGroup(String serviceName, String name, Collection<RaftEndpoint> endpoints, int commitIndex) {
        // keep configuration on every metadata node
        RaftGroupInfo groupInfo = raftGroups.get(name);
        if (groupInfo != null) {
            if (groupInfo.members().size() == endpoints.size()) {
                logger.warning("Raft group " + name + " already exists. Ignoring add raft node request.");
                return new RaftGroupId(name, groupInfo.commitIndex());
            }
            throw new IllegalStateException("Raft group " + name
                    + " already exists with different group size. Ignoring add raft node request.");
        }

        raftGroups.put(name, new RaftGroupInfo(serviceName, name, endpoints, commitIndex));

        RaftGroupId groupId = new RaftGroupId(name, commitIndex);
        if (!endpoints.contains(localEndpoint)) {
            return groupId;
        }

        assert nodes.get(groupId) == null : "Raft node with name " + name + " should not exist!";

        RaftIntegration raftIntegration = new NodeEngineRaftIntegration(nodeEngine, groupId);
        RaftNode node = new RaftNode(serviceName, name, localEndpoint, endpoints, config, raftIntegration, executor);
        nodes.put(groupId, node);
        node.start();
        return groupId;
    }

    void resetKnownLeader(String raftName) {
        logger.fine("Resetting known leader for raft: " + raftName);
        knownLeaders.remove(raftName);
    }

    void setKnownLeader(String raftName, RaftEndpoint leader) {
        logger.fine("Setting known leader for raft: " + raftName + " to " + leader);
        knownLeaders.put(raftName, leader);
    }

    RaftEndpoint getKnownLeader(String raftName) {
        return knownLeaders.get(raftName);
    }
}