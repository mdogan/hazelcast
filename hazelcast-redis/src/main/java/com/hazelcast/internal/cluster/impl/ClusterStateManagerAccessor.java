package com.hazelcast.internal.cluster.impl;

import com.hazelcast.cluster.ClusterState;
import com.hazelcast.instance.MemberImpl;
import com.hazelcast.version.Version;

import java.util.Collection;

/**
 * Accessor used to invoke package-private
 * methods on ClusterStateManager in a safe manner.
 * <p>
 * Normally these methods are not allowed to be called,
 * they are not required for ClusterState logic.
 * <p>
 * Hot Restart needs to set {@link ClusterState} and cluster's {@link Version} directly
 * without a cluster-wide transaction, it's already managing a consensus of its own.
 */
public final class ClusterStateManagerAccessor {

    private ClusterStateManagerAccessor() {
    }

    public static void setClusterState(ClusterServiceImpl clusterService, ClusterState newState, boolean isTransient) {
        clusterService.getClusterStateManager().setClusterState(newState, isTransient);
    }

    public static void setClusterVersion(ClusterServiceImpl clusterService, Version newVersion) {
        clusterService.getClusterStateManager().setClusterVersion(newVersion);
    }

    public static void setMissingMembers(ClusterServiceImpl clusterService, Collection<MemberImpl> members) {
        clusterService.getMembershipManager().setMissingMembers(members);
    }
}
