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

package com.hazelcast.config.raft;

import static com.hazelcast.util.Preconditions.checkTrue;

/**
 * TODO: Javadoc Pending...
 *
 */
public class RaftMetadataGroupConfig {

    /**
     * Size of the Raft members group.
     */
    private int groupSize;

    /**
     * Size of the metadata Raft group. If not specified explicitly, then all Raft members will be in metadata group.
     */
    private int metadataGroupSize;

    public RaftMetadataGroupConfig() {
    }

    public RaftMetadataGroupConfig(RaftMetadataGroupConfig config) {
        this.groupSize = config.groupSize;
        this.metadataGroupSize = config.metadataGroupSize;
    }

    public int getGroupSize() {
        return groupSize;
    }

    public RaftMetadataGroupConfig setGroupSize(int groupSize) {
        checkTrue(groupSize >= 2, "The group must have at least 2 members");
        checkTrue(metadataGroupSize <= groupSize,
                "The metadata group cannot be bigger than the number of raft members");
        this.groupSize = groupSize;
        return this;
    }

    public int getMetadataGroupSize() {
        return metadataGroupSize;
    }

    public RaftMetadataGroupConfig setMetadataGroupSize(int metadataGroupSize) {
        checkTrue(metadataGroupSize >= 2, "The metadata group must have at least 2 members");
        checkTrue(metadataGroupSize <= groupSize,
                "The metadata group cannot be bigger than the number of raft members");
        this.metadataGroupSize = metadataGroupSize;
        return this;
    }
}
