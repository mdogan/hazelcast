package com.hazelcast.redis;

import com.hazelcast.core.HazelcastException;
import com.hazelcast.instance.DefaultNodeExtension;
import com.hazelcast.instance.Node;
import com.hazelcast.internal.partition.InternalPartition;
import com.hazelcast.internal.partition.PartitionReplica;
import com.hazelcast.internal.partition.PartitionTableView;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.IOUtil;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileFilter;
import java.io.FileReader;
import java.io.IOException;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;

/**
 */
public class RedisNodeExtension extends DefaultNodeExtension {

    private static final String REDIS_PATH = System.getProperty("hazelcast.redis.import.path");
    private static final String NODES_CONF = "nodes.conf";
    private static final String DUMP_RDB = "dump.rdb";
    private static final int SLOT_COUNT = 16384;

    private volatile String uuid;

    public RedisNodeExtension(Node node) {

        super(node);

        if (REDIS_PATH == null) {
            logger.warning("Redis import path is null...");
            return;
        }

        File baseDir = new File(REDIS_PATH);
        if (!baseDir.exists() || !baseDir.isDirectory()) {
            logger.warning(REDIS_PATH + " does not exist!");
            return;
        }

        File[] dirs = getRedisDirs(baseDir);
        if (dirs == null) {
            logger.severe(REDIS_PATH + " does not contain any redis dumps");
            return;
        }

        List<String> confParams = parseMyDir(dirs);
        if (confParams == null) {
            logger.severe("No redis config is selected!");
            return;
        }

        try {
            PartitionTableView partitionTable = parseConfigParams(confParams);
            node.partitionService.setInitialState(partitionTable);
        } catch (IOException e) {
            throw new HazelcastException(e);
        }
    }

    private PartitionTableView parseConfigParams(List<String> confParams) throws IOException {
        PartitionReplica[][] replicas = new PartitionReplica[SLOT_COUNT][InternalPartition.MAX_REPLICA_COUNT];
        PartitionTableView partitionTable = new PartitionTableView(replicas, SLOT_COUNT);

        for (String param : confParams) {
            String[] params = param.split("\\s+");
            if (!params[2].contains("master")) {
                continue;
            }

            String uuid = params[0];
            Address address = parseAddress(params[1]);
            String[] slotRange = params[8].split("-");
            int from = Integer.parseInt(slotRange[0]);
            int to = Integer.parseInt(slotRange[1]);

            for (int ix = from; ix <= to; ix++) {
                replicas[ix][0] = new PartitionReplica(address, uuid);
            }
        }
        return partitionTable;
    }

    private File[] getRedisDirs(File baseDir) {
        return baseDir.listFiles(new FileFilter() {
            @Override
            public boolean accept(File f) {
                if (!f.isDirectory()) {
                    return false;
                }
                if (!new File(f, DUMP_RDB).exists()) {
                    return false;
                }
                File nodes = new File(f, NODES_CONF);
                if (!nodes.exists()) {
                    return false;
                }
                return true;
            }
        });
    }

    private List<String> parseNodesConf(File dir) {
        List<String> lines = new ArrayList<String>();
        File nodes = new File(dir, NODES_CONF);
        BufferedReader reader = null;
        try {
            reader = new BufferedReader(new FileReader(nodes));
            String line;
            while ((line = reader.readLine()) != null) {
                lines.add(line);
            }
        } catch (IOException e) {
            throw new HazelcastException(e);
        } finally {
            IOUtil.closeResource(reader);
        }
        return lines;
    }

    private List<String> parseMyDir(File[] dirs) {
        Address thisAddress = node.getThisAddress();
        for (File dir : dirs) {
            try {
                logger.fine("Trying to lock directory: " + dir.getAbsolutePath());
                List<String> lines = parseNodesConf(dir);
                for (String line : lines) {
                    String[] params = line.split("\\s+");
                    if (!"myself,master".equals(params[2])) {
                        continue;
                    }

                    Address address = parseAddress(params[1]);
                    if (thisAddress.equals(address)) {
                        logger.info("Using directory: " + dir.getAbsolutePath());
                        this.uuid = params[0];
                        return lines;
                    }
                }

            } catch (Exception e) {
                logger.fine("Could not lock directory: " + dir.getAbsolutePath() + ". Reason: " + e.getMessage());
            }
        }
        return null;
    }

    private Address parseAddress(String param) throws UnknownHostException {
        String[] args = param.split("@")[0].split(":");
        String host = args[0];
        int port = Integer.parseInt(args[1]);
        return new Address(host, port);
    }

    @Override
    public void beforeJoin() {
        super.beforeJoin();



    }

    @Override
    public String createMemberUuid(Address address) {
        return uuid != null ? uuid : super.createMemberUuid(address);
    }
}
