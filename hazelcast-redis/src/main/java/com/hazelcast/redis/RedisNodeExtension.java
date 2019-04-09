package com.hazelcast.redis;

import com.hazelcast.cluster.ClusterState;
import com.hazelcast.core.HazelcastException;
import com.hazelcast.instance.DefaultNodeExtension;
import com.hazelcast.instance.MemberImpl;
import com.hazelcast.instance.Node;
import com.hazelcast.internal.cluster.impl.ClusterServiceImpl;
import com.hazelcast.internal.partition.InternalPartition;
import com.hazelcast.internal.partition.PartitionReplica;
import com.hazelcast.internal.partition.PartitionTableView;
import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.internal.serialization.impl.HeapData;
import com.hazelcast.map.impl.MapService;
import com.hazelcast.map.impl.MapServiceContext;
import com.hazelcast.map.impl.PartitionContainer;
import com.hazelcast.map.impl.record.Record;
import com.hazelcast.map.impl.recordstore.RecordStore;
import com.hazelcast.map.impl.recordstore.Storage;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.IOUtil;
import com.hazelcast.nio.redis.CRC16;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.spi.impl.PartitionSpecificRunnable;
import com.hazelcast.spi.impl.operationservice.InternalOperationService;
import com.hazelcast.spi.properties.GroupProperty;
import com.hazelcast.util.Clock;
import net.whitbeck.rdbparser.DbSelect;
import net.whitbeck.rdbparser.Entry;
import net.whitbeck.rdbparser.Eof;
import net.whitbeck.rdbparser.KeyValuePair;
import net.whitbeck.rdbparser.RdbParser;
import net.whitbeck.rdbparser.ValueType;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileFilter;
import java.io.FileReader;
import java.io.IOException;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static com.hazelcast.internal.cluster.impl.ClusterStateManagerAccessor.setClusterState;
import static com.hazelcast.internal.cluster.impl.ClusterStateManagerAccessor.setMissingMembers;

/**
 *
 */
public class RedisNodeExtension extends DefaultNodeExtension {

    private static final String REDIS_PATH = System.getProperty("hazelcast.redis.import.path");
    private static final String NODES_CONF = "nodes.conf";
    private static final String DUMP_RDB = "dump.rdb";
    private static final int SLOT_COUNT = 16384;

    private volatile String uuid;
    private volatile File dir;

    public RedisNodeExtension(Node node) {
        super(node);

        if (node.getProperties().getInteger(GroupProperty.PARTITION_COUNT) != SLOT_COUNT) {
            throw new HazelcastException("invalid partition count!");
        }

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

        parseMyDir(dirs);
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

    private void parseMyDir(File[] dirs) {
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
                        this.dir = dir;
                    }
                }

            } catch (Exception e) {
                logger.fine("Could not lock directory: " + dir.getAbsolutePath() + ". Reason: " + e.getMessage());
            }
        }
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

        if (dir == null) {
            logger.severe("No address match!");
            return;
        }

        try {
            ClusterServiceImpl clusterService = node.clusterService;
            setClusterState(clusterService, ClusterState.FROZEN, true);

            PartitionTableView partitionTable = parseConfigParams(parseNodesConf(dir));
            Set<MemberImpl> members = new HashSet<MemberImpl>();

            MemberImpl localMember = node.getLocalMember();
            for (int i = 0; i < partitionTable.getLength(); i++) {
                PartitionReplica replica = partitionTable.getReplica(i, 0);
                if (!replica.isIdentical(localMember)) {
                    members.add(new MemberImpl(replica.address(), localMember.getVersion(), false, replica.uuid()));
                }
            }
            setMissingMembers(clusterService, members);

            node.partitionService.setInitialState(partitionTable);
        } catch (IOException e) {
            throw new HazelcastException(e);
        }

        try {
            parseRdb(new File(dir, DUMP_RDB));
        } catch (Exception e) {
            throw new HazelcastException(e);
        }
    }

    private void parseRdb(File file) throws Exception {
        NodeEngineImpl nodeEngine = node.getNodeEngine();
        MapService mapService = nodeEngine.getService(MapService.SERVICE_NAME);
        final MapServiceContext mapServiceContext = mapService.getMapServiceContext();
        InternalOperationService operationService = nodeEngine.getOperationService();
        final InternalSerializationService serializationService = node.getSerializationService();

        RdbParser parser = new RdbParser(file);
        try {
            Entry e;
            while ((e = parser.readNext()) != null) {
                switch (e.getType()) {
                    case DB_SELECT:
                        System.out.println("Processing DB: " + ((DbSelect) e).getId());
                        System.out.println("------------");
                        break;

                    case EOF:
                        System.out.print("End of file. Checksum: ");
                        for (byte b : ((Eof) e).getChecksum()) {
                            System.out.print(String.format("%02x", b & 0xff));
                        }
                        System.out.println();
                        System.out.println("------------");
                        break;

                    case KEY_VALUE_PAIR:
                        final KeyValuePair kvp = (KeyValuePair) e;
                        final int crc16 = CRC16.getCRC16(kvp.getKey());
                        final int partitionId = CRC16.getSlot(crc16);

                        operationService.execute(new PartitionSpecificRunnable() {
                            @Override
                            public int getPartitionId() {
                                return partitionId;
                            }

                            @Override
                            public void run() {
                                PartitionContainer pc = mapServiceContext.getPartitionContainer(partitionId);
                                RecordStore recordStore = pc.getRecordStore(RedisCommandHandler.REDIS_MAP_NAME, true);

                                Storage storage = recordStore.getStorage();
                                HeapData key = serializationService.toData(new String(kvp.getKey()));
                                key.setPartitionHash(crc16);

                                assert kvp.getValueType() == ValueType.VALUE : kvp.getValueType();
                                HeapData value = serializationService.toData(new String(kvp.getValues().get(0)));

                                Record record = recordStore.createRecord(key, value, -1, -1, Clock.currentTimeMillis());
                                storage.put(key, record);

                                logger.info("Load entry: " + new String(kvp.getKey()));
                            }
                        });
                        break;
                }
            }
        } finally {
            parser.close();
        }
    }

    @Override
    public String createMemberUuid(Address address) {
        return uuid != null ? uuid : super.createMemberUuid(address);
    }
}
