package com.hazelcast.redis;

import com.hazelcast.core.ExecutionCallback;
import com.hazelcast.internal.serialization.impl.HeapData;
import com.hazelcast.map.impl.MapService;
import com.hazelcast.map.impl.MapServiceContext;
import com.hazelcast.map.impl.operation.MapOperationProvider;
import com.hazelcast.nio.Connection;
import com.hazelcast.nio.redis.CRC16;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.ExecutionService;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.OperationFactory;
import com.hazelcast.util.function.Consumer;

import java.util.Map;
import java.util.concurrent.Executor;

import static com.hazelcast.map.impl.MapService.SERVICE_NAME;
import static com.hazelcast.redis.RESPReply.error;

public class RedisCommandHandler implements Consumer<byte[][]>{

    public static final String REDIS_MAP_NAME = "redis";

    private final NodeEngine nodeEngine;
    private final Connection connection;
    private final Executor executor;

    public RedisCommandHandler(NodeEngine nodeEngine, Connection connection) {
        this.nodeEngine = nodeEngine;
        this.connection = connection;
        this.executor = nodeEngine.getExecutionService().getExecutor(ExecutionService.ASYNC_EXECUTOR);
    }

    @Override
    public void accept(byte[][] args) {
        executor.execute(new RedisTask(args));
    }

    private class RedisTask implements Runnable {
        private final byte[][] args;

        RedisTask(byte[][] args) {
            this.args = args;
        }

        @Override
        public void run() {
            String cmd = new String(args[0]);
            if (cmd.equalsIgnoreCase("SET")) { // map.set
                doSet(args);
            } else if (cmd.equalsIgnoreCase("GETSET")) { // map.put
                doPut(args);
            } else if (cmd.equalsIgnoreCase("GET")) { // map.get
                doGet(args);
            } else if (cmd.equalsIgnoreCase("DEL")) {
                doRemove(args);
            } else if (cmd.equalsIgnoreCase("DBSIZE")) {
                doSize();
            } else {
                connection.write(RESPReply.error("Not implemented!"));
            }
        }
    }

    private void doSet(byte[][] args) {
        HeapData key = toKeydata(new String(args[1]));
        int partitionId = CRC16.getSlot(key.getPartitionHash());

        Data value = nodeEngine.toData(new String(args[2]));

        Operation op = getMapOperationProvider(REDIS_MAP_NAME).createSetOperation(REDIS_MAP_NAME, key, value, -1, -1);
        nodeEngine.getOperationService().invokeOnPartition(MapService.SERVICE_NAME, op, partitionId)
                  .andThen(new ExecutionCallback<Object>() {
            @Override
            public void onResponse(Object response) {
                connection.write(RESPReply.ok());
            }

            @Override
            public void onFailure(Throwable t) {
                connection.write(RESPReply.error(t.getMessage()));
            }
        });
    }

    private HeapData toKeydata(String arg) {
        HeapData key = nodeEngine.getSerializationService().toData(arg);
        int crc16 = CRC16.getCRC16(arg);
        key.setPartitionHash(crc16);
        return key;
    }

    private void doRemove(byte[][] args) {
        HeapData key = toKeydata(new String(args[1]));
        int partitionId = CRC16.getSlot(key.getPartitionHash());

        Operation op = getMapOperationProvider(REDIS_MAP_NAME).createRemoveOperation(REDIS_MAP_NAME, key, false);
        nodeEngine.getOperationService().invokeOnPartition(MapService.SERVICE_NAME, op, partitionId)
                .andThen(new ExecutionCallback<Object>() {
                    @Override
                    public void onResponse(Object response) {
                        if (response != null) {
                            connection.write(RESPReply.integer(1));
                        } else {
                            connection.write(RESPReply.integer(0));
                        }
                    }

                    @Override
                    public void onFailure(Throwable t) {
                        connection.write(RESPReply.error(t.getMessage()));
                    }
                });
    }

    private void doSize() {
        OperationFactory sizeOperationFactory = getMapOperationProvider(REDIS_MAP_NAME).createMapSizeOperationFactory(REDIS_MAP_NAME);
        Map<Integer, Object> results = null;
        try {
            results = nodeEngine.getOperationService().invokeOnAllPartitions(SERVICE_NAME, sizeOperationFactory);
            int total = 0;
            for (Object result : results.values()) {
                Integer size = nodeEngine.getSerializationService().toObject(result);
                total += size;
            }
            connection.write(RESPReply.integer(total));
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void doPut(byte[][] args) {
        HeapData key = toKeydata(new String(args[1]));
        int partitionId = CRC16.getSlot(key.getPartitionHash());

        Data value = nodeEngine.toData(new String(args[2]));

        Operation op = getMapOperationProvider(REDIS_MAP_NAME).createPutOperation(REDIS_MAP_NAME, key, value, -1, -1);
        nodeEngine.getOperationService().invokeOnPartition(MapService.SERVICE_NAME, op, partitionId)
                  .andThen(new ExecutionCallback<Object>() {
            @Override
            public void onResponse(Object response) {
                if (response != null) {
                    String str = nodeEngine.toObject(response);
                    connection.write(RESPReply.string(str));
                } else {
                    connection.write(RESPReply.nil());
                }
            }

            @Override
            public void onFailure(Throwable t) {
                connection.write(error(t.getMessage()));
            }
        });

    }

    private void doGet(byte[][] args) {
        HeapData key = toKeydata(new String(args[1]));
        int partitionId = CRC16.getSlot(key.getPartitionHash());

        Operation op = getMapOperationProvider(REDIS_MAP_NAME).createGetOperation(REDIS_MAP_NAME, key);
        nodeEngine.getOperationService().invokeOnPartition(MapService.SERVICE_NAME, op, partitionId)
                  .andThen(new ExecutionCallback<Object>() {
            @Override
            public void onResponse(Object response) {
                if (response != null) {
                    String str = nodeEngine.toObject(response);
                    connection.write(RESPReply.string(str));
                } else {
                    connection.write(RESPReply.nil());
                }
            }

            @Override
            public void onFailure(Throwable t) {
                connection.write(error(t.getMessage()));
            }
        });
    }

    protected final MapOperationProvider getMapOperationProvider(String mapName) {
        MapService mapService = nodeEngine.getService(MapService.SERVICE_NAME);
        MapServiceContext mapServiceContext = mapService.getMapServiceContext();
        return mapServiceContext.getMapOperationProvider(mapName);
    }

}
