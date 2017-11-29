package com.hazelcast.raft.service.atomiclong;

import com.hazelcast.config.Config;
import com.hazelcast.config.ServiceConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IAtomicLong;
import com.hazelcast.core.ICompletableFuture;
import com.hazelcast.core.IFunction;
import com.hazelcast.nio.Address;
import com.hazelcast.raft.impl.RaftNode;
import com.hazelcast.raft.impl.service.HazelcastRaftTestSupport;
import com.hazelcast.raft.impl.service.RaftServiceUtil;
import com.hazelcast.raft.service.atomiclong.proxy.RaftAtomicLongProxy;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.util.RandomPicker;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import static com.hazelcast.raft.impl.RaftUtil.getLeaderEndpoint;
import static com.hazelcast.raft.service.atomiclong.proxy.RaftAtomicLongProxy.create;

@RunWith(HazelcastSerialClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class RaftAtomicLongBasicTest extends HazelcastRaftTestSupport {

    private HazelcastInstance[] instances;
    private IAtomicLong atomicLong;
    private final int raftGroupSize = 3;

    @Before
    public void setup() throws ExecutionException, InterruptedException {
        Address[] raftAddresses = createAddresses(5);
        instances = newInstances(raftAddresses, raftAddresses.length + 2);

        String name = "id";
        atomicLong = create(instances[RandomPicker.getInt(instances.length)], name, raftGroupSize);
        Assert.assertNotNull(atomicLong);
    }

    @Test
    public void createNewAtomicLong() throws Exception {
        HazelcastTestSupport.assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                int count = 0;
                RaftAtomicLongProxy proxy = (RaftAtomicLongProxy) atomicLong;
                for (HazelcastInstance instance : instances) {
                    RaftNode raftNode = RaftServiceUtil.getRaftService(instance).getRaftNode(proxy.getGroupId());
                    if (raftNode != null) {
                        count++;
                        Assert.assertNotNull(getLeaderEndpoint(raftNode));
                    }
                }
                Assert.assertEquals(raftGroupSize, count);
            }
        });
    }

    @Test
    public void testSet() {
        atomicLong.set(271);
        Assert.assertEquals(271, atomicLong.get());
    }

    @Test
    public void testGet() {
        Assert.assertEquals(0, atomicLong.get());
    }

    @Test
    public void testDecrementAndGet() {
        Assert.assertEquals(-1, atomicLong.decrementAndGet());
        Assert.assertEquals(-2, atomicLong.decrementAndGet());
    }

    @Test
    public void testIncrementAndGet() {
        Assert.assertEquals(1, atomicLong.incrementAndGet());
        Assert.assertEquals(2, atomicLong.incrementAndGet());
    }

    @Test
    public void testGetAndSet() {
        Assert.assertEquals(0, atomicLong.getAndSet(271));
        Assert.assertEquals(271, atomicLong.get());
    }

    @Test
    public void testAddAndGet() {
        Assert.assertEquals(271, atomicLong.addAndGet(271));
    }

    @Test
    public void testGetAndAdd() {
        Assert.assertEquals(0, atomicLong.getAndAdd(271));
        Assert.assertEquals(271, atomicLong.get());
    }

    @Test
    public void testCompareAndSet_whenSuccess() {
        Assert.assertTrue(atomicLong.compareAndSet(0, 271));
        Assert.assertEquals(271, atomicLong.get());
    }

    @Test
    public void testCompareAndSet_whenNotSuccess() {
        Assert.assertFalse(atomicLong.compareAndSet(172, 0));
        Assert.assertEquals(0, atomicLong.get());
    }

    @Test
    public void testAlter() {
        atomicLong.set(2);

        atomicLong.alter(new MultiplyByTwo());

        Assert.assertEquals(4, atomicLong.get());
    }

    @Test
    public void testAlterAndGet() {
        atomicLong.set(2);

        long result = atomicLong.alterAndGet(new MultiplyByTwo());

        Assert.assertEquals(4, result);
    }

    @Test
    public void testGetAndAlter() {
        atomicLong.set(2);

        long result = atomicLong.getAndAlter(new MultiplyByTwo());

        Assert.assertEquals(2, result);
        Assert.assertEquals(4, atomicLong.get());
    }

    @Test
    public void testAlterAsync() throws ExecutionException, InterruptedException {
        atomicLong.set(2);

        ICompletableFuture<Void> f = atomicLong.alterAsync(new MultiplyByTwo());
        f.get();

        Assert.assertEquals(4, atomicLong.get());
    }

    @Test
    public void testAlterAndGetAsync() throws ExecutionException, InterruptedException {
        atomicLong.set(2);

        ICompletableFuture<Long> f = atomicLong.alterAndGetAsync(new MultiplyByTwo());
        long result = f.get();

        Assert.assertEquals(4, result);
    }

    @Test
    public void testGetAndAlterAsync() throws ExecutionException, InterruptedException {
        atomicLong.set(2);

        ICompletableFuture<Long> f = atomicLong.getAndAlterAsync(new MultiplyByTwo());
        long result = f.get();

        Assert.assertEquals(2, result);
        Assert.assertEquals(4, atomicLong.get());
    }

    @Test
    public void testApply() {
        atomicLong.set(2);

        long result = atomicLong.apply(new MultiplyByTwo());

        Assert.assertEquals(4, result);
        Assert.assertEquals(2, atomicLong.get());
    }

    @Test
    public void testApplyAsync() throws ExecutionException, InterruptedException {
        atomicLong.set(2);

        Future<Long> f = atomicLong.applyAsync(new MultiplyByTwo());
        long result = f.get();

        Assert.assertEquals(4, result);
        Assert.assertEquals(2, atomicLong.get());
    }

    @Override
    protected Config createConfig(Address[] raftAddresses) {
        ServiceConfig atomicLongServiceConfig = new ServiceConfig().setEnabled(true)
                .setName(RaftAtomicLongService.SERVICE_NAME).setClassName(RaftAtomicLongService.class.getName());

        Config config = super.createConfig(raftAddresses);
        config.getServicesConfig().addServiceConfig(atomicLongServiceConfig);
        return config;
    }

    public static class MultiplyByTwo implements IFunction<Long, Long> {

        @Override
        public Long apply(Long input) {
            return input * 2;
        }
    }
}