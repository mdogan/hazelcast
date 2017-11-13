package com.hazelcast.raft.impl.log;

import com.hazelcast.raft.RaftOperation;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;

import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;

@RunWith(HazelcastSerialClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class RaftLogTest {

    @Rule
    public ExpectedException exception = ExpectedException.none();

    private RaftLog log;

    @Before
    public void setUp() throws Exception {
        log = new RaftLog();
    }

    @Test
    public void test_initialState() throws Exception {
        assertEquals(0, log.lastLogTerm());
        assertEquals(0, log.lastLogIndex());
    }

    @Test
    public void test_appendEntries_withSameTerm() throws Exception {
        log.appendEntries(new LogEntry(1, 1, null));
        log.appendEntries(new LogEntry(1, 2, null));
        LogEntry last = new LogEntry(1, 3, null);
        log.appendEntries(last);

        assertEquals(last.term(), log.lastLogTerm());
        assertEquals(last.index(), log.lastLogIndex());
    }

    @Test
    public void test_appendEntries_withHigherTerms() throws Exception {
        LogEntry[] entries = new LogEntry[] {
            new LogEntry(1, 1, null),
            new LogEntry(1, 2, null),
            new LogEntry(1, 3, null)
        };
        log.appendEntries(entries);
        LogEntry last = new LogEntry(2, 4, null);
        log.appendEntries(last);

        assertEquals(last.term(), log.lastLogTerm());
        assertEquals(last.index(), log.lastLogIndex());

        LogEntry lastLogEntry = log.lastLogEntry();
        assertEquals(last.term(), lastLogEntry.term());
        assertEquals(last.index(), lastLogEntry.index());
    }

    @Test
    public void test_appendEntries_withLowerTerm() throws Exception {
        LogEntry[] entries = new LogEntry[] {
                new LogEntry(2, 1, null),
                new LogEntry(2, 2, null),
                new LogEntry(2, 3, null)
        };
        log.appendEntries(entries);

        exception.expect(IllegalArgumentException.class);
        log.appendEntries(new LogEntry(1, 4, null));
    }

    @Test
    public void test_appendEntries_withLowerIndex() throws Exception {
        LogEntry[] entries = new LogEntry[] {
                new LogEntry(2, 1, null),
                new LogEntry(2, 2, null),
                new LogEntry(2, 3, null)
        };
        log.appendEntries(entries);

        exception.expect(IllegalArgumentException.class);
        log.appendEntries(new LogEntry(2, 2, null));
    }

    @Test
    public void test_appendEntries_withEqualIndex() throws Exception {
        LogEntry[] entries = new LogEntry[] {
                new LogEntry(2, 1, null),
                new LogEntry(2, 2, null),
                new LogEntry(2, 3, null)
        };
        log.appendEntries(entries);

        exception.expect(IllegalArgumentException.class);
        log.appendEntries(new LogEntry(2, 3, null));
    }

    @Test
    public void test_appendEntries_withGreaterIndex() throws Exception {
        LogEntry[] entries = new LogEntry[] {
                new LogEntry(2, 1, null),
                new LogEntry(2, 2, null),
                new LogEntry(2, 3, null)
        };
        log.appendEntries(entries);

        exception.expect(IllegalArgumentException.class);
        log.appendEntries(new LogEntry(2, 5, null));
    }

    @Test
    public void test_appendEntriesAfterSnapshot_withSameTerm() {
        LogEntry[] entries = new LogEntry[] {
                new LogEntry(1, 1, null),
                new LogEntry(1, 2, null),
                new LogEntry(1, 3, null)
        };
        log.appendEntries(entries);
        log.setSnapshot(3, null);

        LogEntry last = new LogEntry(1, 4, null);
        log.appendEntries(last);

        assertEquals(last.term(), log.lastLogTerm());
        assertEquals(last.index(), log.lastLogIndex());
    }

    @Test
    public void test_appendEntriesAfterSnapshot_withHigherTerm() {
        LogEntry[] entries = new LogEntry[] {
                new LogEntry(1, 1, null),
                new LogEntry(1, 2, null),
                new LogEntry(1, 3, null)
        };
        log.appendEntries(entries);
        log.setSnapshot(3, null);

        LogEntry last = new LogEntry(2, 4, null);
        log.appendEntries(last);

        assertEquals(last.term(), log.lastLogTerm());
        assertEquals(last.index(), log.lastLogIndex());
    }

    @Test
    public void test_appendEntriesAfterSnapshot_withLowerTerm() throws Exception {
        LogEntry[] entries = new LogEntry[] {
                new LogEntry(2, 1, null),
                new LogEntry(2, 2, null),
                new LogEntry(2, 3, null)
        };
        log.appendEntries(entries);
        log.setSnapshot(3, null);

        exception.expect(IllegalArgumentException.class);
        log.appendEntries(new LogEntry(1, 4, null));
    }

    @Test
    public void test_appendEntriesAfterSnapshot_withLowerIndex() throws Exception {
        LogEntry[] entries = new LogEntry[] {
                new LogEntry(2, 1, null),
                new LogEntry(2, 2, null),
                new LogEntry(2, 3, null)
        };
        log.appendEntries(entries);
        log.setSnapshot(3, null);

        exception.expect(IllegalArgumentException.class);
        log.appendEntries(new LogEntry(2, 2, null));
    }

    @Test
    public void test_appendEntriesAfterSnapshot_withEqualIndex() throws Exception {
        LogEntry[] entries = new LogEntry[] {
                new LogEntry(2, 1, null),
                new LogEntry(2, 2, null),
                new LogEntry(2, 3, null)
        };
        log.appendEntries(entries);
        log.setSnapshot(3, null);

        exception.expect(IllegalArgumentException.class);
        log.appendEntries(new LogEntry(2, 3, null));
    }

    @Test
    public void test_appendEntriesAfterSnapshot_withGreaterIndex() throws Exception {
        LogEntry[] entries = new LogEntry[] {
                new LogEntry(2, 1, null),
                new LogEntry(2, 2, null),
                new LogEntry(2, 3, null)
        };
        log.appendEntries(entries);
        log.setSnapshot(3, null);

        exception.expect(IllegalArgumentException.class);
        log.appendEntries(new LogEntry(2, 5, null));
    }

    @Test
    public void getEntry() throws Exception {
        LogEntry[] entries = new LogEntry[] {
                new LogEntry(1, 1, null),
                new LogEntry(1, 2, null),
                new LogEntry(1, 3, null)
        };
        log.appendEntries(entries);

        for (int i = 1; i <= log.lastLogIndex(); i++) {
            LogEntry entry = log.getEntry(i);
            assertEquals(1, entry.term());
            assertEquals(i, entry.index());
        }
    }

    @Test
    public void getEntryAfterSnapshot() throws Exception {
        LogEntry[] entries = new LogEntry[] {
                new LogEntry(1, 1, null),
                new LogEntry(1, 2, null),
                new LogEntry(1, 3, null)
        };
        log.appendEntries(entries);
        log.setSnapshot(3, null);

        log.appendEntries(new LogEntry(1, 4, null));
        log.appendEntries(new LogEntry(1, 5, null));

        for (int i = 1; i <= 3; i++) {
            assertNull(log.getEntry(i));
        }

        for (int i = 4; i <= log.lastLogIndex(); i++) {
            LogEntry entry = log.getEntry(i);
            assertEquals(1, entry.term());
            assertEquals(i, entry.index());
        }
    }

    @Test
    public void getEntry_withUnknownIndex() throws Exception {
        assertNull(log.getEntry(1));
    }

    @Test
    public void getEntry_withZeroIndex() throws Exception {
        exception.expect(IllegalArgumentException.class);
        log.getEntry(0);
    }

    @Test
    public void getEntriesBetween() throws Exception {
        LogEntry[] entries = new LogEntry[] {
                new LogEntry(1, 1, null),
                new LogEntry(1, 2, null),
                new LogEntry(1, 3, null)
        };
        log.appendEntries(entries);

        LogEntry[] result = log.getEntriesBetween(1, 3);
        assertArrayEquals(entries, result);

        result = log.getEntriesBetween(1, 2);
        assertArrayEquals(Arrays.copyOfRange(entries, 0, 2), result);

        result = log.getEntriesBetween(2, 3);
        assertArrayEquals(Arrays.copyOfRange(entries, 1, 3), result);
    }

    @Test
    public void getEntriesBetweenAfterSnapshot() throws Exception {
        LogEntry[] entries = new LogEntry[] {
                new LogEntry(1, 1, null),
                new LogEntry(1, 2, null),
                new LogEntry(1, 3, null)
        };
        log.appendEntries(entries);
        log.setSnapshot(2, null);

        LogEntry[] result = log.getEntriesBetween(3, 3);
        assertArrayEquals(Arrays.copyOfRange(entries, 2, 3), result);
    }

    @Test
    public void getEntriesBetweenBeforeSnapshotIndex() {
        LogEntry[] entries = new LogEntry[] {
                new LogEntry(1, 1, null),
                new LogEntry(1, 2, null),
                new LogEntry(1, 3, null)
        };
        log.appendEntries(entries);
        log.setSnapshot(2, null);

        exception.expect(IllegalArgumentException.class);
        log.getEntriesBetween(2, 3);
    }

    @Test
    public void truncateEntriesFrom() throws Exception {
        LogEntry[] entries = new LogEntry[] {
                new LogEntry(1, 1, null),
                new LogEntry(1, 2, null),
                new LogEntry(1, 3, null),
                new LogEntry(1, 4, null)
        };
        log.appendEntries(entries);

        List<LogEntry> truncated = log.truncateEntriesFrom(3);
        assertEquals(2, truncated.size());
        assertArrayEquals(Arrays.copyOfRange(entries, 2, 4), truncated.toArray());

        for (int i = 1; i <= 2; i++) {
            assertEquals(entries[i - 1], log.getEntry(i));
        }

        assertNull(log.getEntry(3));
    }

    @Test
    public void truncateEntriesFrom_afterSnapshot() throws Exception {
        LogEntry[] entries = new LogEntry[] {
                new LogEntry(1, 1, null),
                new LogEntry(1, 2, null),
                new LogEntry(1, 3, null),
                new LogEntry(1, 4, null)
        };
        log.appendEntries(entries);
        log.setSnapshot(2, null);

        List<LogEntry> truncated = log.truncateEntriesFrom(3);
        assertEquals(2, truncated.size());
        assertArrayEquals(Arrays.copyOfRange(entries, 2, 4), truncated.toArray());

        assertNull(log.getEntry(3));
    }

    @Test
    public void truncateEntriesFrom_outOfRange() throws Exception {
        LogEntry[] entries = new LogEntry[] {
                new LogEntry(1, 1, null),
                new LogEntry(1, 2, null),
                new LogEntry(1, 3, null),
        };
        log.appendEntries(entries);

        exception.expect(IllegalArgumentException.class);
        log.truncateEntriesFrom(4);
    }

    @Test
    public void truncateEntriesFrom_beforeSnapshotIndex() throws Exception {
        LogEntry[] entries = new LogEntry[] {
                new LogEntry(1, 1, null),
                new LogEntry(1, 2, null),
                new LogEntry(1, 3, null),
                };
        log.appendEntries(entries);
        log.setSnapshot(2, null);

        exception.expect(IllegalArgumentException.class);
        log.truncateEntriesFrom(1);
    }

    @Test
    public void setSnapshotAtLastLogIndex_forSingleEntryLog() {
        LogEntry[] entries = new LogEntry[] { new LogEntry(1, 1, null) };
        log.appendEntries(entries);
        RaftOperation snapshotOp = mock(RaftOperation.class);
        log.setSnapshot(1, snapshotOp);

        LogEntry lastLogEntry = log.lastLogEntry();
        assertEquals(1, lastLogEntry.index());
        assertEquals(1, lastLogEntry.term());
        assertEquals(log.lastLogIndex(), 1);
        assertEquals(log.lastLogTerm(), 1);
        assertEquals(log.snapshotIndex(), 1);

        LogEntry snapshot = log.snapshot();
        assertEquals(1, snapshot.index());
        assertEquals(1, snapshot.term());
        assertTrue(snapshotOp == snapshot.operation());

    }

    @Test
    public void setSnapshotAtLastLogIndex_forMultiEntryLog() {
        LogEntry[] entries = new LogEntry[] {
                new LogEntry(1, 1, null),
                new LogEntry(1, 2, null),
                new LogEntry(1, 3, null),
                new LogEntry(1, 4, null),
                new LogEntry(1, 5, null),
                };
        log.appendEntries(entries);

        log.setSnapshot(5, null);

        LogEntry lastLogEntry = log.lastLogEntry();
        assertEquals(5, lastLogEntry.index());
        assertEquals(1, lastLogEntry.term());
        assertEquals(log.lastLogIndex(), 5);
        assertEquals(log.lastLogTerm(), 1);
        assertEquals(log.snapshotIndex(), 5);

        LogEntry snapshot = log.snapshot();
        assertEquals(5, snapshot.index());
        assertEquals(1, snapshot.term());
    }

    @Test
    public void setSnapshot() {
        LogEntry[] entries = new LogEntry[] {
                new LogEntry(1, 1, null),
                new LogEntry(1, 2, null),
                new LogEntry(1, 3, null),
                new LogEntry(1, 4, null),
                new LogEntry(1, 5, null),
                };
        log.appendEntries(entries);

        log.setSnapshot(3, null);

        LogEntry lastLogEntry = log.lastLogEntry();
        assertEquals(5, lastLogEntry.index());
        assertEquals(1, lastLogEntry.term());
        assertTrue(lastLogEntry == log.getEntry(lastLogEntry.index()));
        assertEquals(log.lastLogIndex(), 5);
        assertEquals(log.lastLogTerm(), 1);
        assertEquals(log.snapshotIndex(), 3);

        LogEntry snapshot = log.snapshot();
        assertEquals(3, snapshot.index());
        assertEquals(1, snapshot.term());
    }

    @Test
    public void setSnapshot_multipleTimes() {
        LogEntry[] entries = new LogEntry[] {
                new LogEntry(1, 1, null),
                new LogEntry(1, 2, null),
                new LogEntry(1, 3, null),
                new LogEntry(1, 4, null),
                new LogEntry(1, 5, null),
                };
        log.appendEntries(entries);

        log.setSnapshot(2, null);
        RaftOperation snapshotOp = mock(RaftOperation.class);
        log.setSnapshot(4, snapshotOp);

        LogEntry lastLogEntry = log.lastLogEntry();
        assertEquals(5, lastLogEntry.index());
        assertEquals(1, lastLogEntry.term());
        assertTrue(lastLogEntry == log.getEntry(lastLogEntry.index()));
        assertEquals(log.lastLogIndex(), 5);
        assertEquals(log.lastLogTerm(), 1);
        assertEquals(log.snapshotIndex(), 4);

        LogEntry snapshot = log.snapshot();
        assertEquals(4, snapshot.index());
        assertEquals(1, snapshot.term());
        assertTrue(snapshot.operation() == snapshotOp);
    }

}
