package net.coreprotect.database;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.List;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import net.coreprotect.config.ConfigHandler;

/**
 * Lycohinya fork: per-statement pending-row batching in {@link RelationalConsumerWriteBatch}.
 * Mirrors the counter-examples of tools/research/coreprotect-audit/run-batch-audit.py and checks
 * row conservation, per-table order, and the commit / rollback / savepoint lifecycle.
 */
class RelationalConsumerWriteBatchTest {

    private static final String BLOCK = "co_block";
    private static final String CONTAINER = "co_container";
    private static final int LIMIT = RelationalConsumerWriteBatch.MAX_PENDING_ROWS_PER_STATEMENT;

    private DatabaseType previousType;
    private String previousPrefix;
    private RecordingJdbc jdbc;
    private RelationalConsumerWriteBatch batch;

    @BeforeEach
    void setUp() throws Exception {
        previousType = ConfigHandler.databaseType;
        previousPrefix = ConfigHandler.prefix;
        ConfigHandler.databaseType = DatabaseType.MYSQL;
        ConfigHandler.prefix = "co_";
        jdbc = new RecordingJdbc();
        batch = new RelationalConsumerWriteBatch(jdbc.connection(), DatabaseType.MYSQL);
        batch.begin();
    }

    @AfterEach
    void tearDown() {
        ConfigHandler.databaseType = previousType;
        ConfigHandler.prefix = previousPrefix;
    }

    private void block(int eventIndex, int row) throws Exception {
        batch.addBlock(eventIndex, row, 1, 1, 0, 64, 0, 1, 0, null, null, 1, 0);
    }

    private void container(int eventIndex, int row) throws Exception {
        batch.addContainer(eventIndex, row, 1, 1, 0, 64, 0, 1, 0, 1, null, 1, 0);
    }

    private static List<Integer> sequence(int from, int toExclusive) {
        List<Integer> values = new ArrayList<>();
        for (int value = from; value < toExclusive; value++) {
            values.add(value);
        }
        return values;
    }

    @Test
    void singleTypeFlushesEveryThousandRowsAndConservesRows() throws Exception {
        for (int event = 0; event < 10_000; event++) {
            block(event, event);
        }
        assertEquals(10, jdbc.calls(BLOCK));
        assertEquals(LIMIT, jdbc.maxPendingRows.get(BLOCK));
        assertTrue(batch.commit());
        assertEquals(sequence(0, 10_000), jdbc.rows(BLOCK), "every row executed once, in order");
    }

    @Test
    void interleavedTypesAreBoundedPerStatement() throws Exception {
        // upstream: event index % 1000 hit only the block statement, so container rows piled up to 10,000
        for (int event = 0; event < 20_000; event++) {
            if (event % 2 == 0) {
                block(event, event);
            }
            else {
                container(event, event);
            }
        }
        assertEquals(LIMIT, jdbc.maxPendingRows.get(BLOCK));
        assertEquals(LIMIT, jdbc.maxPendingRows.get(CONTAINER));
        assertEquals(10, jdbc.calls(BLOCK));
        assertEquals(10, jdbc.calls(CONTAINER));
        assertTrue(batch.commit());
        assertEquals(10_000, jdbc.rows(BLOCK).size());
        assertEquals(10_000, jdbc.rows(CONTAINER).size());
    }

    @Test
    void oneEventWithManyRowsDoesNotFlushPerRow() throws Exception {
        // upstream: a container event at index 1000 producing 54 rows executed 54 one-row batches
        for (int row = 0; row < 54; row++) {
            container(1000, row);
        }
        assertEquals(0, jdbc.calls(CONTAINER));
        assertEquals(54, batch.pendingRows(3));
        assertTrue(batch.commit());
        assertEquals(1, jdbc.calls(CONTAINER));
        assertEquals(sequence(0, 54), jdbc.rows(CONTAINER));
        assertEquals(0, batch.pendingRows(3));
    }

    @Test
    void executeBatchIsNotCommit() throws Exception {
        for (int event = 0; event < LIMIT; event++) {
            block(event, event);
        }
        assertEquals(1, jdbc.calls(BLOCK));
        assertFalse(jdbc.events.contains("SQL COMMIT"), "a threshold flush must stay inside the open transaction");
        assertTrue(batch.commit());
        assertTrue(jdbc.events.indexOf("EXECUTE co_block " + LIMIT) < jdbc.events.indexOf("SQL COMMIT"));
    }

    @Test
    void failedCommitRollsBackAndDoesNotLeakRowsIntoNextTransaction() throws Exception {
        for (int event = 0; event < 10; event++) {
            block(event, event);
            container(event, 100 + event);
        }
        jdbc.failExecuteBatchFor = BLOCK;
        assertFalse(batch.commit());
        assertTrue(jdbc.events.contains("SQL ROLLBACK"));
        assertTrue(jdbc.rows(CONTAINER).isEmpty(), "statements after the failing one must not be sent");
        for (int index = 0; index < 16; index++) {
            assertEquals(0, batch.pendingRows(index));
        }

        // retry of the same events in a new transaction: exactly once, nothing from the failed attempt
        jdbc.failExecuteBatchFor = null;
        jdbc.executedRows.clear();
        batch.begin();
        for (int event = 0; event < 10; event++) {
            block(event, event);
            container(event, 100 + event);
        }
        assertTrue(batch.commit());
        assertEquals(sequence(0, 10), jdbc.rows(BLOCK));
        assertEquals(sequence(100, 110), jdbc.rows(CONTAINER));
    }

    @Test
    void rollbackDiscardsPendingRows() throws Exception {
        for (int event = 0; event < 5; event++) {
            block(event, event);
        }
        batch.rollback();
        assertEquals(0, batch.pendingRows(1));
        batch.begin();
        block(9, 9);
        assertTrue(batch.commit());
        assertEquals(List.of(9), jdbc.rows(BLOCK));
    }

    @Test
    void savepointFailureKeepsEarlierRowsAndDropsRowsOfFailedOperation() throws Exception {
        container(0, 1);
        container(1, 2);
        assertThrows(IllegalStateException.class, () -> batch.executeAtomically("entity_container_transaction", () -> {
            container(2, 3);
            container(2, 4);
            throw new IllegalStateException("simulated failure inside atomic event");
        }));
        container(3, 5);
        assertTrue(batch.commit());
        assertEquals(List.of(1, 2, 5), jdbc.rows(CONTAINER), "rows before the savepoint survive once, failed event rows are gone");
        int flush = jdbc.events.indexOf("EXECUTE co_container 2");
        int savepoint = jdbc.events.indexOf("SQL SAVEPOINT entity_container_transaction");
        assertTrue(flush >= 0 && flush < savepoint, "earlier rows are sent before the savepoint: " + jdbc.events);
        assertTrue(jdbc.events.contains("SQL ROLLBACK TO SAVEPOINT entity_container_transaction"));
    }

    @Test
    void savepointSuccessKeepsAllRowsInOrder() throws Exception {
        container(0, 1);
        batch.executeAtomically("entity_container_transaction", () -> {
            container(1, 2);
            container(1, 3);
        });
        container(2, 4);
        assertTrue(batch.commit());
        assertEquals(List.of(1, 2, 3, 4), jdbc.rows(CONTAINER));
    }
}
