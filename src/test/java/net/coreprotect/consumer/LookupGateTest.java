package net.coreprotect.consumer;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import net.coreprotect.config.ConfigHandler;
import net.coreprotect.database.DatabaseType;

/**
 * Lycohinya fork: read-only lookups and the consumer writer.
 */
class LookupGateTest {

    private DatabaseType previousType;

    @BeforeEach
    void setUp() {
        previousType = ConfigHandler.databaseType;
        Consumer.isPaused = false;
        ConfigHandler.purgeRunning = false;
        ConfigHandler.converterRunning = false;
        ConfigHandler.migrationRunning = false;
    }

    @AfterEach
    void tearDown() {
        ConfigHandler.databaseType = previousType;
        Consumer.isPaused = false;
        ConfigHandler.purgeRunning = false;
    }

    @Test
    void mysqlLookupDoesNotPauseWriter() throws Exception {
        ConfigHandler.databaseType = DatabaseType.MYSQL;
        boolean paused = LookupGate.acquire();
        try {
            assertFalse(paused);
            // processConsumerBatch skips a batch only when isPaused is set; an active lookup must not set it
            assertFalse(Consumer.isPaused, "active MySQL lookup must not block the consumer writer");
        }
        finally {
            LookupGate.release(paused);
        }
    }

    @Test
    void mysqlLookupDoesNotWaitForActiveWriter() throws Exception {
        ConfigHandler.databaseType = DatabaseType.MYSQL;
        Consumer.isPaused = true; // Process.processConsumer holds this while a consumer batch is in flight
        long start = System.nanoTime();
        boolean paused = LookupGate.acquire();
        long elapsedMillis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start);
        LookupGate.release(paused);
        assertTrue(elapsedMillis < 200, "lookup waited " + elapsedMillis + " ms for the writer");
        assertTrue(Consumer.isPaused, "lookup must not clear the writer's flag");
    }

    @Test
    void mysqlLookupStillWaitsForPurge() throws Exception {
        ConfigHandler.databaseType = DatabaseType.MYSQL;
        ConfigHandler.purgeRunning = true;
        CountDownLatch entered = new CountDownLatch(1);
        AtomicBoolean failed = new AtomicBoolean();
        Thread lookup = new Thread(() -> {
            try {
                LookupGate.release(LookupGate.acquire());
                entered.countDown();
            }
            catch (InterruptedException exception) {
                failed.set(true);
            }
        });
        lookup.start();
        assertFalse(entered.await(150, TimeUnit.MILLISECONDS), "lookup must wait while purge rewrites tables");
        ConfigHandler.purgeRunning = false;
        assertTrue(entered.await(2, TimeUnit.SECONDS));
        lookup.join();
        assertFalse(failed.get());
    }

    @Test
    void sqliteKeepsUpstreamMutualExclusion() throws Exception {
        ConfigHandler.databaseType = DatabaseType.SQLITE;
        boolean paused = LookupGate.acquire();
        assertTrue(paused);
        assertTrue(Consumer.isPaused, "SQLite lookup still pauses the writer");
        LookupGate.release(paused);
        assertFalse(Consumer.isPaused);

        Consumer.isPaused = true; // writer active
        CountDownLatch entered = new CountDownLatch(1);
        Thread lookup = new Thread(() -> {
            try {
                LookupGate.release(LookupGate.acquire());
                entered.countDown();
            }
            catch (InterruptedException ignored) {
                Thread.currentThread().interrupt();
            }
        });
        lookup.start();
        assertFalse(entered.await(150, TimeUnit.MILLISECONDS), "SQLite lookup waits for the writer batch");
        Consumer.isPaused = false;
        assertTrue(entered.await(2, TimeUnit.SECONDS));
        lookup.join();
    }

    @Test
    void columnarEnginesUnchanged() {
        assertFalse(LookupGate.allowsConcurrentReads(DatabaseType.SQLITE));
        assertFalse(LookupGate.allowsConcurrentReads(DatabaseType.DUCKDB));
        assertFalse(LookupGate.allowsConcurrentReads(DatabaseType.CLICKHOUSE));
        assertTrue(LookupGate.allowsConcurrentReads(DatabaseType.MYSQL));
    }
}
