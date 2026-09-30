package net.coreprotect.consumer;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.lang.management.ManagementFactory;
import java.lang.management.ThreadMXBean;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import net.coreprotect.config.ConfigHandler;
import net.coreprotect.database.DatabaseType;

/**
 * Lycohinya fork: bounded consumer wait with producer wake-up (MySQL/MariaDB only).
 */
class ConsumerAdaptiveWaitTest {

    private DatabaseType previousType;
    private boolean previousRunning;

    @BeforeEach
    void setUp() {
        previousType = ConfigHandler.databaseType;
        previousRunning = ConfigHandler.serverRunning;
        ConfigHandler.databaseType = DatabaseType.MYSQL;
        ConfigHandler.serverRunning = true;
        Consumer.consumer.put(0, new java.util.ArrayList<>());
        Consumer.consumer.put(1, new java.util.ArrayList<>());
        Consumer.currentConsumer = 0;
    }

    @AfterEach
    void tearDown() {
        ConfigHandler.databaseType = previousType;
        ConfigHandler.serverRunning = previousRunning;
    }

    @Test
    void adaptiveWaitIsOptInAndMySqlOnly() {
        // default (no -Dcoreprotect.lycohinya.adaptiveConsumer=true): upstream pacing on every engine
        assertFalse(Consumer.usesAdaptiveWait());
        assertTrue(Consumer.usesAdaptiveWait(true, DatabaseType.MYSQL));
        assertFalse(Consumer.usesAdaptiveWait(true, DatabaseType.SQLITE));
        assertFalse(Consumer.usesAdaptiveWait(true, DatabaseType.DUCKDB));
        assertFalse(Consumer.usesAdaptiveWait(true, DatabaseType.CLICKHOUSE));
        assertFalse(Consumer.usesAdaptiveWait(false, DatabaseType.MYSQL));
    }

    @Test
    void idleWaitIsBoundedAndDoesNotSpin() throws Exception {
        ThreadMXBean threads = ManagementFactory.getThreadMXBean();
        long cpuBefore = threads.getCurrentThreadCpuTime();
        long start = System.nanoTime();
        Consumer.awaitQueuedWork(start + TimeUnit.MILLISECONDS.toNanos(Consumer.ADAPTIVE_MAX_WAIT_MILLIS));
        long elapsed = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start);
        long cpu = TimeUnit.NANOSECONDS.toMillis(threads.getCurrentThreadCpuTime() - cpuBefore);
        assertTrue(elapsed >= Consumer.ADAPTIVE_MAX_WAIT_MILLIS - 5 && elapsed < Consumer.ADAPTIVE_MAX_WAIT_MILLIS + 250, "elapsed " + elapsed);
        assertTrue(cpu < 50, "idle wait burned " + cpu + " ms CPU (busy spin)");
    }

    @Test
    void producerCrossingThresholdWakesConsumer() throws Exception {
        AtomicLong woke = new AtomicLong();
        Thread consumer = new Thread(() -> {
            try {
                long start = System.nanoTime();
                Consumer.awaitQueuedWork(start + TimeUnit.SECONDS.toNanos(10));
                woke.set(TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start));
            }
            catch (InterruptedException ignored) {
                Thread.currentThread().interrupt();
            }
        });
        consumer.start();
        Thread.sleep(100);
        java.util.ArrayList<Object[]> buffer = Consumer.consumer.get(0);
        for (int index = 0; index < Consumer.ADAPTIVE_WAKE_THRESHOLD; index++) {
            int before = buffer.size();
            buffer.add(new Object[0]);
            Consumer.notifyQueued(before, before + 1);
        }
        consumer.join(5_000);
        assertFalse(consumer.isAlive());
        assertTrue(woke.get() < 2_000, "consumer woke after " + woke.get() + " ms");
    }

    @Test
    void fullBufferSkipsWait() throws Exception {
        java.util.ArrayList<Object[]> buffer = Consumer.consumer.get(0);
        for (int index = 0; index < Consumer.ADAPTIVE_WAKE_THRESHOLD; index++) {
            buffer.add(new Object[0]);
        }
        long start = System.nanoTime();
        Consumer.awaitQueuedWork(start + TimeUnit.SECONDS.toNanos(10));
        assertTrue(TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start) < 100);
    }

    @Test
    void shutdownEndsWaitImmediately() throws Exception {
        ConfigHandler.serverRunning = false;
        long start = System.nanoTime();
        Consumer.awaitQueuedWork(start + TimeUnit.SECONDS.toNanos(10));
        assertTrue(TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start) < 100);
    }
}
