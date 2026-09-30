package net.coreprotect.consumer;

import net.coreprotect.config.ConfigHandler;
import net.coreprotect.database.DatabaseType;

/**
 * Coordinates read-only lookups with the consumer writer.
 *
 * <p>Upstream CoreProtect serialises every lookup against the consumer through the global
 * {@link Consumer#isPaused} flag: a lookup waits for the current consumer batch, then holds the flag
 * so the next batch is skipped until the lookup SQL finishes. That is required for the embedded
 * engines (SQLite, DuckDB) and the ClickHouse path, but on MySQL/MariaDB (InnoDB) every lookup runs
 * on its own pooled connection in autocommit mode, where each SELECT reads a consistent snapshot of
 * committed transactions only. The consumer publishes each buffer as whole transactions, so a
 * concurrent lookup never observes a partially written consumer transaction.
 *
 * <p>Lycohinya fork: for {@link DatabaseType#MYSQL} read-only lookups no longer take or wait for the
 * writer's pause flag. They still wait while table-rewriting maintenance runs (purge, schema patch,
 * converter, migration), which upstream also excluded through the same flag. All other engines keep
 * the upstream behaviour unchanged.
 */
public final class LookupGate {

    private LookupGate() {
        throw new IllegalStateException("Utility class");
    }

    /**
     * @return whether read-only lookups on this engine may run concurrently with the consumer writer
     */
    public static boolean allowsConcurrentReads(DatabaseType databaseType) {
        return databaseType != null && databaseType.isMySQL();
    }

    /**
     * Maintenance that rewrites or rebuilds tables must still exclude lookups.
     */
    static boolean maintenanceBlocksReads() {
        return ConfigHandler.purgeRunning || ConfigHandler.converterRunning || ConfigHandler.migrationRunning;
    }

    /**
     * Enter a read-only lookup section.
     *
     * @return {@code true} when this call took the global {@link Consumer#isPaused} gate and must pass
     *         {@code true} to {@link #release(boolean)}
     */
    public static boolean acquire() throws InterruptedException {
        if (allowsConcurrentReads(ConfigHandler.databaseType)) {
            while (maintenanceBlocksReads() && !Consumer.isPersistenceHalted()) {
                Thread.sleep(1);
            }
            return false;
        }

        while (Consumer.isPaused && !Consumer.isPersistenceHalted()) {
            Thread.sleep(1);
        }
        Consumer.isPaused = true;
        return true;
    }

    /**
     * Leave a read-only lookup section entered with {@link #acquire()}.
     */
    public static void release(boolean paused) {
        if (paused && !Consumer.isPersistenceHalted()) {
            Consumer.isPaused = false;
        }
    }
}
