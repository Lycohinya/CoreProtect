package net.coreprotect.database;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import net.coreprotect.config.ConfigHandler;

/**
 * Lycohinya fork: runs {@link RelationalConsumerWriteBatch} against real databases. SQLite (in memory)
 * always runs; MariaDB/MySQL runs when CP_TEST_MYSQL_URL / CP_TEST_MYSQL_USER / CP_TEST_MYSQL_PASSWORD
 * are set (the tests drop and recreate {@code cpt_block} and {@code cpt_container} in that schema).
 */
class RelationalConsumerWriteBatchDatabaseTest {

    private static final String PREFIX = "cpt_";
    private DatabaseType previousType;
    private String previousPrefix;
    private final List<Connection> opened = new ArrayList<>();

    static Stream<Arguments> engines() {
        return Stream.of(Arguments.of(DatabaseType.SQLITE), Arguments.of(DatabaseType.MYSQL));
    }

    @AfterEach
    void tearDown() throws Exception {
        for (Connection connection : opened) {
            connection.close();
        }
        if (previousType != null) {
            ConfigHandler.databaseType = previousType;
            ConfigHandler.prefix = previousPrefix;
        }
    }

    private Connection open(DatabaseType type) throws Exception {
        Connection connection;
        if (type.isMySQL()) {
            String url = System.getenv("CP_TEST_MYSQL_URL");
            assumeTrue(url != null && !url.isEmpty(), "CP_TEST_MYSQL_URL not set");
            connection = DriverManager.getConnection(url, System.getenv("CP_TEST_MYSQL_USER"), System.getenv("CP_TEST_MYSQL_PASSWORD"));
        }
        else {
            connection = DriverManager.getConnection("jdbc:sqlite:file:cpbatch?mode=memory&cache=shared");
        }
        opened.add(connection);
        return connection;
    }

    private Connection prepare(DatabaseType type) throws Exception {
        previousType = ConfigHandler.databaseType;
        previousPrefix = ConfigHandler.prefix;
        ConfigHandler.databaseType = type;
        ConfigHandler.prefix = PREFIX;
        Connection connection = open(type);
        try (Statement statement = connection.createStatement()) {
            statement.executeUpdate("DROP TABLE IF EXISTS " + PREFIX + "block");
            statement.executeUpdate("DROP TABLE IF EXISTS " + PREFIX + "container");
            String id = type.isMySQL() ? "rowid BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY" : "rowid INTEGER PRIMARY KEY ASC";
            String engine = type.isMySQL() ? " ENGINE=InnoDB" : "";
            statement.executeUpdate("CREATE TABLE " + PREFIX + "block (" + id + ", time INT, user INT, wid INT, x INT, y INT, z INT, type INT, data INT, meta BLOB, blockdata BLOB, action INT, rolled_back INT)" + engine);
            statement.executeUpdate("CREATE TABLE " + PREFIX + "container (" + id + ", time INT, user INT, wid INT, x INT, y INT, z INT, type INT, data INT, amount INT, metadata BLOB, action INT, rolled_back INT)" + engine);
        }
        return connection;
    }

    private static List<Integer> times(Connection connection, String table) throws Exception {
        List<Integer> values = new ArrayList<>();
        try (Statement statement = connection.createStatement(); ResultSet results = statement.executeQuery("SELECT time FROM " + PREFIX + table + " ORDER BY rowid ASC")) {
            while (results.next()) {
                values.add(results.getInt(1));
            }
        }
        return values;
    }

    private static long count(Connection connection, String table) throws Exception {
        try (Statement statement = connection.createStatement(); ResultSet results = statement.executeQuery("SELECT COUNT(*) FROM " + PREFIX + table)) {
            results.next();
            return results.getLong(1);
        }
    }

    @ParameterizedTest
    @MethodSource("engines")
    void mixedWorkloadConservesRowsAndOrder(DatabaseType type) throws Exception {
        Connection connection = prepare(type);
        RelationalConsumerWriteBatch batch = new RelationalConsumerWriteBatch(connection, type);
        batch.begin();
        List<Integer> expectedBlocks = new ArrayList<>();
        List<Integer> expectedContainers = new ArrayList<>();
        int containerRow = 0;
        for (int event = 0; event < 25_000; event++) {
            if (event % 100 == 0) {
                for (int slot = 0; slot < 54; slot++) { // one event, 54 container rows
                    batch.addContainer(event, containerRow, 1, 1, event, 64, 0, 1, 0, 1, null, 1, 0);
                    expectedContainers.add(containerRow++);
                }
            }
            else {
                batch.addBlock(event, event, 1, 1, event, 64, 0, 1, 0, null, null, event % 2, 0);
                expectedBlocks.add(event);
            }
        }
        assertTrue(batch.commit());
        batch.close();
        assertEquals(expectedBlocks, times(connection, "block"), "no loss, no duplicate, insertion order");
        assertEquals(expectedContainers, times(connection, "container"));
    }

    @ParameterizedTest
    @MethodSource("engines")
    void savepointRollbackIsAtomic(DatabaseType type) throws Exception {
        Connection connection = prepare(type);
        RelationalConsumerWriteBatch batch = new RelationalConsumerWriteBatch(connection, type);
        batch.begin();
        batch.addContainer(0, 1, 1, 1, 0, 64, 0, 1, 0, 1, null, 1, 0);
        assertThrows(IllegalStateException.class, () -> batch.executeAtomically("entity_container_transaction", () -> {
            batch.addContainer(1, 2, 1, 1, 0, 64, 0, 1, 0, 1, null, 1, 0);
            throw new IllegalStateException("simulated");
        }));
        batch.addContainer(2, 3, 1, 1, 0, 64, 0, 1, 0, 1, null, 1, 0);
        assertTrue(batch.commit());
        batch.close();
        assertEquals(List.of(1, 3), times(connection, "container"));
    }

    @ParameterizedTest
    @MethodSource("engines")
    void flushedRowsStayInvisibleToOtherConnectionsUntilCommit(DatabaseType type) throws Exception {
        assumeTrue(type.isMySQL(), "reader isolation is only relied on for MySQL/MariaDB lookups");
        Connection writer = prepare(type);
        Connection reader = open(type);
        RelationalConsumerWriteBatch batch = new RelationalConsumerWriteBatch(writer, type);
        batch.begin();
        for (int event = 0; event < 2_500; event++) { // two threshold flushes (executeBatch) inside the transaction
            batch.addBlock(event, event, 1, 1, event, 64, 0, 1, 0, null, null, 1, 0);
        }
        assertEquals(0L, count(reader, "block"), "executeBatch is not a commit: a concurrent lookup sees no partial consumer transaction");
        assertTrue(batch.commit());
        batch.close();
        assertEquals(2_500L, count(reader, "block"));
    }
}
