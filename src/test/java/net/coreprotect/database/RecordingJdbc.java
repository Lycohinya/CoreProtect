package net.coreprotect.database;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Proxy;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Minimal JDBC doubles that record the order of batch operations. Rows are identified by the value
 * bound to parameter 1 (the {@code time} column of every consumer insert).
 */
final class RecordingJdbc {

    final List<String> events = new ArrayList<>();
    final Map<String, List<Integer>> executedRows = new HashMap<>();
    final Map<String, Integer> executeBatchCalls = new HashMap<>();
    final Map<String, Integer> maxPendingRows = new HashMap<>();
    String failExecuteBatchFor = null;

    Connection connection() {
        InvocationHandler handler = (proxy, method, args) -> {
            switch (method.getName()) {
                case "prepareStatement":
                    return statement(tableOf((String) args[0]));
                case "createStatement":
                    return plainStatement();
                case "isClosed":
                    return false;
                case "close":
                    return null;
                default:
                    throw new UnsupportedOperationException(method.getName());
            }
        };
        return (Connection) Proxy.newProxyInstance(getClass().getClassLoader(), new Class<?>[] { Connection.class }, handler);
    }

    private static String tableOf(String sql) {
        String marker = "INSERT INTO ";
        int start = sql.indexOf(marker);
        if (start < 0) {
            return sql;
        }
        start += marker.length();
        int end = sql.indexOf(' ', start);
        return sql.substring(start, end);
    }

    private Statement plainStatement() {
        InvocationHandler handler = (proxy, method, args) -> {
            switch (method.getName()) {
                case "executeUpdate":
                case "execute":
                    events.add("SQL " + args[0]);
                    return method.getReturnType() == boolean.class ? false : 0;
                case "close":
                    return null;
                case "isClosed":
                    return false;
                default:
                    throw new UnsupportedOperationException(method.getName());
            }
        };
        return (Statement) Proxy.newProxyInstance(getClass().getClassLoader(), new Class<?>[] { Statement.class }, handler);
    }

    private PreparedStatement statement(String table) {
        List<Integer> pending = new ArrayList<>();
        int[] current = { 0 };
        InvocationHandler handler = (proxy, method, args) -> {
            switch (method.getName()) {
                case "setInt":
                    if ((int) args[0] == 1) {
                        current[0] = (int) args[1];
                    }
                    return null;
                case "setLong":
                case "setString":
                case "setObject":
                case "setBytes":
                case "setNull":
                case "setDouble":
                case "setFloat":
                    return null;
                case "addBatch":
                    pending.add(current[0]);
                    maxPendingRows.merge(table, pending.size(), Math::max);
                    return null;
                case "executeBatch":
                    executeBatchCalls.merge(table, 1, Integer::sum);
                    events.add("EXECUTE " + table + " " + pending.size());
                    try {
                        if (table.equals(failExecuteBatchFor)) {
                            throw new SQLException("simulated executeBatch failure");
                        }
                        executedRows.computeIfAbsent(table, ignored -> new ArrayList<>()).addAll(pending);
                        return new int[pending.size()];
                    }
                    finally {
                        pending.clear(); // JDBC clears the batch after executeBatch, success or failure
                    }
                case "clearBatch":
                    events.add("CLEAR " + table + " " + pending.size());
                    pending.clear();
                    return null;
                case "close":
                    return null;
                default:
                    throw new UnsupportedOperationException(method.getName());
            }
        };
        return (PreparedStatement) Proxy.newProxyInstance(getClass().getClassLoader(), new Class<?>[] { PreparedStatement.class }, handler);
    }

    List<Integer> rows(String table) {
        return executedRows.getOrDefault(table, new ArrayList<>());
    }

    int calls(String table) {
        return executeBatchCalls.getOrDefault(table, 0);
    }
}
