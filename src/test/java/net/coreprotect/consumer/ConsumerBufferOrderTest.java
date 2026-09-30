package net.coreprotect.consumer;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.ArrayList;
import java.util.List;

import org.junit.jupiter.api.Test;

/**
 * Lycohinya fork: an undrained buffer is retried before the newer buffer is written.
 */
class ConsumerBufferOrderTest {

    @Test
    void swapsWhenPreviousBufferDrained() {
        assertArrayEquals(new int[] { 0, 1 }, Consumer.selectProcessBuffer(0, new boolean[] { true, true }));
        assertArrayEquals(new int[] { 1, 1 }, Consumer.selectProcessBuffer(1, new boolean[] { true, true }));
    }

    @Test
    void retriesUndrainedBufferWithoutSwapping() {
        // filling buffer 1, buffer 0 left undrained by the previous pass
        assertArrayEquals(new int[] { 0, 0 }, Consumer.selectProcessBuffer(1, new boolean[] { false, true }));
        assertArrayEquals(new int[] { 1, 0 }, Consumer.selectProcessBuffer(0, new boolean[] { true, false }));
    }

    /**
     * Simulates the run loop: events are appended in order to the filling buffer; some passes fail
     * and leave their buffer undrained. Persisted order must equal enqueue order.
     */
    @Test
    void persistedOrderMatchesEnqueueOrderAcrossFailedPasses() {
        List<List<Integer>> buffers = List.of(new ArrayList<>(), new ArrayList<>());
        boolean[] drained = { true, true };
        int current = 0;
        int next = 0;
        List<Integer> persisted = new ArrayList<>();
        boolean[] failPass = { false, true, false, false, true, true, false, false, false, false, false, false };
        for (boolean fail : failPass) {
            for (int k = 0; k < 3; k++) {
                buffers.get(current).add(next++);
            }
            int[] selection = Consumer.selectProcessBuffer(current, drained);
            int processId = selection[0];
            if (selection[1] == 1) {
                current = processId == 0 ? 1 : 0;
            }
            if (!fail) {
                persisted.addAll(buffers.get(processId));
                buffers.get(processId).clear();
            }
            drained[processId] = buffers.get(processId).isEmpty();
        }
        for (int i = 1; i < persisted.size(); i++) {
            assertEquals(persisted.get(i - 1) + 1, (int) persisted.get(i), "out of order at " + i + ": " + persisted);
        }
    }
}
