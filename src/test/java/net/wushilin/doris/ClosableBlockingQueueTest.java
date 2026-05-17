package net.wushilin.doris;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.AbstractCollection;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.*;

class ClosableBlockingQueueTest {

    @Test
    void pollWithTimeoutDistinguishesItemTimeoutAndClosed() throws Exception {
        ClosableBlockingQueue<String> queue = new ClosableBlockingQueue<>(2);

        ClosableBlockingQueue.PollResult<String> timeout =
                queue.pollWithTimeout(1, TimeUnit.MILLISECONDS);
        assertFalse(timeout.ok());
        assertTrue(timeout.isTimeout());
        assertFalse(timeout.isClosed());

        assertTrue(queue.put("a").ok());
        ClosableBlockingQueue.PollResult<String> timedItem =
                queue.pollWithTimeout(1, TimeUnit.MILLISECONDS);
        assertTrue(timedItem.ok());
        assertEquals("a", timedItem.item());

        assertTrue(queue.put("b").ok());
        ClosableBlockingQueue.PollResult<String> blockingItem = queue.poll();
        assertTrue(blockingItem.ok());
        assertEquals("b", blockingItem.item());

        queue.close();
        ClosableBlockingQueue.PollResult<String> timedClosed =
                queue.pollWithTimeout(1, TimeUnit.MILLISECONDS);
        assertFalse(timedClosed.ok());
        assertFalse(timedClosed.isTimeout());
        assertTrue(timedClosed.isClosed());

        ClosableBlockingQueue.PollResult<String> blockingClosed = queue.poll();
        assertFalse(blockingClosed.ok());
        assertTrue(blockingClosed.isClosed());
    }

    @Test
    void pollWaitsUntilItemOrClosed() throws Exception {
        ClosableBlockingQueue<String> queue = new ClosableBlockingQueue<>(1);

        Thread producer = new Thread(() -> {
            try {
                Thread.sleep(20);
                queue.put("delayed");
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        });
        producer.start();

        ClosableBlockingQueue.PollResult<String> item = queue.poll();
        assertTrue(item.ok());
        assertEquals("delayed", item.item());
        producer.join();

        Thread closer = new Thread(() -> {
            try {
                Thread.sleep(20);
                queue.close();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        });
        closer.start();

        ClosableBlockingQueue.PollResult<String> closed = queue.poll();
        assertFalse(closed.ok());
        assertTrue(closed.isClosed());
        closer.join();
    }

    @Test
    void closePreventsNewItemsButPreservesAcceptedItems() throws Exception {
        ClosableBlockingQueue<String> queue = new ClosableBlockingQueue<>(2);

        assertTrue(queue.put("accepted").ok());
        queue.close();

        ClosableBlockingQueue.OfferResult late = queue.offer("late", 1, TimeUnit.MILLISECONDS);
        assertFalse(late.ok());
        assertFalse(late.isTimeout());
        assertTrue(late.isClosed());
        assertEquals("accepted", queue.poll().item());
        assertTrue(queue.poll().isClosed());
    }

    @Test
    void offerWithZeroOrNegativeTimeoutStillEnqueuesWhenCapacityExists() throws Exception {
        ClosableBlockingQueue<String> queue = new ClosableBlockingQueue<>(2);

        assertTrue(queue.offer("zero", 0, TimeUnit.NANOSECONDS).ok());
        assertTrue(queue.offer("negative", -1, TimeUnit.NANOSECONDS).ok());

        assertEquals("zero", queue.poll().item());
        assertEquals("negative", queue.poll().item());
    }

    @Test
    void offerWithZeroOrNegativeTimeoutDoesNotWaitWhenFull() throws Exception {
        ClosableBlockingQueue<String> queue = new ClosableBlockingQueue<>(1);

        assertTrue(queue.put("full").ok());
        ClosableBlockingQueue.OfferResult zero = queue.offer("zero", 0, TimeUnit.NANOSECONDS);
        assertFalse(zero.ok());
        assertTrue(zero.isTimeout());
        assertFalse(zero.isClosed());

        ClosableBlockingQueue.OfferResult negative = queue.offer("negative", -1, TimeUnit.NANOSECONDS);
        assertFalse(negative.ok());
        assertTrue(negative.isTimeout());
        assertFalse(negative.isClosed());

        assertEquals("full", queue.poll().item());
    }

    @Test
    void drainToWithTimeoutDrainsAvailableItemsInFifoOrder() throws Exception {
        ClosableBlockingQueue<String> queue = new ClosableBlockingQueue<>(4);
        assertTrue(queue.put("a").ok());
        assertTrue(queue.put("b").ok());
        assertTrue(queue.put("c").ok());

        List<String> drained = new ArrayList<>();
        ClosableBlockingQueue.DrainResult result =
                queue.drainToWithTimeout(drained, 10, 1, TimeUnit.MILLISECONDS);

        assertTrue(result.ok());
        assertEquals(3, result.count());
        assertEquals(List.of("a", "b", "c"), drained);
        assertTrue(queue.pollWithTimeout(0, TimeUnit.NANOSECONDS).isTimeout());
    }

    @Test
    void drainToRespectsMaxItems() throws Exception {
        ClosableBlockingQueue<String> queue = new ClosableBlockingQueue<>(4);
        assertTrue(queue.put("a").ok());
        assertTrue(queue.put("b").ok());
        assertTrue(queue.put("c").ok());

        List<String> drained = new ArrayList<>();
        ClosableBlockingQueue.DrainResult result = queue.drainTo(drained, 2);

        assertTrue(result.ok());
        assertEquals(2, result.count());
        assertEquals(List.of("a", "b"), drained);
        assertEquals("c", queue.poll().item());
    }

    @Test
    void drainToReturnsAcceptedItemsBeforeClosedSignal() throws Exception {
        ClosableBlockingQueue<String> queue = new ClosableBlockingQueue<>(2);
        assertTrue(queue.put("accepted").ok());
        queue.close();

        List<String> drained = new ArrayList<>();
        ClosableBlockingQueue.DrainResult items = queue.drainTo(drained, 10);

        assertTrue(items.ok());
        assertEquals(1, items.count());
        assertEquals(List.of("accepted"), drained);

        ClosableBlockingQueue.DrainResult closed = queue.drainTo(drained, 10);
        assertFalse(closed.ok());
        assertTrue(closed.isClosed());
    }

    @Test
    void drainToWithTimeoutDistinguishesTimeoutAndClosed() throws Exception {
        ClosableBlockingQueue<String> queue = new ClosableBlockingQueue<>(1);
        List<String> drained = new ArrayList<>();

        ClosableBlockingQueue.DrainResult timeout =
                queue.drainToWithTimeout(drained, 10, 0, TimeUnit.NANOSECONDS);
        assertFalse(timeout.ok());
        assertTrue(timeout.isTimeout());
        assertFalse(timeout.isClosed());
        assertTrue(drained.isEmpty());

        queue.close();
        ClosableBlockingQueue.DrainResult closed =
                queue.drainToWithTimeout(drained, 10, 0, TimeUnit.NANOSECONDS);
        assertFalse(closed.ok());
        assertFalse(closed.isTimeout());
        assertTrue(closed.isClosed());
        assertTrue(drained.isEmpty());
    }

    @Test
    void drainToDoesNotRemoveItemRejectedByTargetCollection() throws Exception {
        ClosableBlockingQueue<String> queue = new ClosableBlockingQueue<>(1);
        assertTrue(queue.put("kept").ok());

        AbstractCollection<String> rejectingCollection = new AbstractCollection<>() {
            @Override
            public boolean add(String item) {
                return false;
            }

            @Override
            public java.util.Iterator<String> iterator() {
                return List.<String>of().iterator();
            }

            @Override
            public int size() {
                return 0;
            }
        };

        assertThrows(IllegalStateException.class, () -> queue.drainTo(rejectingCollection, 1));
        assertEquals("kept", queue.poll().item());
    }

    @Test
    void timedOperationsIncludeTimeSpentWaitingForLock() throws Exception {
        ClosableBlockingQueue<String> queue = new ClosableBlockingQueue<>(1);
        assertTrue(queue.put("held").ok());

        CountDownLatch drainHasLock = new CountDownLatch(1);
        CountDownLatch releaseDrain = new CountDownLatch(1);
        AtomicReference<Throwable> drainError = new AtomicReference<>();
        AbstractCollection<String> blockingTarget = new AbstractCollection<>() {
            @Override
            public boolean add(String item) {
                drainHasLock.countDown();
                try {
                    assertTrue(releaseDrain.await(5, TimeUnit.SECONDS));
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new AssertionError(e);
                }
                return true;
            }

            @Override
            public java.util.Iterator<String> iterator() {
                return List.<String>of().iterator();
            }

            @Override
            public int size() {
                return 0;
            }
        };

        Thread lockHolder = new Thread(() -> {
            try {
                assertTrue(queue.drainTo(blockingTarget, 1).ok());
            } catch (Throwable e) {
                drainError.set(e);
            }
        });
        lockHolder.start();
        assertTrue(drainHasLock.await(5, TimeUnit.SECONDS));

        ClosableBlockingQueue.PollResult<String> poll =
                queue.pollWithTimeout(20, TimeUnit.MILLISECONDS);
        assertFalse(poll.ok());
        assertTrue(poll.isTimeout());

        ClosableBlockingQueue.OfferResult offer =
                queue.offer("late", 20, TimeUnit.MILLISECONDS);
        assertFalse(offer.ok());
        assertTrue(offer.isTimeout());

        List<String> drained = new ArrayList<>();
        ClosableBlockingQueue.DrainResult drain =
                queue.drainToWithTimeout(drained, 1, 20, TimeUnit.MILLISECONDS);
        assertFalse(drain.ok());
        assertTrue(drain.isTimeout());
        assertTrue(drained.isEmpty());

        releaseDrain.countDown();
        lockHolder.join(5_000);
        assertFalse(lockHolder.isAlive());
        assertNull(drainError.get());
    }
}
