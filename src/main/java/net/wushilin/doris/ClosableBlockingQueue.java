package net.wushilin.doris;

import java.util.ArrayDeque;
import java.util.Collection;
import java.util.Deque;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Bounded blocking queue with explicit end-of-stream semantics.
 *
 * <p>After {@link #close()}, no new item can be added. Waiting producers and
 * consumers are woken. Consumers keep draining already accepted items.
 */
class ClosableBlockingQueue<T> {
    private final ReentrantLock lock = new ReentrantLock();
    private final Condition notEmpty = lock.newCondition();
    private final Condition notFull = lock.newCondition();
    private final Deque<T> items = new ArrayDeque<>();
    private final int capacity;
    private boolean closed;

    ClosableBlockingQueue(int capacity) {
        if (capacity <= 0) {
            throw new IllegalArgumentException("capacity must be > 0");
        }
        this.capacity = capacity;
    }

    OfferResult offer(T item, long timeout, TimeUnit unit) throws InterruptedException {
        Objects.requireNonNull(item, "item");
        long timeoutNanos = unit.toNanos(timeout);
        long deadlineNanos = deadlineFromNow(timeoutNanos);
        if (!tryLockUntil(deadlineNanos, timeoutNanos)) {
            return OfferResult.timeout();
        }
        try {
            while (!closed && items.size() == capacity) {
                long remainingNanos = remainingNanos(deadlineNanos);
                if (remainingNanos <= 0L) {
                    return OfferResult.timeout();
                }
                notFull.awaitNanos(remainingNanos);
            }
            if (closed) {
                return OfferResult.closed();
            }
            items.addLast(item);
            notEmpty.signal();
            return OfferResult.accepted();
        } finally {
            lock.unlock();
        }
    }

    OfferResult put(T item) throws InterruptedException {
        Objects.requireNonNull(item, "item");
        lock.lockInterruptibly();
        try {
            while (!closed && items.size() == capacity) {
                notFull.await();
            }
            if (closed) {
                return OfferResult.closed();
            }
            items.addLast(item);
            notEmpty.signal();
            return OfferResult.accepted();
        } finally {
            lock.unlock();
        }
    }

    PollResult<T> pollWithTimeout(long timeout, TimeUnit unit) throws InterruptedException {
        long timeoutNanos = unit.toNanos(timeout);
        long deadlineNanos = deadlineFromNow(timeoutNanos);
        if (!tryLockUntil(deadlineNanos, timeoutNanos)) {
            return PollResult.timeout();
        }
        try {
            while (items.isEmpty() && !closed) {
                long remainingNanos = remainingNanos(deadlineNanos);
                if (remainingNanos <= 0L) {
                    return PollResult.timeout();
                }
                notEmpty.awaitNanos(remainingNanos);
            }
            T item = items.pollFirst();
            if (item != null) {
                notFull.signal();
                return PollResult.item(item);
            }
            if (closed) {
                return PollResult.closed();
            }
            throw new AssertionError("Queue is open and empty after timed poll wait");
        } finally {
            lock.unlock();
        }
    }

    DrainResult drainToWithTimeout(Collection<? super T> target, int maxItems,
                                   long timeout, TimeUnit unit) throws InterruptedException {
        Objects.requireNonNull(target, "target");
        if (maxItems <= 0) {
            throw new IllegalArgumentException("maxItems must be > 0");
        }

        long timeoutNanos = unit.toNanos(timeout);
        long deadlineNanos = deadlineFromNow(timeoutNanos);
        if (!tryLockUntil(deadlineNanos, timeoutNanos)) {
            return DrainResult.timeout();
        }
        try {
            while (items.isEmpty() && !closed) {
                long remainingNanos = remainingNanos(deadlineNanos);
                if (remainingNanos <= 0L) {
                    return DrainResult.timeout();
                }
                notEmpty.awaitNanos(remainingNanos);
            }
            int drained = drainAvailable(target, maxItems);
            if (drained > 0) {
                notFull.signalAll();
                return DrainResult.items(drained);
            }
            if (closed) {
                return DrainResult.closed();
            }
            throw new AssertionError("Queue is open and empty after timed drain wait");
        } finally {
            lock.unlock();
        }
    }

    DrainResult drainTo(Collection<? super T> target, int maxItems) throws InterruptedException {
        Objects.requireNonNull(target, "target");
        if (maxItems <= 0) {
            throw new IllegalArgumentException("maxItems must be > 0");
        }

        lock.lockInterruptibly();
        try {
            while (items.isEmpty() && !closed) {
                notEmpty.await();
            }
            int drained = drainAvailable(target, maxItems);
            if (drained > 0) {
                notFull.signalAll();
                return DrainResult.items(drained);
            }
            if (closed) {
                return DrainResult.closed();
            }
            throw new AssertionError("Queue is open and empty after blocking drain wait");
        } finally {
            lock.unlock();
        }
    }

    private int drainAvailable(Collection<? super T> target, int maxItems) {
        int drained = 0;
        while (drained < maxItems) {
            T item = items.peekFirst();
            if (item == null) {
                return drained;
            }
            if (!target.add(item)) {
                throw new IllegalStateException("Target collection rejected drained item");
            }
            items.removeFirst();
            drained++;
        }
        return drained;
    }

    private boolean tryLockUntil(long deadlineNanos, long timeoutNanos) throws InterruptedException {
        if (timeoutNanos <= 0L) {
            return lock.tryLock();
        }
        long remainingNanos = remainingNanos(deadlineNanos);
        if (remainingNanos <= 0L) {
            return false;
        }
        return lock.tryLock(remainingNanos, TimeUnit.NANOSECONDS);
    }

    private long deadlineFromNow(long timeoutNanos) {
        return System.nanoTime() + timeoutNanos;
    }

    private long remainingNanos(long deadlineNanos) {
        return deadlineNanos - System.nanoTime();
    }

    PollResult<T> poll() throws InterruptedException {
        lock.lockInterruptibly();
        try {
            while (items.isEmpty() && !closed) {
                notEmpty.await();
            }
            T item = items.pollFirst();
            if (item != null) {
                notFull.signal();
                return PollResult.item(item);
            }
            if (closed) {
                return PollResult.closed();
            }
            throw new AssertionError("Queue is open and empty after blocking poll wait");
        } finally {
            lock.unlock();
        }
    }

    void close() {
        lock.lock();
        try {
            closed = true;
            notEmpty.signalAll();
            notFull.signalAll();
        } finally {
            lock.unlock();
        }
    }

    int size() {
        lock.lock();
        try {
            return items.size();
        } finally {
            lock.unlock();
        }
    }

    static final class DrainResult {
        private static final DrainResult TIMEOUT = new DrainResult(Status.TIMEOUT, 0);
        private static final DrainResult CLOSED = new DrainResult(Status.CLOSED, 0);

        private final Status status;
        private final int count;

        private DrainResult(Status status, int count) {
            this.status = status;
            this.count = count;
        }

        static DrainResult items(int count) {
            if (count <= 0) {
                throw new IllegalArgumentException("count must be > 0");
            }
            return new DrainResult(Status.ITEM, count);
        }

        static DrainResult timeout() {
            return TIMEOUT;
        }

        static DrainResult closed() {
            return CLOSED;
        }

        boolean hasItems() {
            return status == Status.ITEM;
        }

        boolean ok() {
            return hasItems();
        }

        boolean isTimeout() {
            return status == Status.TIMEOUT;
        }

        boolean isClosed() {
            return status == Status.CLOSED;
        }

        int count() {
            if (!hasItems()) {
                throw new IllegalStateException("Drain result does not contain items");
            }
            return count;
        }
    }

    static final class PollResult<T> {
        private static final PollResult<?> TIMEOUT = new PollResult<>(Status.TIMEOUT, null);
        private static final PollResult<?> CLOSED = new PollResult<>(Status.CLOSED, null);

        private final Status status;
        private final T item;

        private PollResult(Status status, T item) {
            this.status = status;
            this.item = item;
        }

        static <T> PollResult<T> item(T item) {
            return new PollResult<>(Status.ITEM, item);
        }

        @SuppressWarnings("unchecked")
        static <T> PollResult<T> timeout() {
            return (PollResult<T>) TIMEOUT;
        }

        @SuppressWarnings("unchecked")
        static <T> PollResult<T> closed() {
            return (PollResult<T>) CLOSED;
        }

        boolean hasItem() {
            return status == Status.ITEM;
        }

        boolean ok() {
            return hasItem();
        }

        boolean isTimeout() {
            return status == Status.TIMEOUT;
        }

        boolean isClosed() {
            return status == Status.CLOSED;
        }

        T item() {
            if (!hasItem()) {
                throw new IllegalStateException("Poll result does not contain an item");
            }
            return item;
        }
    }

    enum Status {
        ITEM,
        TIMEOUT,
        CLOSED
    }

    static final class OfferResult {
        private static final OfferResult ACCEPTED = new OfferResult(OfferStatus.ACCEPTED);
        private static final OfferResult TIMEOUT = new OfferResult(OfferStatus.TIMEOUT);
        private static final OfferResult CLOSED = new OfferResult(OfferStatus.CLOSED);

        private final OfferStatus status;

        private OfferResult(OfferStatus status) {
            this.status = status;
        }

        static OfferResult accepted() {
            return ACCEPTED;
        }

        static OfferResult timeout() {
            return TIMEOUT;
        }

        static OfferResult closed() {
            return CLOSED;
        }

        boolean isAccepted() {
            return status == OfferStatus.ACCEPTED;
        }

        boolean ok() {
            return isAccepted();
        }

        boolean isTimeout() {
            return status == OfferStatus.TIMEOUT;
        }

        boolean isClosed() {
            return status == OfferStatus.CLOSED;
        }
    }

    enum OfferStatus {
        ACCEPTED,
        TIMEOUT,
        CLOSED
    }
}
