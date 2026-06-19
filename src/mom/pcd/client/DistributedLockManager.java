package pcd.client;

import pcd.util.LockTarget;

/**
 * Client interface for acquiring and releasing distributed locks.
 * <p>
 * Implementations of this interface coordinate with a centralized Message-Oriented
 * Middleware to enforce mutual exclusion across distributed processes.
 */
public interface DistributedLockManager {

    /**
     * Requests a lock for the specified target and blocks the current thread until the lock is granted.
     *
     * @param target the hierarchical lock identifier to acquire
     * @throws InterruptedException if the thread is interrupted while waiting, the request times out, or the lock is explicitly denied
     */
    void acquire(LockTarget target) throws InterruptedException;

    /**
     * Releases a previously acquired lock for the specified target.
     *
     * @param target the hierarchical lock identifier to release
     */
    void release(LockTarget target);

    /**
     * Closes the underlying network connections and message channels.
     */
    void close();
}
