package pcd.client;

import pcd.util.LockTarget;

public interface DistributedLockManager {

    void acquire(LockTarget target) throws InterruptedException;

    void release(LockTarget target);

    void close();
}
