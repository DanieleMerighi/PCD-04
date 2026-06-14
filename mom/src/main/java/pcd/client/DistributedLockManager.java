package pcd.client;

public interface DistributedLockManager {

    void acquire(String resourceId) throws InterruptedException;

    void release(String resourceId);

    void close();
}
