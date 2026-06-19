package pcd;

import pcd.client.DistributedLockManager;
import pcd.client.RabbitMQLockManagerClient;
import pcd.util.LockTarget;

import java.util.concurrent.FutureTask;

/*
 * Test class to simulate lock usage
 */
public class DistributedProcess {

    public static final int WORK_DURATION = 2000;
    private final String processId;
    private final DistributedLockManager lockManager;

    public DistributedProcess(String processId, String rabbitmqHost) throws Exception {
        this.processId = processId;
        this.lockManager = new RabbitMQLockManagerClient(processId, rabbitmqHost);
    }

    public Void executeCriticalSection(LockTarget target, long workDurationMs) throws InterruptedException {
        boolean acquired = false;
        try {
            System.out.printf("[%s] Requesting lock for %s%n", processId, target);
            lockManager.acquire(target);
            acquired = true;
            System.out.printf("[%s] ENTER critical section for %s%n", processId, target);
            Thread.sleep(workDurationMs);
            System.out.printf("[%s] EXIT critical section for %s%n", processId, target);
            return null;
        } catch (InterruptedException e) {
            System.out.printf("[%s] Interrupted while waiting for lock for %s%n", processId, target);
            throw e;
        } finally {
            if (acquired) {
                System.out.printf("[%s] Releasing lock for %s%n", processId, target);
                lockManager.release(target);
            }
        }
    }

    public FutureTask<Void> executeAsync(LockTarget target) {
        return executeAsync(target, WORK_DURATION);
    }

    public FutureTask<Void> executeAsync(LockTarget target, long workDurationMs) {
        FutureTask<Void> task = new FutureTask<>(() -> executeCriticalSection(target, workDurationMs));
        Thread.ofVirtual().start(task);
        return task;
    }

    public void close() {
        lockManager.close();
    }

    public static void main(String[] args) throws Exception {
        String processId = args.length > 0 ? args[0] : "Process1";
        String rabbitmqHost = args.length > 1 ? args[1] : "localhost";
        LockTarget target = args.length > 2 ? LockTarget.of(args[2]) : LockTarget.GLOBAL.sub("database").sub("resource1");
        long workDurationMs = args.length > 3 ? Long.parseLong(args[3]) : WORK_DURATION;

        var process = new DistributedProcess(processId, rabbitmqHost);

        try {
            process.executeCriticalSection(target, workDurationMs);
        } finally {
            process.close();
        }
    }
}
