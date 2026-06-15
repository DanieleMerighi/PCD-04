package pcd;

import pcd.client.RabbitMQLockManagerClient;
import pcd.util.LockTarget;

public class DistributedProcess {

    public static final int WORK_DURATION = 5000;
    private final String processId;
    private final RabbitMQLockManagerClient lockManager;

    public DistributedProcess(String processId, String rabbitmqHost) throws Exception {
        this.processId = processId;
        this.lockManager = new RabbitMQLockManagerClient(processId, rabbitmqHost);
    }

    public void executeCriticalSection(LockTarget target, long workDurationMs) throws InterruptedException {
        System.out.printf("%n[%s] Attempting to acquire: %s%n", processId, target);

        try {
            lockManager.acquire(target);
        } catch (InterruptedException e) {
            System.err.printf("[%s] Timeout acquiring lock for: %s%n", processId, target);
            throw e;
        }

        System.out.printf("[%s] CRITICAL SECTION STARTED for: %s%n", processId, target);

        try {
            Thread.sleep(workDurationMs);
        } catch (InterruptedException e) {
            System.err.printf("[%s] Interrupted during critical work.%n", processId);
            throw e;
        }

        System.out.printf("[%s] CRITICAL SECTION ENDED for: %s%n", processId, target);
        lockManager.release(target);
    }

    public void close() {
        lockManager.close();
    }

    public static void main(String[] args) throws Exception {
        String processId = args.length > 0 ? args[0] : "Process1";
        String rabbitmqHost = args.length > 1 ? args[1] : "localhost";

        // Dynamic, hierarchical target selection
        LockTarget target = LockTarget.GLOBAL.sub("database").sub("table_users");

        DistributedProcess process = new DistributedProcess(processId, rabbitmqHost);

        try {
            process.executeCriticalSection(target, WORK_DURATION);
        } finally {
            process.close();
        }
    }
}