package pcd;

import pcd.util.LockTarget;

import java.util.List;
import java.util.concurrent.FutureTask;

/*
 * Test class to simulate concurrent lock usage
 */
public class ConcurrentLockDemo {

    public static void main(String[] args) throws Exception {
        String rabbitmqHost = args.length > 0 ? args[0] : "localhost";

        var globalProcess = new DistributedProcess("global-process", rabbitmqHost);
        var independentA = new DistributedProcess("proc-a", rabbitmqHost);
        var independentB = new DistributedProcess("proc-b", rabbitmqHost);
        var independentC = new DistributedProcess("proc-c", rabbitmqHost);
        System.out.println();

        try {
            List<FutureTask<Void>> tasks = List.of(
                    globalProcess.executeAsync(LockTarget.GLOBAL),
                    independentA.executeAsync(LockTarget.GLOBAL.sub("database").sub("resource1")),
                    independentB.executeAsync(LockTarget.GLOBAL.sub("database").sub("resource2")),
                    independentC.executeAsync(LockTarget.GLOBAL.sub("database").sub("resource3"))
            );

            for (FutureTask<Void> task : tasks) {
                task.get();
            }
        } finally {
            globalProcess.close();
            independentA.close();
            independentB.close();
            independentC.close();
        }
    }
}