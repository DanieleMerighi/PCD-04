package pcd;

import pcd.util.LockTarget;

/*
 * Test class to verify fair parallelism
 */
public class FairParallelismTest {

    public static void main(String[] args) throws Exception {
        String rabbitmqHost = args.length > 0 ? args[0] : "localhost";

        var proc1 = new DistributedProcess("Proc-1", rabbitmqHost);
        var proc2 = new DistributedProcess("Proc-2", rabbitmqHost);
        var proc3 = new DistributedProcess("Proc-3", rabbitmqHost);

        System.out.println("Avvio test di parallelismo equo...");

        try {
            LockTarget targetA = LockTarget.GLOBAL.sub("db").sub("tableA");
            LockTarget targetB = LockTarget.GLOBAL.sub("db").sub("tableB");

            System.out.println("Starting proc1 execution");
            var task1 = proc1.executeAsync(targetA, 5000);
            Thread.sleep(500);

            System.out.println("Starting proc2 execution");
            var task2 = proc2.executeAsync(targetA, 2000);
            Thread.sleep(500);

            System.out.println("Starting proc3 execution");
            var task3 = proc3.executeAsync(targetB, 6000);

            task1.get();
            task2.get();
            task3.get();

        } finally {
            proc1.close();
            proc2.close();
            proc3.close();
        }
    }
}