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

            // 1. Proc-1 acquisisce targetA per 6 secondi
            var task1 = proc1.executeAsync(targetA, 6000);
            Thread.sleep(500); // Attesa per garantire l'ordine di ricezione sul server

            // 2. Proc-2 richiede targetA. Viene accodato (conflitto con Proc-1).
            var task2 = proc2.executeAsync(targetA, 2000);
            Thread.sleep(500);

            // 3. Proc-3 richiede targetB.
            // Logica precedente: bloccato a causa della coda non vuota.
            // Nuova logica: acquisito immediatamente (nessun conflitto con targetA).
            var task3 = proc3.executeAsync(targetB, 2000);

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