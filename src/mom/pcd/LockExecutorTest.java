package pcd;

import pcd.client.DistributedLockManager;
import pcd.client.LockExecutor;
import pcd.client.RabbitMQLockManagerClient;
import pcd.util.LockTarget;

import java.util.concurrent.CompletableFuture;

/**
 * Test methods to execute callables inside a critical section
 */
public class LockExecutorTest {

    public static void main(String[] args) throws Exception {
        String rabbitmqHost = args.length > 0 ? args[0] : "localhost";
        DistributedLockManager lockManager = new RabbitMQLockManagerClient("Process1", rabbitmqHost);
        LockTarget target = LockTarget.GLOBAL.sub("database").sub("resource1");

        try {
            String result = LockExecutor.executeInLock(lockManager, target, () -> {
                Thread.sleep(2000);
                return "Dati letti dal database";
            });
            System.out.println("Risultato sincrono: " + result);
        } catch (Exception e) {
            System.err.println("Errore esecuzione sincrona: " + e.getMessage());
        }

        CompletableFuture<Integer> futureResult = LockExecutor.executeInLockAsync(lockManager, target, () -> {
            Thread.sleep(2000);
            return 42;
        });

        // System.out.println("Risultato asincrono: " + futureResult.join());

        futureResult.thenAccept(val -> System.out.println("Risultato asincrono ricevuto: " + val))
                .exceptionally(ex -> {
                    System.err.println("Errore asincrono: " + ex.getMessage());
                    return null;
                });
        Thread.sleep(6000);
        lockManager.close();
    }
}
