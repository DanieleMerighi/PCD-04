package pcd;

import pcd.client.RabbitMQLockManagerClient;

public class DistributedProcess {

    public static final int WORK_DURATION = 5000;
    private final String processId;
    private final RabbitMQLockManagerClient lockManager;

    public DistributedProcess(String processId, String rabbitmqHost) throws Exception {
        this.processId = processId;
        this.lockManager = new RabbitMQLockManagerClient(processId, rabbitmqHost);
    }

    public void executeCriticalSection(String resourceId, long workDurationMs) throws InterruptedException {
        System.out.println("\n[" + processId + "]" + " tenta di acquisire " + resourceId);
        try {
            lockManager.acquire(resourceId);
        } catch (InterruptedException e) {
            System.out.println("[" + processId + "]" + " timeout nell'acquisizione del lock per la risorsa " + resourceId);
            throw e;
        }
        System.out.println("[" + processId + "]" + " - SEZIONE CRITICA INIZIATA per " + resourceId);

        try { // Simula il lavoro critico
            Thread.sleep(workDurationMs);
        } catch (InterruptedException e) {
            System.err.println("[" + processId + "]" + " interrotto durante il lavoro critico");
            throw e;
        }
        
        System.out.println("[" + processId + "]" + " - SEZIONE CRITICA TERMINATA per " + resourceId);
        lockManager.release(resourceId);
        System.out.println("[" + processId + "]" + " rilasciato " + resourceId);
    }

    public void close() {
        lockManager.close();
    }

    public static void main(String[] args) throws Exception {
        String processId = args.length > 0 ? args[0] : "Process1";
        String resourceId = args.length > 1 ? args[1] : "resource_1";
        String rabbitmqHost = args.length > 2 ? args[2] : "localhost";

        DistributedProcess process = new DistributedProcess(processId, rabbitmqHost);

        try {
            process.executeCriticalSection(resourceId, WORK_DURATION);
        } finally {
            process.close();
        }
    }
}
