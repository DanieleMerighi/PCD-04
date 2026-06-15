package pcd.client;

import com.rabbitmq.client.*;
import pcd.messages.AcquireRequest;
import pcd.messages.GrantMessage;
import pcd.messages.ReleaseRequest;

import java.io.*;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

/**
 * Usa il pattern direct routing:
 * - Scrive le richieste di acquire/release su una coda di richieste
 * - Ascolta una coda privata di risposta per ricevere i grant
 * - Serializza i messaggi Java come byte array
 */
public class RabbitMQLockManagerClient implements DistributedLockManager {

    private static final String REQUEST_EXCHANGE = "lock_requests_exchange";
    private static final String REQUEST_QUEUE = "lock_requests_queue";
    private static final String GRANT_EXCHANGE = "lock_grants_exchange";
    private static final long TIMEOUT_MS = 30000; // timeout di 30 secondi per acquire

    private final String processId;
    private final String replyQueueName;
    private final Connection connection;
    private final Channel requestChannel;
    private final Channel replyChannel;
    private final BlockingQueue<GrantMessage> grantQueue;
    private final Thread listenerThread;

    public RabbitMQLockManagerClient(String processId, String rabbitmqHost) throws Exception {
        this.processId = processId;
        this.grantQueue = new LinkedBlockingQueue<>();

        // Connessione a RabbitMQ
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost(rabbitmqHost);
        this.connection = factory.newConnection();
        this.requestChannel = connection.createChannel();
        this.replyChannel = connection.createChannel();

        // Dichiara l'exchange per le richieste (direct routing)
        requestChannel.exchangeDeclare(REQUEST_EXCHANGE, "direct", true);
        requestChannel.queueDeclare(REQUEST_QUEUE, true, false, false, null);
        requestChannel.queueBind(REQUEST_QUEUE, REQUEST_EXCHANGE, "acquire");
        requestChannel.queueBind(REQUEST_QUEUE, REQUEST_EXCHANGE, "release");

        // Dichiara l'exchange per i grant (direct routing)
        replyChannel.exchangeDeclare(GRANT_EXCHANGE, "direct", true);
        this.replyQueueName = "reply_" + processId;
        replyChannel.queueDeclare(replyQueueName, false, true, false, null);
        replyChannel.queueBind(replyQueueName, GRANT_EXCHANGE, processId);

        // Avvia il thread listener per ricevere i grant
        this.listenerThread = new Thread(this::listenForGrants);
        this.listenerThread.setDaemon(true);
        this.listenerThread.start();

        System.out.println("[Client] " + processId + " pronto. Reply queue: " + replyQueueName);
    }

    @Override
    public void acquire(String resourceId) throws InterruptedException {
        try {
            // Crea la richiesta
            AcquireRequest request = new AcquireRequest(resourceId, processId, replyQueueName);

            // Serializza in byte array
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            ObjectOutputStream oos = new ObjectOutputStream(baos);
            oos.writeObject(request);
            oos.close();
            byte[] messageBody = baos.toByteArray();

            // Pubblica la richiesta
            requestChannel.basicPublish(REQUEST_EXCHANGE, "acquire", null, messageBody);
            System.out.println("[Client] " + processId + " - Richiesta acquire per: " + resourceId);

            // Attende il grant
            GrantMessage grant = grantQueue.poll(TIMEOUT_MS, TimeUnit.MILLISECONDS);
            if (grant == null) {
                throw new InterruptedException("Timeout in attesa del lock per: " + resourceId);
            }

            if (!grant.isGranted()) {
                throw new InterruptedException("Lock denied for: " + resourceId);
            }

            System.out.println("[Client] " + processId + " - Lock acquisito per: " + resourceId);

        } catch (IOException e) {
            throw new InterruptedException("[Client] Errore nel acquisire il lock: " + e.getMessage());
        }
    }

    @Override
    public void release(String resourceId) {
        try {
            ReleaseRequest request = new ReleaseRequest(resourceId, processId);

            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            ObjectOutputStream oos = new ObjectOutputStream(baos);
            oos.writeObject(request);
            oos.close();
            byte[] messageBody = baos.toByteArray();

            requestChannel.basicPublish(REQUEST_EXCHANGE, "release", null, messageBody);
            System.out.println("[Client] " + processId + " - Richiesta release per: " + resourceId);

        } catch (IOException e) {
            System.err.println("[Client] Errore nel rilasciare il lock: " + e.getMessage());
        }
    }

    private void listenForGrants() {
        try {
            DeliverCallback deliverCallback = (consumerTag, delivery) -> {
                try {
                    byte[] body = delivery.getBody();
                    ByteArrayInputStream bais = new ByteArrayInputStream(body);
                    ObjectInputStream ois = new ObjectInputStream(bais);
                    GrantMessage grant = (GrantMessage) ois.readObject();
                    ois.close();

                    grantQueue.offer(grant);
                    System.out.println("[Client] " + processId + " - Ricevuto grant per: " + grant.getResourceId());

                } catch (ClassNotFoundException | IOException e) {
                    System.err.println("[Client] Errore deserializzazione grant: " + e.getMessage());
                }
            };

            boolean autoAck = true;
            replyChannel.basicConsume(replyQueueName, autoAck, deliverCallback, consTag -> {});

        } catch (IOException e) {
            System.err.println("[Client] Errore nel listener di grant: " + e.getMessage());
        }
    }

    @Override
    public void close() {
        try {
            replyChannel.close();
            requestChannel.close();
            connection.close();
            System.out.println("[Client] " + processId + " chiuso.");
        } catch (Exception e) {
            System.err.println("[Client] Errore nella chiusura: " + e.getMessage());
        }
    }
}
