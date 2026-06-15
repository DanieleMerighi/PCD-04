package pcd.server;

import com.rabbitmq.client.*;
import pcd.messages.AcquireRequest;
import pcd.messages.GrantMessage;
import pcd.messages.ReleaseRequest;

import java.io.*;
import java.util.*;

public class RabbitMQLockServer {

    private static final String REQUEST_EXCHANGE = "lock_requests_exchange";
    private static final String REQUEST_QUEUE = "lock_requests_queue";
    private static final String GRANT_EXCHANGE = "lock_grants_exchange";

    private final Channel channel;
    private final Connection connection;

    private final Map<String, String> resourceOwners = new HashMap<>();
    private final Map<String, Queue<AcquireRequest>> waitingQueues = new HashMap<>();
    
    private volatile boolean running = true;

    public RabbitMQLockServer(String rabbitmqHost) throws Exception {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost(rabbitmqHost);
        this.connection = factory.newConnection();
        this.channel = connection.createChannel();

        // Dichiara gli exchange e le code
        channel.exchangeDeclare(REQUEST_EXCHANGE, "direct", true);
        channel.queueDeclare(REQUEST_QUEUE, true, false, false, null);
        channel.queueBind(REQUEST_QUEUE, REQUEST_EXCHANGE, "acquire");
        channel.queueBind(REQUEST_QUEUE, REQUEST_EXCHANGE, "release");

        channel.exchangeDeclare(GRANT_EXCHANGE, "direct", true);

        System.out.println("[Server] lock manager avviato");
    }

    public void start() throws Exception {
        System.out.println("[Server] in ascolto su: " + REQUEST_QUEUE);

        DeliverCallback deliverCallback = (consumerTag, delivery) -> {
            try {
                byte[] body = delivery.getBody();
                String routingKey = delivery.getEnvelope().getRoutingKey();

                if ("acquire".equals(routingKey)) {
                    handleAcquireRequest(body);
                } else if ("release".equals(routingKey)) {
                    handleReleaseRequest(body);
                }

            } catch (Exception e) {
                System.err.println("[Server] errore nel processing del messaggio: " + e.getMessage());
                e.printStackTrace();
            }
        };

        boolean autoAck = true;
        channel.basicConsume(REQUEST_QUEUE, autoAck, deliverCallback, consTag -> {});

        // Blocca il thread principale
        synchronized (this) {
            while (running) {
                this.wait();
            }
        }
    }

    private void handleAcquireRequest(byte[] body) throws Exception {
        ByteArrayInputStream bais = new ByteArrayInputStream(body);
        ObjectInputStream ois = new ObjectInputStream(bais);
        AcquireRequest request = (AcquireRequest) ois.readObject();
        ois.close();

        String resourceId = request.getResourceId();
        String processId = request.getProcessId();
        String replyQueue = request.getReplyQueueName();

        System.out.println("[Server] Acquire: " + processId + " richiede " + resourceId);

        synchronized (this) {
            String owner = resourceOwners.get(resourceId);

            if (owner == null) {
                // La risorsa è libera: assegna il lock subito
                resourceOwners.put(resourceId, processId);
                sendGrant(resourceId, processId, replyQueue, true);
                System.out.println("[Server] Lock concesso a " + processId + " per " + resourceId);

            } else {
                // La risorsa è occupata: metti in coda
                Queue<AcquireRequest> waitingQueue = waitingQueues.computeIfAbsent(
                    resourceId, 
                    k -> new LinkedList<>()
                );
                waitingQueue.offer(request);
                System.out.println("[Server] " + processId + " in attesa per " + resourceId +
                                 " (coda: " + waitingQueue.size() + ")");
            }
        }
    }

    private void handleReleaseRequest(byte[] body) throws Exception {
        ByteArrayInputStream bais = new ByteArrayInputStream(body);
        ObjectInputStream ois = new ObjectInputStream(bais);
        ReleaseRequest request = (ReleaseRequest) ois.readObject();
        ois.close();

        String resourceId = request.getResourceId();
        String processId = request.getProcessId();

        System.out.println("[Server] Release: " + processId + " rilascia " + resourceId);

        synchronized (this) {
            String owner = resourceOwners.get(resourceId);

            if (owner != null && owner.equals(processId)) {
                resourceOwners.remove(resourceId);
                System.out.println("[Server] Lock rilasciato da " + processId + " per " + resourceId);

                // Controlla se c'è qualcuno in attesa
                Queue<AcquireRequest> waitingQueue = waitingQueues.get(resourceId);
                if (waitingQueue != null && !waitingQueue.isEmpty()) {
                    AcquireRequest nextRequest = waitingQueue.poll();
                    resourceOwners.put(resourceId, nextRequest.getProcessId());
                    sendGrant(resourceId, nextRequest.getProcessId(), 
                             nextRequest.getReplyQueueName(), true);
                    System.out.println("[Server] Lock concesso a " + nextRequest.getProcessId() +
                                     " per " + resourceId);

                    if (waitingQueue.isEmpty()) {
                        waitingQueues.remove(resourceId);
                    }
                }

            } else {
                System.err.println("[Server] Errore: " + processId + " non possiede " + resourceId);
            }
        }
    }

    private void sendGrant(String resourceId, String processId, String replyQueue, 
                          boolean granted) throws IOException {
        GrantMessage grant = new GrantMessage(resourceId, processId, granted, 
                                             granted ? "Lock concesso" : "Lock negato");

        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        ObjectOutputStream oos = new ObjectOutputStream(baos);
        oos.writeObject(grant);
        oos.close();
        byte[] messageBody = baos.toByteArray();

        channel.basicPublish(GRANT_EXCHANGE, processId, null, messageBody);
    }

    public void stop() {
        running = false;
        synchronized (this) {
            this.notify();
        }
        try {
            channel.close();
            connection.close();
            System.out.println("[Server] Server chiuso.");
        } catch (Exception e) {
            System.err.println("[Server] Errore nella chiusura del server: " + e.getMessage());
        }
    }

    public static void main(String[] args) throws Exception {
        String rabbitmqHost = "localhost";
        if (args.length > 0) {
            rabbitmqHost = args[0];
        }

        RabbitMQLockServer server = new RabbitMQLockServer(rabbitmqHost);
        Runtime.getRuntime().addShutdownHook(new Thread(server::stop));
        server.start();
    }
}
