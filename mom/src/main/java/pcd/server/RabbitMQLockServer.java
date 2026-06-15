package pcd.server;

import com.rabbitmq.client.*;
import pcd.messages.AcquireRequest;
import pcd.messages.GrantMessage;
import pcd.messages.ReleaseRequest;
import pcd.util.RabbitConfig;

import java.io.*;
import java.util.*;

import static pcd.util.Serialize.serialize;
import static pcd.util.Serialize.deserialize;

public class RabbitMQLockServer {

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

        channel.exchangeDeclare(RabbitConfig.REQUEST_EXCHANGE, "direct", true);
        channel.queueDeclare(RabbitConfig.REQUEST_QUEUE, true, false, false, null);
        channel.queueBind(RabbitConfig.REQUEST_QUEUE, RabbitConfig.REQUEST_EXCHANGE, RabbitConfig.ROUTING_ACQUIRE);
        channel.queueBind(RabbitConfig.REQUEST_QUEUE, RabbitConfig.REQUEST_EXCHANGE, RabbitConfig.ROUTING_RELEASE);

        channel.exchangeDeclare(RabbitConfig.GRANT_EXCHANGE, "direct", true);

        System.out.println("[Server] Lock manager started.");
    }

    public void start() throws Exception {
        System.out.println("[Server] Listening on: " + RabbitConfig.REQUEST_QUEUE);

        DeliverCallback deliverCallback = (consumerTag, delivery) -> {
            try {
                byte[] body = delivery.getBody();
                String routingKey = delivery.getEnvelope().getRoutingKey();

                if (RabbitConfig.ROUTING_ACQUIRE.equals(routingKey)) {
                    handleAcquireRequest(body);
                } else if (RabbitConfig.ROUTING_RELEASE.equals(routingKey)) {
                    handleReleaseRequest(body);
                }
            } catch (Exception e) {
                System.err.println("[Server] Message processing error: " + e.getMessage());
            }
        };

        boolean autoAck = true;
        channel.basicConsume(RabbitConfig.REQUEST_QUEUE, autoAck, deliverCallback, consTag -> {});

        synchronized (this) {
            while (running) {
                this.wait();
            }
        }
    }

    private void handleAcquireRequest(byte[] body) throws Exception {
        AcquireRequest request = (AcquireRequest) deserialize(body);
        String resourceId = request.resourceId();
        String processId = request.processId();

        System.out.printf("[Server] Acquire: %s requests %s%n", processId, resourceId);

        synchronized (this) {
            String owner = resourceOwners.get(resourceId);

            if (owner == null) {
                resourceOwners.put(resourceId, processId);
                sendGrant(resourceId, processId);
                System.out.printf("[Server] Lock granted to %s for %s%n", processId, resourceId);
            } else {
                Queue<AcquireRequest> waitingQueue = waitingQueues.computeIfAbsent(resourceId, k -> new LinkedList<>());
                waitingQueue.offer(request);
                System.out.printf("[Server] %s enqueued for %s (Queue depth: %d)%n", processId, resourceId, waitingQueue.size());
            }
        }
    }

    private void handleReleaseRequest(byte[] body) throws Exception {
        ReleaseRequest request = (ReleaseRequest) deserialize(body);
        String resourceId = request.resourceId();
        String processId = request.processId();

        System.out.printf("[Server] Release: %s releases %s%n", processId, resourceId);

        synchronized (this) {
            String owner = resourceOwners.get(resourceId);

            if (processId.equals(owner)) {
                resourceOwners.remove(resourceId);
                System.out.printf("[Server] Lock released by %s for %s%n", processId, resourceId);

                Queue<AcquireRequest> waitingQueue = waitingQueues.get(resourceId);
                if (waitingQueue != null && !waitingQueue.isEmpty()) {
                    AcquireRequest nextRequest = waitingQueue.poll();
                    resourceOwners.put(resourceId, nextRequest.processId());
                    sendGrant(resourceId, nextRequest.processId());
                    System.out.printf("[Server] Lock granted to next in queue %s for %s%n", nextRequest.processId(), resourceId);

                    if (waitingQueue.isEmpty()) {
                        waitingQueues.remove(resourceId);
                    }
                }
            } else {
                System.err.printf("[Server] Error: %s does not own %s%n", processId, resourceId);
            }
        }
    }

    private void sendGrant(String resourceId, String processId) throws IOException {
        GrantMessage grant = new GrantMessage(resourceId, processId, true, "Lock Granted");
        channel.basicPublish(RabbitConfig.GRANT_EXCHANGE, processId, null, serialize(grant));
    }

    public void stop() {
        running = false;
        synchronized (this) { this.notify(); }
        try {
            channel.close();
            connection.close();
            System.out.println("[Server] Server closed.");
        } catch (Exception e) {
            System.err.println("[Server] Error during closure: " + e.getMessage());
        }
    }

    public static void main(String[] args) throws Exception {
        String rabbitmqHost = args.length > 0 ? args[0] : "localhost";
        RabbitMQLockServer server = new RabbitMQLockServer(rabbitmqHost);
        Runtime.getRuntime().addShutdownHook(new Thread(server::stop));
        server.start();
    }
}