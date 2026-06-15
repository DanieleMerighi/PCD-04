package pcd.server;

import com.rabbitmq.client.*;
import pcd.messages.AcquireRequest;
import pcd.messages.GrantMessage;
import pcd.messages.ReleaseRequest;
import pcd.util.LockTarget;
import pcd.util.RabbitConfig;

import java.io.IOException;
import java.util.HashMap;
import java.util.Deque;
import java.util.LinkedList;
import java.util.Map;

import static pcd.util.Serialize.serialize;
import static pcd.util.Serialize.deserialize;

public class RabbitMQLockServer {

    private final Channel channel;
    private final Connection connection;

    private final Map<String, String> resourceOwners = new HashMap<>();
    private final Deque<AcquireRequest> waitingRequests = new LinkedList<>();

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
        System.out.println("[Server] Listening for lock requests on queue: " + RabbitConfig.REQUEST_QUEUE);

        channel.basicQos(1);
        DeliverCallback deliverCallback = (consumerTag, delivery) -> {
            try {
                var body = delivery.getBody();
                var routingKey = delivery.getEnvelope().getRoutingKey();

                if (RabbitConfig.ROUTING_ACQUIRE.equals(routingKey)) {
                    handleAcquireRequest(body);
                } else if (RabbitConfig.ROUTING_RELEASE.equals(routingKey)) {
                    handleReleaseRequest(body);
                }
            } catch (Exception e) {
                System.err.println("[Server] Message processing error: " + e.getMessage());
            } finally {
                channel.basicAck(delivery.getEnvelope().getDeliveryTag(), false);
            }
        };

        boolean autoAck = false;
        channel.basicConsume(RabbitConfig.REQUEST_QUEUE, autoAck, deliverCallback, consTag -> {});

        synchronized (this) {
            while (running) {
                this.wait();
            }
        }
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


    private void handleAcquireRequest(byte[] body) throws Exception {
        AcquireRequest request = (AcquireRequest) deserialize(body);
        String resourceId = request.resourceId();
        String processId = request.processId();

        System.out.printf("[Server] ACQUIRE received: process=%s resource=%s%n", processId, resourceId);

        synchronized (this) {
            String blockingOwner = findBlockingOwner(resourceId);

            if (blockingOwner == null && waitingRequests.isEmpty()) { // Necessary to prevent starvation, even if a process could go it will wait if the queue is not empty
                resourceOwners.put(resourceId, processId);
                sendGrant(resourceId, processId);
                System.out.printf("[Server] GRANTED: process=%s resource=%s%n", processId, resourceId);
            } else {
                waitingRequests.addLast(request);
                System.out.printf("[Server] QUEUED: process=%s resource=%s blockedBy=%s queueDepth=%d%n",
                        processId, resourceId, blockingOwner == null ? "earlier waiting request" : blockingOwner, waitingRequests.size());
            }
        }
    }

    private void handleReleaseRequest(byte[] body) throws Exception {
        ReleaseRequest request = (ReleaseRequest) deserialize(body);
        String resourceId = request.resourceId();
        String processId = request.processId();

        System.out.printf("[Server] RELEASE received: process=%s resource=%s%n", processId, resourceId);

        synchronized (this) {
            String owner = resourceOwners.get(resourceId);

            if (processId.equals(owner)) {
                resourceOwners.remove(resourceId);
                System.out.printf("[Server] RELEASED: process=%s resource=%s%n", processId, resourceId);
                grantWaitingRequests();
            } else {
                System.err.printf("[Server] RELEASE ignored: process=%s does not own resource=%s currentOwner=%s%n",
                        processId, resourceId, owner);
            }
        }
    }

    private void grantWaitingRequests() throws IOException {
        while (true) {
            AcquireRequest nextRequest = waitingRequests.peekFirst();

            if (nextRequest == null) {
                return;
            }

            if (findBlockingOwner(nextRequest.resourceId()) != null) {
                return;
            }

            waitingRequests.removeFirst();
            resourceOwners.put(nextRequest.resourceId(), nextRequest.processId());
            sendGrant(nextRequest.resourceId(), nextRequest.processId());
            System.out.printf("[Server] GRANTED FROM QUEUE: process=%s resource=%s remaining=%d%n",
                    nextRequest.processId(), nextRequest.resourceId(), waitingRequests.size());
        }
    }

    private String findBlockingOwner(String resourceId) {
        for (Map.Entry<String, String> entry : resourceOwners.entrySet()) {
            String ownedResourceId = entry.getKey();
            if (LockTarget.conflicts(resourceId, ownedResourceId)) {
                return entry.getValue() + " on " + ownedResourceId;
            }
        }

        return null;
    }

    private void sendGrant(String resourceId, String processId) throws IOException {
        GrantMessage grant = new GrantMessage(resourceId, processId, true, "Lock Granted");
        channel.basicPublish(RabbitConfig.GRANT_EXCHANGE, processId, null, serialize(grant));
    }

    public static void main(String[] args) throws Exception {
        String rabbitmqHost = args.length > 0 ? args[0] : "localhost";
        RabbitMQLockServer server = new RabbitMQLockServer(rabbitmqHost);
        Runtime.getRuntime().addShutdownHook(new Thread(server::stop));
        server.start();
    }
}