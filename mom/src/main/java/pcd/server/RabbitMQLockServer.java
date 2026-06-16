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
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static pcd.util.Serialize.serialize;
import static pcd.util.Serialize.deserialize;

public class RabbitMQLockServer {

    private record QueuedRequest(AcquireRequest request, AMQP.BasicProperties props) {}
    private record LockState(String processId, long expirationTimeMs) {}

    private static final long LEASE_DURATION_MS = 15000;

    private final Channel channel;
    private final Connection connection;

    private final Map<String, LockState> resourceOwners = new HashMap<>();
    private final Deque<QueuedRequest> waitingRequests = new LinkedList<>();

    private final ExecutorService processor = Executors.newSingleThreadExecutor();
    private final ScheduledExecutorService timer = Executors.newSingleThreadScheduledExecutor();

    public RabbitMQLockServer(String rabbitmqHost) throws Exception {
        var factory = new ConnectionFactory();
        factory.setHost(rabbitmqHost);
        this.connection = factory.newConnection();
        this.channel = connection.createChannel();

        channel.exchangeDeclare(RabbitConfig.REQUEST_EXCHANGE, BuiltinExchangeType.DIRECT, true);
        channel.queueDeclare(RabbitConfig.REQUEST_QUEUE, true, false, false, null);
        channel.queueBind(RabbitConfig.REQUEST_QUEUE, RabbitConfig.REQUEST_EXCHANGE, RabbitConfig.ROUTING_ACQUIRE);
        channel.queueBind(RabbitConfig.REQUEST_QUEUE, RabbitConfig.REQUEST_EXCHANGE, RabbitConfig.ROUTING_RELEASE);

        System.out.println("[Server] Lock manager started.");
    }

    public void start() throws Exception {
        System.out.println("[Server] Listening for lock requests on queue: " + RabbitConfig.REQUEST_QUEUE);
        channel.basicQos(1);
        DeliverCallback deliverCallback = (consumerTag, delivery) -> {
            processor.submit(() -> {
                try {
                    processMessage(delivery);
                } finally {
                    try {
                        channel.basicAck(delivery.getEnvelope().getDeliveryTag(), false);
                    } catch (IOException e) {
                        System.err.println("[Server] Ack error: " + e.getMessage());
                    }
                }
            });
        };
        var autoAck = false;
        channel.basicConsume(RabbitConfig.REQUEST_QUEUE, autoAck, deliverCallback, consTag -> {});
        timer.scheduleAtFixedRate(() -> processor.submit(this::evictExpiredLocks), 5, 5, TimeUnit.SECONDS);
    }

    public void stop() {
        processor.shutdown();
        timer.shutdown();
        try {
            channel.close();
            connection.close();
            System.out.println("[Server] Server closed.");
        } catch (Exception e) {
            System.err.println("[Server] Error during closure: " + e.getMessage());
        }
    }

    private void processMessage(Delivery delivery) {
        try {
            var props = delivery.getProperties();
            var body = delivery.getBody();
            var routingKey = delivery.getEnvelope().getRoutingKey();
            if (RabbitConfig.ROUTING_ACQUIRE.equals(routingKey)) {
                handleAcquireRequest(body, props);
            } else if (RabbitConfig.ROUTING_RELEASE.equals(routingKey)) {
                handleReleaseRequest(body);
            }
        } catch (Exception e) {
            System.err.println("[Server] Message processing error: " + e.getMessage());
        }
    }

    private void handleAcquireRequest(byte[] body, AMQP.BasicProperties props) throws Exception {
        var request = (AcquireRequest) deserialize(body);
        var resourceId = request.resourceId();
        var processId = request.processId();
        System.out.printf("[Server] ACQUIRE received: process=%s resource=%s%n", processId, resourceId);

        var blockingOwner = findBlockingOwner(resourceId);
        if (blockingOwner == null && waitingRequests.isEmpty()) {
            grantLock(resourceId, processId, props);
        } else {
            waitingRequests.addLast(new QueuedRequest(request, props));
            System.out.printf("[Server] QUEUED: process=%s resource=%s blockedBy=%s queueDepth=%d%n",
                    processId, resourceId, blockingOwner == null ? "earlier request" : blockingOwner, waitingRequests.size());
        }
    }

    private void grantLock(String resourceId, String processId, AMQP.BasicProperties props) throws IOException {
        var expiration = System.currentTimeMillis() + LEASE_DURATION_MS;
        resourceOwners.put(resourceId, new LockState(processId, expiration));
        sendGrant(props, resourceId, processId);
        System.out.printf("[Server] GRANTED: process=%s resource=%s (expires in %ds)%n",
                processId, resourceId, LEASE_DURATION_MS / 1000);
    }

    private void sendGrant(AMQP.BasicProperties requestProps, String resourceId, String processId) throws IOException {
        var grant = new GrantMessage(resourceId, processId, true, "Lock Granted");
        var replyProps = new AMQP.BasicProperties.Builder()
                .correlationId(requestProps.getCorrelationId())
                .build();
        channel.basicPublish("", requestProps.getReplyTo(), replyProps, serialize(grant));
    }

    private void handleReleaseRequest(byte[] body) throws Exception {
        var request = (ReleaseRequest) deserialize(body);
        var resourceId = request.resourceId();
        var processId = request.processId();

        System.out.printf("[Server] RELEASE received: process=%s resource=%s%n", processId, resourceId);
        var state = resourceOwners.get(resourceId);
        if (state != null && processId.equals(state.processId())) {
            resourceOwners.remove(resourceId);
            System.out.printf("[Server] RELEASED: process=%s resource=%s%n", processId, resourceId);
            grantWaitingRequests();
        } else {
            System.err.printf("[Server] RELEASE ignored: invalid owner process=%s resource=%s%n", processId, resourceId);
        }
    }

    private void grantWaitingRequests() throws IOException {
        while (true) {
            var next = waitingRequests.peekFirst();
            if (next == null) return;

            var nextRequest = next.request();
            if (findBlockingOwner(nextRequest.resourceId()) != null) return;

            waitingRequests.removeFirst();
            grantLock(nextRequest.resourceId(), nextRequest.processId(), next.props());
        }
    }

    private String findBlockingOwner(String resourceId) {
        for (var entry : resourceOwners.entrySet()) {
            var ownedResourceId = entry.getKey();
            if (LockTarget.conflicts(resourceId, ownedResourceId)) {
                return entry.getValue().processId() + " on " + ownedResourceId;
            }
        }
        return null;
    }

    private void evictExpiredLocks() {
        var now = System.currentTimeMillis();
        var evicted = false;

        var iterator = resourceOwners.entrySet().iterator();
        while (iterator.hasNext()) {
            var entry = iterator.next();
            if (now > entry.getValue().expirationTimeMs()) {
                System.out.printf("[Server] LEASE EXPIRED: process=%s resource=%s%n",
                        entry.getValue().processId(), entry.getKey());
                iterator.remove();
                evicted = true;
            }
        }
        if (evicted) {
            try {
                grantWaitingRequests();
            } catch (IOException e) {
                System.err.println("[Server] Error granting requests after eviction: " + e.getMessage());
            }
        }
    }

    public static void main(String[] args) throws Exception {
        var rabbitmqHost = args.length > 0 ? args[0] : "localhost";
        var server = new RabbitMQLockServer(rabbitmqHost);
        Runtime.getRuntime().addShutdownHook(new Thread(server::stop));
        server.start();
    }
}