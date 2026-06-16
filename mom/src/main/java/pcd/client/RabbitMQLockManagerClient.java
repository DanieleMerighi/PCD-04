package pcd.client;

import com.rabbitmq.client.*;
import pcd.messages.AcquireRequest;
import pcd.messages.GrantMessage;
import pcd.messages.ReleaseRequest;
import pcd.util.LockTarget;
import pcd.util.RabbitConfig;

import java.io.IOException;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static pcd.util.Serialize.serialize;
import static pcd.util.Serialize.deserialize;

public class RabbitMQLockManagerClient implements DistributedLockManager {

    private final String processId;
    private final String replyQueueName;
    private final Connection connection;
    private final Channel requestChannel;
    private final Channel replyChannel;

    private final ConcurrentHashMap<String, CompletableFuture<GrantMessage>> pendingRequests = new ConcurrentHashMap<>();

    public RabbitMQLockManagerClient(String processId, String rabbitmqHost) throws Exception {
        this.processId = processId;

        var factory = new ConnectionFactory();
        factory.setHost(rabbitmqHost);
        this.connection = factory.newConnection();
        this.requestChannel = connection.createChannel();
        this.replyChannel = connection.createChannel();

        requestChannel.exchangeDeclare(RabbitConfig.REQUEST_EXCHANGE, BuiltinExchangeType.DIRECT, true);
        requestChannel.queueDeclare(RabbitConfig.REQUEST_QUEUE, true, false, false, null);
        requestChannel.queueBind(RabbitConfig.REQUEST_QUEUE, RabbitConfig.REQUEST_EXCHANGE, RabbitConfig.ROUTING_ACQUIRE);
        requestChannel.queueBind(RabbitConfig.REQUEST_QUEUE, RabbitConfig.REQUEST_EXCHANGE, RabbitConfig.ROUTING_RELEASE);

        this.replyQueueName = "reply_" + processId;
        replyChannel.queueDeclare(replyQueueName, false, true, true, null);

        replyChannel.basicConsume(replyQueueName, true, this::handleGrantDelivery, consTag -> {});

        System.out.printf("[%s] Client initialized. replyQueue=%s%n", processId, replyQueueName);
    }

    @Override
    public void acquire(LockTarget target) throws InterruptedException {
        var correlationId = UUID.randomUUID().toString();
        var future = new CompletableFuture<GrantMessage>();
        pendingRequests.put(correlationId, future);

        try {
            var request = new AcquireRequest(target.getPath(), processId);
            var props = new AMQP.BasicProperties.Builder()
                    .correlationId(correlationId)
                    .replyTo(replyQueueName)
                    .build();

            requestChannel.basicPublish(RabbitConfig.REQUEST_EXCHANGE, RabbitConfig.ROUTING_ACQUIRE, props, serialize(request));
            System.out.printf("[%s] Waiting for GRANT target=%s timeout=%dms%n", processId, target, RabbitConfig.ACQUIRE_TIMEOUT_MS);

            var grant = future.get(RabbitConfig.ACQUIRE_TIMEOUT_MS, TimeUnit.MILLISECONDS);

            if (!grant.granted()) {
                throw new InterruptedException("Lock denied for target: " + target);
            }
            System.out.printf("[%s] GRANT received for target=%s%n", processId, target);

        } catch (TimeoutException e) {
            pendingRequests.remove(correlationId);
            throw new InterruptedException("Timeout waiting for grant on target: " + target);
        } catch (Exception e) {
            pendingRequests.remove(correlationId);
            throw new InterruptedException(String.format("[%s] Error acquiring lock: %s", processId, e.getMessage()));
        }
    }

    @Override
    public void release(LockTarget target) {
        try {
            var request = new ReleaseRequest(target.getPath(), processId);
            requestChannel.basicPublish(RabbitConfig.REQUEST_EXCHANGE, RabbitConfig.ROUTING_RELEASE, null, serialize(request));
        } catch (IOException e) {
            System.err.printf("[%s] Error releasing lock: %s%n", processId, e.getMessage());
        }
    }

    @Override
    public void close() {
        try {
            replyChannel.close();
            requestChannel.close();
            connection.close();
            System.out.printf("[%s] Client closed.%n", processId);
        } catch (Exception e) {
            System.err.printf("[%s] Closure error: %s%n", processId, e.getMessage());
        }
    }

    private void handleGrantDelivery(String consumerTag, Delivery delivery) {
        var correlationId = delivery.getProperties().getCorrelationId();
        if (correlationId != null) {
            var future = pendingRequests.remove(correlationId);
            if (future != null) {
                try {
                    var grant = (GrantMessage) deserialize(delivery.getBody());
                    future.complete(grant);
                } catch (Exception e) {
                    future.completeExceptionally(e);
                }
            } else {
                System.out.printf("[%s] Ignored stale grant. correlationId=%s%n", processId, correlationId);
            }
        }
    }
}