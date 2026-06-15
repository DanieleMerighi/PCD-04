package pcd.client;

import com.rabbitmq.client.*;
import pcd.messages.AcquireRequest;
import pcd.messages.GrantMessage;
import pcd.messages.ReleaseRequest;
import pcd.util.LockTarget;
import pcd.util.RabbitConfig;

import java.io.*;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import static pcd.util.Serialize.serialize;
import static pcd.util.Serialize.deserialize;

public class RabbitMQLockManagerClient implements DistributedLockManager {

    private final String processId;
    private final String replyQueueName;
    private final Connection connection;
    private final Channel requestChannel;
    private final Channel replyChannel;
    private final BlockingQueue<GrantMessage> grantQueue;

    public RabbitMQLockManagerClient(String processId, String rabbitmqHost) throws Exception {
        this.processId = processId;
        this.grantQueue = new LinkedBlockingQueue<>();

        var factory = new ConnectionFactory();
        factory.setHost(rabbitmqHost);
        this.connection = factory.newConnection();
        this.requestChannel = connection.createChannel();
        this.replyChannel = connection.createChannel();

        requestChannel.exchangeDeclare(RabbitConfig.REQUEST_EXCHANGE, "direct", true);
        requestChannel.queueDeclare(RabbitConfig.REQUEST_QUEUE, true, false, false, null);
        requestChannel.queueBind(RabbitConfig.REQUEST_QUEUE, RabbitConfig.REQUEST_EXCHANGE, RabbitConfig.ROUTING_ACQUIRE);
        requestChannel.queueBind(RabbitConfig.REQUEST_QUEUE, RabbitConfig.REQUEST_EXCHANGE, RabbitConfig.ROUTING_RELEASE);

        replyChannel.exchangeDeclare(RabbitConfig.GRANT_EXCHANGE, "direct", true);
        this.replyQueueName = "reply_" + processId;
        replyChannel.queueDeclare(replyQueueName, false, true, false, null);
        replyChannel.queueBind(replyQueueName, RabbitConfig.GRANT_EXCHANGE, processId);

        var listenerThread = new Thread(this::listenForGrants);
        listenerThread.setDaemon(true);
        listenerThread.start();

        System.out.printf("[%s] Client initialized. Reply queue: %s%n", processId, replyQueueName);
    }

    @Override
    public void acquire(LockTarget target) throws InterruptedException {
        try {
            var request = new AcquireRequest(target.getPath(), processId, replyQueueName);
            var messageBody = serialize(request);

            requestChannel.basicPublish(RabbitConfig.REQUEST_EXCHANGE, RabbitConfig.ROUTING_ACQUIRE, null, messageBody);
            System.out.printf("[%s] Acquire requested for target: %s%n", processId, target);

            var grant = grantQueue.poll(RabbitConfig.ACQUIRE_TIMEOUT_MS, TimeUnit.MILLISECONDS);
            if (grant == null) {
                throw new InterruptedException("Timeout waiting for lock: " + target);
            }
            if (!grant.granted()) {
                throw new InterruptedException("Lock denied for: " + target);
            }

            System.out.printf("[%s] Lock acquired for target: %s%n", processId, target);

        } catch (IOException e) {
            throw new InterruptedException(String.format("[%s] Error acquiring lock: %s", processId, e.getMessage()));
        }
    }

    @Override
    public void release(LockTarget target) {
        try {
            var request = new ReleaseRequest(target.getPath(), processId);
            var messageBody = serialize(request);

            requestChannel.basicPublish(RabbitConfig.REQUEST_EXCHANGE, RabbitConfig.ROUTING_RELEASE, null, messageBody);
            System.out.printf("[%s] Release requested for target: %s%n", processId, target);

        } catch (IOException e) {
            System.err.printf("[%s] Error releasing lock: %s%n", processId, e.getMessage());
        }
    }

    private void listenForGrants() {
        try {
            DeliverCallback deliverCallback = (consumerTag, delivery) -> {
                try {
                    var grant = (GrantMessage) deserialize(delivery.getBody());
                    grantQueue.offer(grant);
                } catch (Exception e) {
                    System.err.printf("[%s] Grant deserialization error: %s%n", processId, e.getMessage());
                }
            };
            replyChannel.basicConsume(replyQueueName, true, deliverCallback, consTag -> {});
        } catch (IOException e) {
            System.err.printf("[%s] Grant listener error: %s%n", processId, e.getMessage());
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
}