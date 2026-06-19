package pcd.util;

/**
 * Contains configuration constants for the RabbitMQ middleware infrastructure.
 * <p>
 * Defines exchange names, queue names, routing keys, and timeout parameters used by
 * both the central lock server and the distributed clients.
 */
public final class RabbitConfig {
    /** The name of the exchange used for sending acquire and release requests. */
    public static final String REQUEST_EXCHANGE = "lock_requests_exchange";
    /** The name of the queue where the server listens for incoming requests. */
    public static final String REQUEST_QUEUE = "lock_requests_queue";

    /** The routing key used to publish lock acquisition requests. */
    public static final String ROUTING_ACQUIRE = "acquire";
    /** The routing key used to publish lock release requests. */
    public static final String ROUTING_RELEASE = "release";

    /** The maximum time in milliseconds a client will wait for a lock grant before timing out. */
    public static final long ACQUIRE_TIMEOUT_MS = 30000;

    private RabbitConfig() {}
}