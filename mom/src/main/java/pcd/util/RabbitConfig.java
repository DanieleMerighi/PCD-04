package pcd.util;

public final class RabbitConfig {
    public static final String REQUEST_EXCHANGE = "lock_requests_exchange";
    public static final String REQUEST_QUEUE = "lock_requests_queue";
    public static final String GRANT_EXCHANGE = "lock_grants_exchange";

    public static final String ROUTING_ACQUIRE = "acquire";
    public static final String ROUTING_RELEASE = "release";

    public static final long ACQUIRE_TIMEOUT_MS = 30000;

    private RabbitConfig() {} // Prevent instantiation
}