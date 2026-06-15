package pcd.messages;

import java.io.Serializable;

/**
 * Messaggio di richiesta di acquire per una sezione critica.
 * Un processo invia questo messaggio al middleware per richiedere l'accesso a una risorsa.
 */
public class AcquireRequest implements Serializable {
    private static final long serialVersionUID = 1L;

    private final String resourceId;
    private final String processId;
    private final String replyQueueName;

    public AcquireRequest(String resourceId, String processId, String replyQueueName) {
        this.resourceId = resourceId;
        this.processId = processId;
        this.replyQueueName = replyQueueName;
    }

    public String getResourceId() {
        return resourceId;
    }

    public String getProcessId() {
        return processId;
    }

    public String getReplyQueueName() {
        return replyQueueName;
    }

    @Override
    public String toString() {
        return "AcquireRequest{" +
                "resourceId='" + resourceId + '\'' +
                ", processId='" + processId + '\'' +
                ", replyQueueName='" + replyQueueName + '\'' +
                '}';
    }
}
