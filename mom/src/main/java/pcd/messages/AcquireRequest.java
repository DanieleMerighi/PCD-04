package pcd.messages;

import java.io.Serial;
import java.io.Serializable;

/**
 * Messaggio di richiesta di acquire per una sezione critica.
 * Un processo invia questo messaggio al middleware per richiedere l'accesso a una risorsa.
 */
public record AcquireRequest(String resourceId, String processId, String replyQueueName) implements Serializable {
    @Serial
    private static final long serialVersionUID = 1L;

    @Override
    public String toString() {
        return "AcquireRequest{" +
                "resourceId='" + resourceId + '\'' +
                ", processId='" + processId + '\'' +
                ", replyQueueName='" + replyQueueName + '\'' +
                '}';
    }
}
