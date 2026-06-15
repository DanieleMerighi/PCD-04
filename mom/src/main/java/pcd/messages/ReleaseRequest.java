package pcd.messages;

import java.io.Serial;
import java.io.Serializable;

/**
 * Messaggio di rilascio di una sezione critica.
 * Un processo invia questo messaggio al middleware per notificare il rilascio di una risorsa.
 */
public record ReleaseRequest(String resourceId, String processId) implements Serializable {
    @Serial
    private static final long serialVersionUID = 1L;

    @Override
    public String toString() {
        return "ReleaseRequest{" +
                "resourceId='" + resourceId + '\'' +
                ", processId='" + processId + '\'' +
                '}';
    }
}
